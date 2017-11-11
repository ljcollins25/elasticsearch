/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.ingest.storedfilter;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.index.storedfilter.LongIterator;
import org.elasticsearch.index.storedfilter.LongList;
import org.elasticsearch.index.storedfilter.StoredFilterUtils;
import org.elasticsearch.ingest.AbstractProcessor;
import org.elasticsearch.ingest.ConfigurationUtils;
import org.elasticsearch.ingest.IngestDocument;
import org.elasticsearch.ingest.Processor;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Processor that joins the different items of an array into a single string value using a separator between each item.
 * Throws exception is the specified includeField is not an array.
 */
public final class BinarySequenceProcessor extends AbstractProcessor {

    public static final String TYPE = "binary_sequence";

    private final String includeField;
    private final String excludeField;
    private final String targetField;

    BinarySequenceProcessor(String tag, String includeField, String excludeField, String targetField) {
        super(tag);
        this.includeField = includeField;
        this.excludeField = excludeField;
        this.targetField = targetField;
    }

    String getIncludeField() {
        return includeField;
    }

    String getExcludeField() {
        return excludeField;
    }

    String getTargetField() {
        return targetField;
    }

    @Override
    public void execute(IngestDocument document) {

        byte[] fieldBytes = document.getFieldValueAsBytes(targetField, true);

        DenseOrderedLongSetBuilder fieldBuilder = new DenseOrderedLongSetBuilder();
        if (fieldBytes != null) {
            fieldBuilder.add(fieldBytes);
        }

        if (includeField != null) {
            List<Long> list = document.getFieldValue(includeField, List.class, true);
            if (list == null) {
                throw new IllegalArgumentException("includeField [" + includeField + "] is null, cannot join.");
            }

            for (Long value : list) {
                fieldBuilder.add(value);
            }

            document.removeField(includeField);
        }

        if (excludeField != null) {
            List<Long> list = document.getFieldValue(excludeField, List.class, true);
            if (list == null) {
                throw new IllegalArgumentException("excludeField [" + excludeField + "] is null, cannot join.");
            }

            for (Long value : list) {
                fieldBuilder.remove(value);
            }

            document.removeField(excludeField);
        }

        document.setFieldValue(targetField, BytesRef.deepCopyOf(fieldBuilder.binaryValue()).bytes);
    }

    @Override
    public String getType() {
        return TYPE;
    }

    public static final class Factory implements Processor.Factory {
        @Override
        public BinarySequenceProcessor create(Map<String, Processor.Factory> registry, String processorTag,
                                              Map<String, Object> config) throws Exception {
            String includeField = ConfigurationUtils.readOptionalStringProperty(TYPE, processorTag, config, "include_field");
            String excludeField = ConfigurationUtils.readOptionalStringProperty(TYPE, processorTag, config, "exclude_field");
            String targetField = ConfigurationUtils.readStringProperty(TYPE, processorTag, config, "target_field");

            return new BinarySequenceProcessor(processorTag, includeField, excludeField, targetField);
        }
    }

    public static class DenseOrderedLongSetBuilder {

        private final ArrayList<byte[]> fieldValues = new  ArrayList<>();
        private final LongList addedValues = new LongList();
        private final LongList removedValues = new LongList();

        public void add(byte[] value) {
            fieldValues.add(value);
        }

        public void add(long value) {
            addedValues.add(value);
        }

        public void remove(long value) {
            removedValues.add(value);
        }

        public BytesRef binaryValue() {
            try {
                // The just return the field value if there is only one and it does not need to be joined with another other sequence numbers
                if (fieldValues.size() == 1 && addedValues.size() == 0 && removedValues.size() == 0) {
                    return new BytesRef(fieldValues.get(0));
                }

                LongIterator baseIterator = LongIterator.empty();

                for (byte[] fieldValue : fieldValues) {
                    baseIterator = LongIterator.sortedUnion(baseIterator, StoredFilterUtils.getLongIterator(new BytesRef(fieldValue)));
                }

                addedValues.sortAndDedup();
                removedValues.sortAndDedup();
                final BytesRef bytes = StoredFilterUtils.getLongValuesBytes(
                    LongIterator.sortedUnique(
                        LongIterator.sortedUnion(
                            addedValues.iterator(),
                            LongIterator.sortedExcept(
                                baseIterator,
                                removedValues.iterator()))));
                return bytes;
            } catch (IOException e) {
                throw new ElasticsearchException("Failed to get binary value", e);
            }

        }
    }
}

