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
import org.apache.lucene.util.SetOnce;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.common.hash.MurmurHash3;
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
    private final String unionField;
    private final String targetField;
    private final String targetCountField;
    private final String targetHashField;

    BinarySequenceProcessor(String tag, String includeField, String excludeField, String unionField, String targetField, String targetCountField, String targetHashField) {
        super(tag);
        this.includeField = includeField;
        this.excludeField = excludeField;
        this.targetField = targetField;
        this.unionField = unionField;
        this.targetHashField = targetHashField;
        this.targetCountField = targetCountField;
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

    @SuppressWarnings("unchecked")
    @Override
    public void execute(IngestDocument document) {

        byte[] fieldBytes = document.getFieldValueAsBytes(targetField, true);

        DenseOrderedLongSetBuilder fieldBuilder = new DenseOrderedLongSetBuilder();
        if (fieldBytes != null) {
            fieldBuilder.add(fieldBytes);
        }

        if (unionField != null) {
            List<Object> list = document.getFieldValue(unionField, List.class, true);
            if (list != null) {
                for (Object value : list) {
                    fieldBuilder.add(StoredFilterUtils.convertFieldValueToBytes(value, unionField));
                }

                document.removeField(unionField);
            }
        }

        if (includeField != null) {
            List<Number> list = document.getFieldValue(includeField, List.class, true);
            if (list != null) {
                for (Number value : list) {
                    fieldBuilder.add(value.longValue());
                }

                document.removeField(includeField);
            }
        }

        if (excludeField != null) {
            List<Number> list = document.getFieldValue(excludeField, List.class, true);
            if (list != null) {
                for (Number value : list) {
                    fieldBuilder.remove(value.longValue());
                }

                document.removeField(excludeField);
            }
        }

        SetOnce<Integer> countSupplier = new SetOnce<>();
        byte[] updatedFieldBytes = BytesRef.deepCopyOf(fieldBuilder.binaryValue(countSupplier)).bytes;
        document.setFieldValue(targetField, updatedFieldBytes);
        if (targetCountField != null) {
            document.setFieldValue(targetCountField, countSupplier.get());
        }

        if (targetHashField != null) {
            String targetHash = document.getFieldValue(targetHashField, String.class, true);

            // Use targetHash == string.Empty as an indicator to hash the field
            // This is a rather odd approach but it allows using the same processor for building up the binary sequence filter
            // and for the creating the final binary sequence filter which includes the hash
            if (targetHash != null && targetHash.isEmpty()) {
                document.setFieldValue(targetHashField, StoredFilterUtils.hashBytes(updatedFieldBytes, 0, updatedFieldBytes.length));
            }
        }
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
            String targetCountField = ConfigurationUtils.readOptionalStringProperty(TYPE, processorTag, config, "target_count_field");
            String targetHashField = ConfigurationUtils.readOptionalStringProperty(TYPE, processorTag, config, "target_hash_field");
            String unionField = ConfigurationUtils.readOptionalStringProperty(TYPE, processorTag, config, "union_field");

            return new BinarySequenceProcessor(processorTag, includeField, excludeField, unionField, targetField, targetCountField, targetHashField);
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

        public BytesRef binaryValue(SetOnce<Integer> countSupplier) {
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
                                removedValues.iterator()))),
                    countSupplier);
                return bytes;
            } catch (IOException e) {
                throw new ElasticsearchException("Failed to get binary value", e);
            }

        }
    }
}

