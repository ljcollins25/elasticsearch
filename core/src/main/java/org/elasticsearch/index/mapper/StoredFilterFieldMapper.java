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

package org.elasticsearch.index.mapper;

import org.apache.lucene.document.StoredField;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.Version;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.storedfilters.LongIterator;
import org.elasticsearch.index.storedfilters.LongList;
import org.elasticsearch.index.storedfilters.StoredFilterUtils;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

public class StoredFilterFieldMapper extends MetadataFieldMapper {
    public static final String NAME = "_stored_filter";

    public static class Defaults  {
        public static final EnabledAttributeMapper ENABLED_STATE = EnabledAttributeMapper.UNSET_DISABLED;

        public static final MappedFieldType FIELD_TYPE =
            new BinaryFieldMapper.BinaryFieldType();

        static {
            FIELD_TYPE.setStored(true);
            FIELD_TYPE.setName(NAME);
            FIELD_TYPE.setIndexOptions(IndexOptions.NONE);
            FIELD_TYPE.setHasDocValues(false);
            FIELD_TYPE.freeze();
        }
    }

    private static MappedFieldType defaultFieldType(Version indexCreated) {
        MappedFieldType defaultFieldType = Defaults.FIELD_TYPE.clone();
        defaultFieldType.setHasDocValues(true);
        return defaultFieldType;
    }

    public static class Builder extends MetadataFieldMapper.Builder<Builder, StoredFilterFieldMapper> {

        protected EnabledAttributeMapper enabledState = EnabledAttributeMapper.UNSET_DISABLED;

        private Builder(MappedFieldType existing, Version indexCreated) {
            super(NAME, existing == null ? defaultFieldType(indexCreated) : existing.clone(),
                defaultFieldType(indexCreated));
            builder = this;
        }

        public Builder enabled(EnabledAttributeMapper enabled) {
            this.enabledState = enabled;
            return builder;
        }

        @Override
        public StoredFilterFieldMapper build(BuilderContext context) {
            setupFieldType(context);
            return new StoredFilterFieldMapper(enabledState, fieldType, context.indexSettings());
        }
    }

    public static class TypeParser implements MetadataFieldMapper.TypeParser {
        @Override
        public MetadataFieldMapper.Builder<?, ?> parse(String name, Map<String, Object> node,
                                                       ParserContext parserContext) throws MapperParsingException {
            Builder builder = new Builder(parserContext.mapperService().fullName(NAME),
                parserContext.indexVersionCreated());
            for (Iterator<Map.Entry<String, Object>> iterator = node.entrySet().iterator(); iterator.hasNext();) {
                Map.Entry<String, Object> entry = iterator.next();
                String fieldName = entry.getKey();
                Object fieldNode = entry.getValue();
                if (fieldName.equals("enabled")) {
                    boolean enabled = TypeParsers.nodeBooleanValue(name, "enabled", fieldNode, parserContext);
                    builder.enabled(enabled ? EnabledAttributeMapper.ENABLED : EnabledAttributeMapper.DISABLED);
                    iterator.remove();
                }
            }
            return builder;
        }

        @Override
        public MetadataFieldMapper getDefault(MappedFieldType fieldType, ParserContext context) {
            final Settings indexSettings = context.mapperService().getIndexSettings().getSettings();
            return new StoredFilterFieldMapper(indexSettings, fieldType);
        }
    }

    private EnabledAttributeMapper enabledState;

    private StoredFilterFieldMapper(Settings indexSettings, MappedFieldType existing) {
        this(Defaults.ENABLED_STATE,
            existing == null ? defaultFieldType(Version.indexCreated(indexSettings)) : existing.clone(),
            indexSettings);
    }

    private StoredFilterFieldMapper(EnabledAttributeMapper enabled, MappedFieldType fieldType, Settings indexSettings) {
        super(NAME, fieldType, defaultFieldType(Version.indexCreated(indexSettings)), indexSettings);
        this.enabledState = enabled;
    }

    @Override
    protected String contentType() {
        return NAME;
    }

    public boolean enabled() {
        return this.enabledState.enabled;
    }

    @Override
    public void preParse(ParseContext context) throws IOException {
        context.docMapper().root().mappingUpdate(this);
    }

    @Override
    public void postParse(ParseContext context) throws IOException {
        // we post parse it so we get the size stored, possibly compressed (source will be preParse)
        //super.parse(context);
    }

    @Override
    public Mapper parse(ParseContext context) throws IOException {
        // nothing to do here, we call the parent in postParse
        return null;
    }

    @Override
    protected void parseCreateField(ParseContext context, List<IndexableField> fields) throws IOException {
        byte[] value = context.parseExternalValue(byte[].class);
        if (value == null) {
            if (context.parser().currentToken() == XContentParser.Token.VALUE_NULL) {
                return;
            } else {
                value = context.parser().binaryValue();
            }
        }
        if (value == null) {
            return;
        }

        getOrCreateField(context).fieldValue = value;
    }

    public static void addStoredFilterSequenceNumber(ParseContext context, long value) {
        getOrCreateField(context).add(value);
    }

    public static void removeStoredFilterSequenceNumber(ParseContext context, long value) {
        getOrCreateField(context).remove(value);
    }

    private static CustomBinaryStoredField getOrCreateField(ParseContext context) {
        CustomBinaryStoredField field = (CustomBinaryStoredField) context.doc().getByKey(StoredFilterFieldMapper.NAME);
        if (field == null) {
            field = new CustomBinaryStoredField(StoredFilterFieldMapper.NAME);
            context.doc().addWithKey(StoredFilterFieldMapper.NAME, field);
        }

        return field;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        boolean includeDefaults = params.paramAsBoolean("include_defaults", false);

        // all are defaults, no need to write it at all
        if (!includeDefaults && enabledState == Defaults.ENABLED_STATE) {
            return builder;
        }
        builder.startObject(contentType());
        if (includeDefaults || enabledState != Defaults.ENABLED_STATE) {
            builder.field("enabled", enabledState.enabled);
        }
        builder.endObject();
        return builder;
    }

    @Override
    protected void doMerge(Mapper mergeWith, boolean updateAllTypes) {
        StoredFilterFieldMapper storedFilterFieldMapperMergeWith = (StoredFilterFieldMapper) mergeWith;
        if (storedFilterFieldMapperMergeWith.enabledState != enabledState && !storedFilterFieldMapperMergeWith.enabledState.unset()) {
            this.enabledState = storedFilterFieldMapperMergeWith.enabledState;
        }
    }

    private static class CustomBinaryStoredField extends StoredField {

        public byte[] fieldValue;
        private final LongList addedSequenceNumbers = new LongList();
        private final LongList removedSequenceNumbers = new LongList();

        public CustomBinaryStoredField(String name) {
            super(name, TYPE);
        }

        public void add(long value) {
            addedSequenceNumbers.add(value);
        }

        public void remove(long value) {
            removedSequenceNumbers.add(value);
        }

        @Override
        public BytesRef binaryValue() {
            try {
                LongIterator baseIterator = LongIterator.empty();
                if (fieldValue != null) {
                    baseIterator = StoredFilterUtils.getLongIterator(new BytesRef(fieldValue));
                }

                addedSequenceNumbers.sortAndDedup();
                removedSequenceNumbers.sortAndDedup();
                final BytesRef bytes = StoredFilterUtils.getLongValuesBytes(
                    LongIterator.sortedUnique(
                        LongIterator.sortedUnion(
                            addedSequenceNumbers.iterator(),
                            LongIterator.sortedExcept(
                                baseIterator,
                                removedSequenceNumbers.iterator()))));
                return bytes;
            } catch (IOException e) {
                throw new ElasticsearchException("Failed to get binary value", e);
            }

        }
    }
}
