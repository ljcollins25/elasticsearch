/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for removeitional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, StoredFilterUpdate 2.0 (the "License"); you may
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

import org.apache.lucene.index.DocValuesType;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.search.Query;
import org.elasticsearch.Version;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.index.query.QueryShardContext;
import org.elasticsearch.index.query.QueryShardException;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/** Mapper for the stored filter add/remove field. */
public class StoredFilterUpdateFieldMapper extends MetadataFieldMapper {

    public static final String ADD_NAME = "_stored_filter_add";
    public static final String REMOVE_NAME = "_stored_filter_remove";
    public static final String CONTENT_TYPE = "_stored_filter_update";

    public enum UpdateKind {
        Add,
        Remove
    }

    private final UpdateKind updateKind;


    public static class Defaults {

        public static final String NAME = StoredFilterUpdateFieldMapper.CONTENT_TYPE;
        public static final MappedFieldType FIELD_TYPE = new StoredFilterUpdateFieldType();

        static {
            FIELD_TYPE.setName(NAME);
            FIELD_TYPE.setDocValuesType(DocValuesType.NONE);
            FIELD_TYPE.setIndexOptions(IndexOptions.NONE);
            FIELD_TYPE.setHasDocValues(false);
            FIELD_TYPE.freeze();
        }
    }

    private static String getUpdateName(UpdateKind updateKind) {
        return (updateKind == UpdateKind.Add ? ADD_NAME : REMOVE_NAME);
    }

    private static MappedFieldType defaultFieldType(Version indexCreated) {
        MappedFieldType defaultFieldType = Defaults.FIELD_TYPE.clone();
        defaultFieldType.setHasDocValues(true);
        return defaultFieldType;
    }

    public static class Builder extends MetadataFieldMapper.Builder<Builder, StoredFilterUpdateFieldMapper> {

        protected EnabledAttributeMapper enabledState = EnabledAttributeMapper.UNSET_DISABLED;
        private final UpdateKind updateKind;

        private Builder(MappedFieldType existing, Version indexCreated, UpdateKind updateKind) {
            super(getUpdateName(updateKind), existing == null ? defaultFieldType(indexCreated) : existing.clone(),
                defaultFieldType(indexCreated));
            this.updateKind = updateKind;
            builder = this;
        }

        public Builder enabled(EnabledAttributeMapper enabled) {
            this.enabledState = enabled;
            return builder;
        }

        @Override
        public StoredFilterUpdateFieldMapper build(BuilderContext context) {
            setupFieldType(context);
            return new StoredFilterUpdateFieldMapper(context.indexSettings(), this.updateKind);
        }
    }

    public static class TypeParser implements MetadataFieldMapper.TypeParser {
        private final UpdateKind updateKind;

        @Override
        public MetadataFieldMapper.Builder<?, ?> parse(String name, Map<String, Object> node, ParserContext parserContext) throws MapperParsingException {
            Builder builder = new Builder(parserContext.mapperService().fullName(getUpdateName(this.updateKind)),
                parserContext.indexVersionCreated(),
                this.updateKind);
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
            return new StoredFilterUpdateFieldMapper(indexSettings, this.updateKind);
        }

        public TypeParser(UpdateKind updateKind) {
            this.updateKind = updateKind;
        }
    }

    static final class StoredFilterUpdateFieldType extends MappedFieldType {

        StoredFilterUpdateFieldType() {
        }

        protected StoredFilterUpdateFieldType(StoredFilterUpdateFieldType ref) {
            super(ref);
        }

        @Override
        public MappedFieldType clone() {
            return new StoredFilterUpdateFieldType(this);
        }

        @Override
        public String typeName() {
            return CONTENT_TYPE;
        }

        @Override
        public Query termQuery(Object value, QueryShardContext context) {
            throw new QueryShardException(context, "The _stored_filter_update field is not searchable");
        }
    }

    private StoredFilterUpdateFieldMapper(Settings indexSettings, UpdateKind updateKind) {
        super((updateKind == UpdateKind.Add ? ADD_NAME : REMOVE_NAME), Defaults.FIELD_TYPE, Defaults.FIELD_TYPE, indexSettings);
        this.updateKind = updateKind;
    }

    @Override
    public String name() {
        return simpleName();
    }

    @Override
    public void preParse(ParseContext context) throws IOException {
        context.docMapper().root().mappingUpdate(this);
    }

    @Override
    protected void parseCreateField(ParseContext context, List<IndexableField> fields) throws IOException {
        long value = context.parser().longValue();

        if (updateKind == UpdateKind.Add) {
            StoredFilterFieldMapper.addStoredFilterSequenceNumber(context, value);
        } else {
            StoredFilterFieldMapper.removeStoredFilterSequenceNumber(context, value);
        }
    }

    @Override
    public Mapper parse(ParseContext context) throws IOException {
        return null;
    }

    @Override
    public void postParse(ParseContext context) throws IOException {

    }

    @Override
    protected String contentType() {
        return CONTENT_TYPE;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        return builder;
    }

    @Override
    protected void doMerge(Mapper mergeWith, boolean updateAllTypes) {
        // nothing to do
    }
}
