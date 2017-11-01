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

import org.apache.lucene.document.Field;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.search.Query;
import org.elasticsearch.common.ParsingException;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentLocation;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryShardContext;
import org.elasticsearch.index.query.QueryShardException;
import org.elasticsearch.index.storedfilters.StoredFilterUtils;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.index.mapper.TypeParsers.parseField;
import static org.elasticsearch.index.query.AbstractQueryBuilder.parseInnerQueryBuilder;

/**
 *
 */
public class StoredFilterFieldMapper extends FieldMapper {

    public static final String CONTENT_TYPE = "stored_filter";

    private final QueryShardContext queryShardContext;

    public static class Defaults {
        public static final MappedFieldType FIELD_TYPE = new StoredFilterFieldType();

        static {
            FIELD_TYPE.setStored(true);
            FIELD_TYPE.setIndexOptions(IndexOptions.NONE);
            FIELD_TYPE.setHasDocValues(false);
            FIELD_TYPE.freeze();
        }
    }

    protected StoredFilterFieldMapper(String simpleName, MappedFieldType fieldType, MappedFieldType defaultFieldType,
                                      Settings indexSettings, MultiFields multiFields, CopyTo copyTo, QueryShardContext queryShardContext) {
        super(simpleName, fieldType, defaultFieldType, indexSettings, multiFields, copyTo);

        this.queryShardContext = queryShardContext;
    }

    public static class Builder extends FieldMapper.Builder<Builder, StoredFilterFieldMapper> {

        private final QueryShardContext queryShardContext;

        public Builder(String name, QueryShardContext queryShardContext) {
            super(name, Defaults.FIELD_TYPE, Defaults.FIELD_TYPE);
            builder = this;
            this.queryShardContext = queryShardContext;
        }

        @Override
        public StoredFilterFieldMapper build(BuilderContext context) {
            setupFieldType(context);
            return new StoredFilterFieldMapper(name, fieldType, defaultFieldType,
                    context.indexSettings(), multiFieldsBuilder.build(this, context), copyTo, queryShardContext);
        }
    }

    public static class TypeParser implements Mapper.TypeParser {
        @Override
        public Mapper.Builder parse(String name, Map<String, Object> node, ParserContext parserContext) throws MapperParsingException {
            StoredFilterFieldMapper.Builder builder = new StoredFilterFieldMapper.Builder(name, parserContext.queryShardContextSupplier().get());
            parseField(builder, name, node, parserContext);
            return builder;
        }
    }

    static final class StoredFilterFieldType extends MappedFieldType {

        public StoredFilterFieldType() {}

        protected StoredFilterFieldType(StoredFilterFieldType ref) {
            super(ref);
        }

        @Override
        public MappedFieldType clone() {
            return new StoredFilterFieldType(this);
        }


        @Override
        public String typeName() {
            return CONTENT_TYPE;
        }

        @Override
        public Query termQuery(Object value, QueryShardContext context) {
            throw new QueryShardException(context, "Binary fields do not support searching");
        }
    }

    @Override
    protected void parseCreateField(ParseContext context, List<IndexableField> fields) throws IOException {
        QueryShardContext queryShardContext = new QueryShardContext(this.queryShardContext);
        if (context.doc().getField(fieldType.name()) != null) {
            // If a percolator query has been defined in an array object then multiple percolator queries
            // could be provided. In order to prevent this we fail if we try to parse more than one query
            // for the current document.
            throw new IllegalArgumentException("a stored filter document can only contain one query");
        }

        XContentParser parser = context.parser();
        QueryBuilder queryBuilder = parseQueryBuilder(
            parser, parser.getTokenLocation()
        );

        //verifyQuery(queryBuilder);
        // Fetching of terms, shapes and indexed scripts happen during this rewrite:
        queryBuilder = queryBuilder.rewrite(queryShardContext);

        boolean mapUnmappedFieldAsString = true;
        Query query = toQuery(queryShardContext, queryBuilder);

        StoredFilterQueryField field = (StoredFilterQueryField) context.doc().getByKey(fieldType().name());
        if (field == null) {
            field = new StoredFilterQueryField(query);
            context.doc().addWithKey(fieldType().name(), field);
        }
    }

    static Query toQuery(QueryShardContext context, QueryBuilder queryBuilder) throws IOException {
        // This means that fields in the query need to exist in the mapping prior to registering this query
        // The reason that this is required, is that if a field doesn't exist then the query assumes defaults, which may be undesired.
        //
        // Even worse when fields mentioned in percolator queries do go added to map after the queries have been registered
        // then the percolator queries don't work as expected any more.
        //
        // Query parsing can't introduce new fields in mappings (which happens when registering a percolator query),
        // because field type can't be inferred from queries (like document do) so the best option here is to disallow
        // the usage of unmapped fields in percolator queries to avoid unexpected behaviour
        context.setAllowUnmappedFields(false);
        context.setMapUnmappedFieldAsString(false);
        return queryBuilder.toQuery(context);
    }

    private static QueryBuilder parseQueryBuilder(XContentParser parser, XContentLocation location) {
        try {
            return parseInnerQueryBuilder(parser);
        } catch (IOException e) {
            throw new ParsingException(location, "Failed to parse", e);
        }
    }

    @Override
    protected String contentType() {
        return CONTENT_TYPE;
    }

    public static class StoredFilterQueryField extends Field {
        private final Query query;

        public StoredFilterQueryField(Query query) {
            super(StoredFilterUtils.STORED_FILTER_QUERY_FIELD_NAME, "queryContent", Defaults.FIELD_TYPE);
            this.query = query;
        }

        public Query query() {
            return query;
        }
    }
}
