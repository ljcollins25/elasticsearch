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

package org.elasticsearch.index.storedfilters;

import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.ConstantScoreQuery;
import org.apache.lucene.search.Query;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.ParsingException;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.lucene.search.Queries;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.mapper.FieldNamesFieldMapper;
import org.elasticsearch.index.query.AbstractQueryBuilder;
import org.elasticsearch.index.query.QueryParseContext;
import org.elasticsearch.index.query.QueryShardContext;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.Objects;
import java.util.Optional;

/**
 * Constructs a query that only match on documents that the field has a value in them.
 */
public class StoredFilterQueryBuilder extends AbstractQueryBuilder<StoredFilterQueryBuilder> {
    public static final String NAME = "stored_filter";

    public static final ParseField ID_FIELD = new ParseField("id");

    private final String filterId;

    public StoredFilterQueryBuilder(String filterId) {
        if (Strings.isEmpty(filterId)) {
            throw new IllegalArgumentException("filter id is null or empty");
        }
        this.filterId = filterId;
    }

    /**
     * Read from a stream.
     */
    public StoredFilterQueryBuilder(StreamInput in) throws IOException {
        super(in);
        filterId = in.readString();
    }

    @Override
    protected void doWriteTo(StreamOutput out) throws IOException {
        out.writeString(filterId);
    }

    /**
     * @return the field name that has to exist for this query to match
     */
    public String filterId() {
        return this.filterId;
    }

    @Override
    protected void doXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(NAME);
        builder.field(ID_FIELD.getPreferredName(), filterId);
        printBoostAndQueryName(builder);
        builder.endObject();
    }

    public static StoredFilterQueryBuilder fromXContent(QueryParseContext parseContext) throws IOException {
        XContentParser parser = parseContext.parser();

        String filterId = null;
        String queryName = null;
        float boost = AbstractQueryBuilder.DEFAULT_BOOST;

        XContentParser.Token token;
        String currentFieldName = null;
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                currentFieldName = parser.currentName();
            } else if (token.isValue()) {
                if (ID_FIELD.match(currentFieldName)) {
                    filterId = parser.text();
                } else if (AbstractQueryBuilder.NAME_FIELD.match(currentFieldName)) {
                    queryName = parser.text();
                } else if (AbstractQueryBuilder.BOOST_FIELD.match(currentFieldName)) {
                    boost = parser.floatValue();
                } else {
                    throw new ParsingException(parser.getTokenLocation(), "[" + StoredFilterQueryBuilder.NAME +
                            "] query does not support [" + currentFieldName + "]");
                }
            } else {
                throw new ParsingException(parser.getTokenLocation(), "[" + StoredFilterQueryBuilder.NAME +
                        "] unknown token [" + token + "] after [" + currentFieldName + "]");
            }
        }

        if (filterId == null) {
            throw new ParsingException(parser.getTokenLocation(), "[" + StoredFilterQueryBuilder.NAME + "] must be provided with a [field]");
        }

        StoredFilterQueryBuilder builder = new StoredFilterQueryBuilder(filterId);
        builder.queryName(queryName);
        builder.boost(boost);
        return builder;
    }

    @Override
    protected Query doToQuery(QueryShardContext context) throws IOException {
        return newFilter(context, filterId);
    }

    public static Query newFilter(QueryShardContext context, String filterId) {

        return new StoredFilterQuery(filterId, context.getStoredFilterManager());
    }

    @Override
    protected int doHashCode() {
        return Objects.hash(filterId);
    }

    @Override
    protected boolean doEquals(StoredFilterQueryBuilder other) {
        return Objects.equals(filterId, other.filterId);
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }
}
