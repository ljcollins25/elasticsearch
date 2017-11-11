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

package org.elasticsearch.index.storedfilter;

import org.apache.lucene.search.Query;
import org.apache.lucene.util.SetOnce;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.ParsingException;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.ObjectParser;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.support.XContentMapValues;
import org.elasticsearch.index.query.*;
import org.elasticsearch.indices.TermsLookup;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.function.Supplier;

/**
 * Constructs a query that only match on documents that the field has a value in them.
 */
public class StoredFilterQueryBuilder extends AbstractQueryBuilder<StoredFilterQueryBuilder> {
    public static final String NAME = "stored_filter";

    public static final ParseField ID_FIELD = new ParseField("id");

    private String filterId;
    private Supplier<LongIterator> iteratorSupplier;

    public StoredFilterQueryBuilder(String filterId) {
        if (Strings.isEmpty(filterId)) {
            throw new IllegalArgumentException("filter id is null or empty");
        }
        this.filterId = filterId;
    }

    public StoredFilterQueryBuilder(String filterId, Supplier<LongIterator> iteratorSupplier) {
        if (Strings.isEmpty(filterId)) {
            throw new IllegalArgumentException("filter id is null or empty");
        }
        this.filterId = filterId;
        this.iteratorSupplier = iteratorSupplier;
    }

    /**
     * Creates a new IdsQueryBuilder with no types specified upfront
     */
    public StoredFilterQueryBuilder() {
        // nothing to do
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

    private StoredFilterQueryBuilder setId(String filterId)
    {
        this.filterId = filterId;
        return this;
    }

    @Override
    protected void doXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(NAME);
        builder.field(ID_FIELD.getPreferredName(), filterId);
        printBoostAndQueryName(builder);
        builder.endObject();
    }

    private static ObjectParser<StoredFilterQueryBuilder, Void> PARSER = new ObjectParser<>(NAME,
        () -> new StoredFilterQueryBuilder());

    static {
        PARSER.declareString(StoredFilterQueryBuilder::setId, StoredFilterQueryBuilder.ID_FIELD);
        declareStandardFields(PARSER);
    }

    public static StoredFilterQueryBuilder fromXContent(XContentParser parser) {
        try {
            return PARSER.apply(parser, null);
        } catch (IllegalArgumentException e) {
            throw new ParsingException(parser.getTokenLocation(), e.getMessage(), e);
        }
    }

    private void fetch(TermsLookup termsLookup, Client client, ActionListener<List<Object>> actionListener) {
        GetRequest getRequest = new GetRequest(termsLookup.index(), termsLookup.type(), termsLookup.id())
            .preference("_local").routing(termsLookup.routing());
        client.get(getRequest, new ActionListener<GetResponse>() {
            @Override
            public void onResponse(GetResponse getResponse) {
                List<Object> terms = new ArrayList<>();
                if (getResponse.isSourceEmpty() == false) { // extract terms only if the doc source exists
                    List<Object> extractedValues = XContentMapValues.extractRawValues(termsLookup.path(), getResponse.getSourceAsMap());
                    terms.addAll(extractedValues);
                }
                actionListener.onResponse(terms);
            }

            @Override
            public void onFailure(Exception e) {
                actionListener.onFailure(e);
            }
        });
    }

    @Override
    protected QueryBuilder doRewrite(QueryRewriteContext queryRewriteContext) throws IOException {
        QueryShardContext shardContext = queryRewriteContext.convertToShardContext();
        // If the context is null we are not on the shard and cannot
        // rewrite so rewrite is noop
        if (shardContext == null) {
            return this;
        }

        SetOnce<LongIterator> supplier = new SetOnce<>();
        queryRewriteContext.registerAsyncAction((client, listener) -> {
            fetch(termsLookup, client, ActionListener.wrap(list -> {
                supplier.set(list);
                listener.onResponse(null);
            }, listener::onFailure));

        });
        return new StoredFilterQueryBuilder(this.filterId, supplier::get);
    }

    @Override
    protected Query doToQuery(QueryShardContext context) throws IOException {
        return newFilter(context, filterId);
    }

    public static Query newFilter(QueryShardContext context, String filterId) {

        return new StoredFilterQuery(filterId);
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
