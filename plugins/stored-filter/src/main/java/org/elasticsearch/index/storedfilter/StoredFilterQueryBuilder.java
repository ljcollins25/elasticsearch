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
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.SetOnce;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.ParsingException;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.hash.MurmurHash3;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.support.XContentMapValues;
import org.elasticsearch.index.query.AbstractQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryRewriteContext;
import org.elasticsearch.index.query.QueryShardContext;
import org.elasticsearch.indices.TermsLookup;

import java.io.IOException;
import java.util.List;
import java.util.Objects;
import java.util.function.Supplier;

/**
 * A filter for a field based on several terms matching on any of them.
 */
public class StoredFilterQueryBuilder extends AbstractQueryBuilder<StoredFilterQueryBuilder> {
    public static final String NAME = "stored_filter";
    public static final String SHARD_ID_TOKEN = "{shard_id}";

    private final String fieldName;
    private final TermsLookup termsLookup;
    private final Supplier<BytesRef> encodedValuesSupplier;

    /**
     * constructor used internally for serialization of both value / termslookup variants
     */
    StoredFilterQueryBuilder(String fieldName, TermsLookup termsLookup) {
        if (Strings.isEmpty(fieldName)) {
            throw new IllegalArgumentException("field name cannot be null.");
        }

        if (termsLookup == null) {
            throw new IllegalArgumentException("No termsLookup specified for stored filter query");
        }

        this.fieldName = fieldName;
        this.termsLookup = termsLookup;
        this.encodedValuesSupplier = null;
    }

    private StoredFilterQueryBuilder(String fieldName, Supplier<BytesRef> encodedValuesSupplier) {
        this.fieldName = fieldName;
        this.termsLookup = null;
        this.encodedValuesSupplier = encodedValuesSupplier;
    }

    /**
     * Read from a stream.
     */
    public StoredFilterQueryBuilder(StreamInput in) throws IOException {
        super(in);
        fieldName = in.readString();
        termsLookup = in.readOptionalWriteable(TermsLookup::new);
        this.encodedValuesSupplier = null;
    }

    @Override
    protected void doWriteTo(StreamOutput out) throws IOException {
        if (encodedValuesSupplier != null) {
            throw new IllegalStateException("encodedValuesSupplier must be null, can't serialize suppliers, missing a rewriteAndFetch?");
        }

        out.writeString(fieldName);
        out.writeOptionalWriteable(termsLookup);
    }

    public TermsLookup termsLookup() {
        return this.termsLookup;
    }

    @Override
    protected void doXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(NAME);
        termsLookup.toXContent(builder, params);
        printBoostAndQueryName(builder);
        builder.endObject();
    }

    public static StoredFilterQueryBuilder fromXContent(XContentParser parser) throws IOException {
        String fieldName = null;
        List<Object> values = null;
        TermsLookup termsLookup = null;

        String queryName = null;
        float boost = AbstractQueryBuilder.DEFAULT_BOOST;

        XContentParser.Token token;
        String currentFieldName = null;
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                currentFieldName = parser.currentName();
            } else if (token == XContentParser.Token.START_OBJECT) {
                if  (fieldName != null) {
                    throw new ParsingException(parser.getTokenLocation(),
                        "[" + StoredFilterQueryBuilder.NAME + "] query does not support more than one field. "
                            + "Already got: [" + fieldName + "] but also found [" + currentFieldName +"]");
                }
                fieldName = currentFieldName;
                termsLookup = TermsLookup.parseTermsLookup(parser);

                if (!termsLookup.id().contains("{shard_id}")) {
                    throw new ParsingException(parser.getTokenLocation(), "[" + StoredFilterQueryBuilder.NAME + "] query requires" +
                        " document lookup specification id field with shard ID token '" + SHARD_ID_TOKEN + "'. ");
                }
            } else if (token.isValue()) {
                if (AbstractQueryBuilder.BOOST_FIELD.match(currentFieldName)) {
                    boost = parser.floatValue();
                } else if (AbstractQueryBuilder.NAME_FIELD.match(currentFieldName)) {
                    queryName = parser.text();
                } else {
                    throw new ParsingException(parser.getTokenLocation(),
                        "[" + StoredFilterQueryBuilder.NAME + "] query does not support [" + currentFieldName + "]");
                }
            } else {
                throw new ParsingException(parser.getTokenLocation(),
                    "[" + StoredFilterQueryBuilder.NAME + "] unknown token [" + token + "] after [" + currentFieldName + "]");
            }
        }

        if (fieldName == null) {
            throw new ParsingException(parser.getTokenLocation(), "[" + StoredFilterQueryBuilder.NAME + "] query requires a field name, " +
                "followed by document lookup specification");
        }

        return new StoredFilterQueryBuilder(fieldName, termsLookup)
            .boost(boost)
            .queryName(queryName);
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    @Override
    protected Query doToQuery(QueryShardContext context) throws IOException {
        if (encodedValuesSupplier == null) {
            throw new UnsupportedOperationException("query must be rewritten first");
        }

        // TODO[LANCEC]: Introduce/improve cache story for stored filters
        BytesRef encodedValues = encodedValuesSupplier.get();
        MurmurHash3.Hash128 valuesHash128 = MurmurHash3.hash128(encodedValues.bytes, encodedValues.offset, encodedValues.length, 0, new MurmurHash3.Hash128());
        String valuesHash = Long.toHexString(valuesHash128.h1) + Long.toHexString(valuesHash128.h2);

        LongIterator values = StoredFilterUtils.getLongIterator(encodedValuesSupplier.get());

        return new StoredFilterQuery(fieldName, valuesHash, values);
    }

    private void fetch(TermsLookup termsLookup, Client client, ActionListener<BytesRef> actionListener) {
        GetRequest getRequest = new GetRequest(termsLookup.index(), termsLookup.type(), termsLookup.id())
            .preference("_local").routing(termsLookup.routing());
        client.get(getRequest, new ActionListener<GetResponse>() {
            @Override
            public void onResponse(GetResponse getResponse) {
                BytesRef encodedValues = new BytesRef();
                if (getResponse.isSourceEmpty() == false) { // extract terms only if the doc source exists
                    // TODO: Convert extractValue to BytesRef
                    Object extractedValue = XContentMapValues.extractValue(termsLookup.path(), getResponse.getSourceAsMap());

                    encodedValues = new BytesRef(StoredFilterUtils.convertFieldValueToBytes(extractedValue, termsLookup.path()));
                }
                actionListener.onResponse(encodedValues);
            }

            @Override
            public void onFailure(Exception e) {
                actionListener.onFailure(e);
            }
        });
    }

    @Override
    protected int doHashCode() {
        return Objects.hash(fieldName, termsLookup, encodedValuesSupplier);
    }

    @Override
    protected boolean doEquals(StoredFilterQueryBuilder other) {
        return Objects.equals(fieldName, other.fieldName) &&
            Objects.equals(termsLookup, other.termsLookup) &&
            Objects.equals(encodedValuesSupplier, other.encodedValuesSupplier);
    }

    @Override
    protected QueryBuilder doRewrite(QueryRewriteContext queryRewriteContext) {
        if (queryRewriteContext.shardId() == null || encodedValuesSupplier != null) {
            return this;
        } else if (this.termsLookup != null) {

            TermsLookup detokenizedTermsLookup = new TermsLookup(
                termsLookup.index(),
                termsLookup.type(),
                termsLookup.id().replace(SHARD_ID_TOKEN, Integer.toString(queryRewriteContext.shardId())),
                termsLookup.path()
            ).routing(termsLookup.routing());

            SetOnce<BytesRef> supplier = new SetOnce<>();
            queryRewriteContext.registerAsyncAction((client, listener) -> {
                fetch(detokenizedTermsLookup, client, ActionListener.wrap(list -> {
                    supplier.set(list);
                    listener.onResponse(null);
                }, listener::onFailure));

            });

            return new StoredFilterQueryBuilder(this.fieldName, supplier::get);
        }
        return this;
    }
}
