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

import org.apache.lucene.index.*;
import org.apache.lucene.search.*;
import org.elasticsearch.common.text.Text;

import java.io.IOException;
import java.util.Objects;

/** A {@link Query} that matches documents in a stored filter. */
public final class StoredFilterQuery extends Query {

    private final Text filterName;

    /** Sole constructor. */
    public StoredFilterQuery(String filterName) {
        this.filterName = new Text(filterName);
    }

    @Override
    public int hashCode() {
        return Objects.hash(classHash(), filterName);
    }

    @Override
    public boolean equals(Object obj) {
        if (sameClassAs(obj) == false) {
            return false;
        }
        StoredFilterQuery that = (StoredFilterQuery) obj;
        return filterName == that.filterName;
    }

    @Override
    public Weight createWeight(IndexSearcher searcher, boolean needsScores, float boost) throws IOException {

        // The first phase gets the filter document for this filter if it exists. This document must exists if the filter exists.
        // Even if segment docs have been merged into their respective documents this document will remain as a proxy for the existence of the filter
        TopDocs filterDocMatch = searcher.search(new TermQuery(new Term(StoredFilterUtils.STORED_FILTER_NAME_FIELD_NAME, filterName.string())), 1);
        if (filterDocMatch.totalHits == 0)
        {
            return getEmptyWeight();
        }

        PointInLongSetQuery subQuery = new PointInLongSetQuery(
            StoredFilterUtils.loadSequenceNumbersField(searcher, filterDocMatch.scoreDocs[0].doc));

        return subQuery.createWeight(this, searcher, needsScores, boost);
    }

    private Weight getEmptyWeight() {
        return new ConstantScoreWeight(this, 1.0f) {
            @Override
            public Scorer scorer(LeafReaderContext context) throws IOException {
                return null;
            }
        };
    }

    @Override
    public String toString(String field) {
        return "StoredFilterQuery(filterName=" + filterName.string()  + ")";
    }
}
