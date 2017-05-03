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

import org.apache.lucene.document.Document;
import org.apache.lucene.index.*;
import org.apache.lucene.search.*;
import org.apache.lucene.store.ByteArrayDataInput;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.text.Text;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

/** A {@link Query} that matches documents in a stored filter. */
public final class StoredFilterQuery extends Query {

    private final Text filterName;
    private final StoredFilterManager storedFilterManager;

    /** Sole constructor. */
    public StoredFilterQuery(String filterName, StoredFilterManager storedFilterManager) {
        this.filterName = new Text(filterName);
        this.storedFilterManager = storedFilterManager;
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
    public Query rewrite(IndexReader reader) throws IOException {
        Query storedFilter = storedFilterManager.tryGetStoredFilter(filterName.string());
        if (storedFilter == null) return this;
        else return storedFilter.rewrite(reader);
    }

    @Override
    public Weight createWeight(IndexSearcher searcher, boolean needsScores) throws IOException {

        // The first phase gets the filter document for this filter if it exists. This document must exists if the filter exists.
        // Even if segment docs have been merged into their respective documents this document will remain as a proxy for the existence of the filter
        TopDocs filterDocMatch = searcher.search(new TermQuery(new Term(StoredFilterUtils.STORED_FILTER_NAME_FIELD_NAME, filterName.string())), 1);
        if (filterDocMatch.totalHits == 0)
        {
            return getEmptyWeight();
        }

        Document document = searcher.doc(filterDocMatch.scoreDocs[0].doc);
        IndexableField[] segmentsFields = document.getFields(StoredFilterUtils.STORED_FILTER_SEGMENTS_FIELD_NAME);
        IndexableField[] docsFields = document.getFields(StoredFilterUtils.STORED_FILTER_DOCS_FIELD_NAME);

        // This should not occur unless there is data corruption or incorrect code of some sort.
        assert  (segmentsFields.length != docsFields.length);

        Map<String, RoaringDocIdSet> docsBySegmentMap = new HashMap<>();
        for (int i = 0; i < segmentsFields.length; i++) {
            String segmentName = segmentsFields[i].stringValue();
            BytesRef bytes = docsFields[i].binaryValue();
            RoaringDocIdSet docIdSet = RoaringDocIdSet.read(new ByteArrayDataInput(bytes.bytes, bytes.offset, bytes.length));
            docsBySegmentMap.put(segmentName, docIdSet);
        }

        return new ConstantScoreWeight(this) {
            @Override
            public Scorer scorer(LeafReaderContext context) throws IOException {

                SegmentReader segmentReader = StoredFilterUtils.segmentReader(context.reader());
                String segmentName = segmentReader.getSegmentName();
                RoaringDocIdSet docIdSet = docsBySegmentMap.getOrDefault(segmentName, null);
                if (docIdSet != null) {
                    return new ConstantScoreScorer(this, score(), docIdSet.iterator());
                }

                // If there is no segment doc id set for this segment stored in the filter document,
                // check for the filter tag term for this filter.
                // TODO: Maybe the filter tag should be something other than the filter name (i.e. a unique auto-generated id)
                Terms storedFilterTerms = context.reader().terms(StoredFilterUtils.STORED_FILTER_TERM_FIELD_NAME);
                if (storedFilterTerms == null) {
                    return null;
                }

                TermsEnum storedFilterTermsEnum = storedFilterTerms.iterator();
                boolean foundFilter = storedFilterTermsEnum.seekExact(filterName.bytes().toBytesRef());
                if (!foundFilter) {
                    return null;
                }

                PostingsEnum postings = storedFilterTermsEnum.postings(null);
                return new ConstantScoreScorer(this, score(), postings);
            }
        };
    }

    private Weight getEmptyWeight() {
        return new ConstantScoreWeight(this) {
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
