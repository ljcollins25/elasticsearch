/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.elasticsearch.index.storedfilter;

import java.io.IOException;
import java.util.Objects;

import org.apache.lucene.document.IntPoint;
import org.apache.lucene.document.LongPoint;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.PointValues.IntersectVisitor;
import org.apache.lucene.index.PointValues.Relation;
import org.apache.lucene.index.PointValues;
import org.apache.lucene.search.ConstantScoreScorer;
import org.apache.lucene.search.ConstantScoreWeight;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.Weight;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefIterator;
import org.apache.lucene.util.DocIdSetBuilder;

/**
 * Abstract query class to find all documents whose single or multi-dimensional point values, previously indexed with e.g. {@link IntPoint},
 * is contained in the specified set.
 *
 * <p>
 * This is for subclasses and works on the underlying binary encoding: to
 * create range queries for lucene's standard {@code Point} types, refer to factory
 * methods on those classes, e.g. {@link IntPoint#newSetQuery IntPoint.newSetQuery()} for
 * fields indexed with {@link IntPoint}.
 * @see PointValues
 * @lucene.experimental */

public class StoredFilterQuery extends Query {
    private final LongIterator values;

    private final String fieldName;
    private final String valuesHash;

    @Override
    public String toString(String field) {
        return "StoredFilterQuery(fieldName=" + fieldName  + ", valuesHash=" + valuesHash + ")";
    }

    @Override
    public boolean equals(Object obj) {
        if (sameClassAs(obj) == false) {
            return false;
        }
        StoredFilterQuery that = (StoredFilterQuery) obj;
        return fieldName.equals(that.fieldName) && valuesHash.equals(that.valuesHash);
    }

    @Override
    public int hashCode() {
        return Objects.hash(classHash(), fieldName, valuesHash);
    }

    /**
     * Iterator of encoded point values.
     */
    public static abstract class Stream implements BytesRefIterator {
        @Override
        public abstract BytesRef next();
    };

    /** The {@code packedPoints} iterator must be in sorted order. */
    public StoredFilterQuery(String fieldName, String valuesHash, LongIterator values) {
        this.fieldName = fieldName;
        this.valuesHash = valuesHash;
        this.values = values;
    }

    @Override
    public final Weight createWeight(IndexSearcher searcher, boolean needsScores, float boost) throws IOException {

        // We don't use RandomAccessWeight here: it's no good to approximate with "match all docs".
        // This is an inverted structure and should be used in the first pass:

        return new ConstantScoreWeight(this, boost) {

            @Override
            public Scorer scorer(LeafReaderContext context) throws IOException {
                LeafReader reader = context.reader();

                PointValues values = reader.getPointValues(fieldName);
                if (values == null) {
                    // No docs in this segment/field indexed any points
                    return null;
                }

                DocIdSetBuilder result = new DocIdSetBuilder(reader.maxDoc(), values, fieldName);

                // We optimize this common case, effectively doing a merge sort of the indexed values vs the queried set:
                values.intersect(new MergePointVisitor(result));

                return new ConstantScoreScorer(this, score(), result.build().iterator());
            }
        };
    }

    /** Essentially does a merge sort, only collecting hits when the indexed point and query point are the same.  This is an optimization,
     *  used in the 1D case. */
    private class MergePointVisitor implements IntersectVisitor {

        private final DocIdSetBuilder result;
        private Stream iterator;
        private BytesRef nextQueryPoint;
        private final BytesRef scratch = new BytesRef();
        private DocIdSetBuilder.BulkAdder adder;

        public MergePointVisitor(DocIdSetBuilder result) throws IOException {
            this.result = result;
            scratch.length = Long.BYTES;

            iterator = new Stream()
            {
                private BytesRef iteratorScratch = new BytesRef(new byte[Long.BYTES]);
                private LongIterator sortedIterator = values.newIterator();

                @Override
                public BytesRef next() {
                    try {
                        // TODO: Maybe add facility to LongIterator to go to next value >= some target value
                        if (sortedIterator.moveNext()) {
                            LongPoint.encodeDimension(sortedIterator.longValue(), iteratorScratch.bytes, 0);
                            return iteratorScratch;
                        }
                    } catch (IOException e) {
                        e.printStackTrace();
                    }

                    return null;
                }
            };

            nextQueryPoint = iterator.next();
        }

        @Override
        public void grow(int count) {
            adder = result.grow(count);
        }

        @Override
        public void visit(int docID) {
            adder.add(docID);
        }

        @Override
        public void visit(int docID, byte[] packedValue) {
            scratch.bytes = packedValue;
            while (nextQueryPoint != null) {
                int cmp = nextQueryPoint.compareTo(scratch);
                if (cmp == 0) {
                    // Query point equals index point, so collect and return
                    adder.add(docID);
                    break;
                } else if (cmp < 0) {
                    // Query point is before index point, so we move to next query point
                    nextQueryPoint = iterator.next();
                } else {
                    // Query point is after index point, so we don't collect and we return:
                    break;
                }
            }
        }

        @Override
        public Relation compare(byte[] minPackedValue, byte[] maxPackedValue) {
            while (nextQueryPoint != null) {
                scratch.bytes = minPackedValue;
                int cmpMin = nextQueryPoint.compareTo(scratch);
                if (cmpMin < 0) {
                    // query point is before the start of this cell
                    nextQueryPoint = iterator.next();
                    continue;
                }
                scratch.bytes = maxPackedValue;
                int cmpMax = nextQueryPoint.compareTo(scratch);
                if (cmpMax > 0) {
                    // query point is after the end of this cell
                    return Relation.CELL_OUTSIDE_QUERY;
                }

                if (cmpMin == 0 && cmpMax == 0) {
                    // NOTE: we only hit this if we are on a cell whose min and max values are exactly equal to our point,
                    // which can easily happen if many (> 1024) docs share this one value
                    return Relation.CELL_INSIDE_QUERY;
                } else {
                    return Relation.CELL_CROSSES_QUERY;
                }
            }

            // We exhausted all points in the query:
            return Relation.CELL_OUTSIDE_QUERY;
        }
    }
}
