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

package org.apache.lucene.codecs.autoprefix;

import java.io.IOException;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.search.ConstantScoreScorer;
import org.apache.lucene.search.ConstantScoreWeight;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.PrefixQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.Weight;
import org.apache.lucene.util.StringHelper;

/**
 * A Query that matches documents containing terms with a specified prefix.
 * Works only with auto-prefix fields built by {@link AutoPrefixFieldsConsumer}.
 */
public class AutoPrefixQuery extends Query {
    private final Term prefix;

    public AutoPrefixQuery(Term prefix) {
        this.prefix = prefix;
    }

    @Override
    public Weight createWeight(IndexSearcher searcher, boolean needsScores) throws IOException {
        return new AutoPrefixWeight(this, searcher);
    }

    @Override
    public String toString(String field) {
        return null;
    }

    /** Returns true iff <code>o</code> is equal to this. */
    @Override
    public boolean equals(Object other) {
        return sameClassAs(other) &&
            prefix.equals(((AutoPrefixQuery) other).prefix);
    }

    @Override
    public int hashCode() {
        return classHash() ^ prefix.hashCode();
    }

    final class AutoPrefixWeight extends ConstantScoreWeight {
        final IndexSearcher searcher;
        protected AutoPrefixWeight(AutoPrefixQuery query, IndexSearcher searcher) {
            super(query);
            this.searcher = searcher;
        }

        protected Scorer getScorer(LeafReaderContext context, Terms prefixTerms) throws IOException {
            TermsEnum prefixEnum = prefixTerms.iterator();
            TermsEnum.SeekStatus status = prefixEnum.seekCeil(prefix.bytes());
            if (status == TermsEnum.SeekStatus.FOUND) {
                // this is an exact match so we need only one inverted list.
                return new ConstantScoreScorer(this, 1.0f, prefixEnum.postings(null, 0));
            }
            if (status != TermsEnum.SeekStatus.END && StringHelper.startsWith(prefixEnum.term(), prefix.bytes())) {
                // the auto-prefix is bigger than the requested prefix but it's also the first match so we can use this
                // inverted list to answer the query.
                return new ConstantScoreScorer(this, 1.0f, prefixEnum.postings(null, 0));
            }

            // No match
            return null;
        }

        @Override
        public Scorer scorer(LeafReaderContext context) throws IOException {
            Terms terms = context.reader().terms(prefix.field());
            return getScorer(context, terms);
        }
    }
}
