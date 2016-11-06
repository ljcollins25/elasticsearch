package org.elasticsearch.index.storedfilters;

import org.apache.lucene.index.*;
import org.apache.lucene.search.*;
import org.apache.lucene.util.BytesRef;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * Created by lancec on 11/5/2016.
 */
public class FilterDataFields extends Fields {
    private Map<String, StoredFilterData> filterDataMap;
    private List<StoredFilterData> storedFilterDatas;
    private List<String> filterFieldName;
    private LeafReader reader;
    private IndexSearcher leafSearcher;

    // TODO: Add constructor
    // TODO: Call this during merge by replacing fields (ie FieldsProducer) for segment with a multi field including this
    // TODO: Should the doc id set be cached between instances? No. A segment should only be merging in one place a time.
    // TODO: Track which segments have filter data written so that data isn't serialized into _stored_filter_docs stored field
    // Two new fields:
    // _stored_filter (terms are filter names)
    // IndexOptions.DOCS (not stored)
    // _stored_filter_docs (no terms) - per segment document?
    // IndexOptions.NONE (stored)

    public RoaringDocIdSet getFilterDocs(Query filter) throws IOException {
        IndexReaderContext readerContext = leafSearcher.getTopReaderContext();
        RoaringDocIdSet.Builder docIdSetBuilder = new RoaringDocIdSet.Builder(reader.maxDoc());

        leafSearcher.search(filter, new Collector() {
            @Override
            public LeafCollector getLeafCollector(LeafReaderContext context) throws IOException {
                if (context != readerContext) {
                    return null;
                }

                return new LeafCollector() {
                    @Override
                    public void setScorer(Scorer scorer) throws IOException {
                    }

                    @Override
                    public void collect(int doc) throws IOException {
                        docIdSetBuilder.add(doc);
                    }
                };
            }

            @Override
            public boolean needsScores() {
                return false;
            }
        });

        return docIdSetBuilder.build();
    }

    @Override
    public Iterator<String> iterator() {
        return filterFieldName.iterator();
    }

    @Override
    public Terms terms(String field) throws IOException {
        if (field != filterFieldName.get(0))
        {
            return null;
        }

        StoredFilterData storedFilterData = filterDataMap.getOrDefault(field, null);
        if (storedFilterData == null){
            return null;
        }

        return null;
    }

    @Override
    public int size() {
        return filterFieldName.size();
    }

    private class FilterDataTerms extends Terms{

        @Override
        public TermsEnum iterator() throws IOException {
            return new FilterDataTermsEnum();
        }

        @Override
        public long size() throws IOException {
            return storedFilterDatas.size();
        }

        @Override
        public long getSumTotalTermFreq() throws IOException {
            return -1;
        }

        @Override
        public long getSumDocFreq() throws IOException {
            return -1;
        }

        @Override
        public int getDocCount() throws IOException {
            return -1;
        }

        @Override
        public boolean hasFreqs() {
            return false;
        }

        @Override
        public boolean hasOffsets() {
            return false;
        }

        @Override
        public boolean hasPositions() {
            return false;
        }

        @Override
        public boolean hasPayloads() {
            return false;
        }
    }

    private class FilterDataTermsEnum extends TermsEnum {
        private int ord = -1;
        private StoredFilterData current;

        @Override
        public PostingsEnum postings(PostingsEnum reuse, int flags) throws IOException {
            return new DocIdSetPostingsEnum(getFilterDocs(current.filter));
        }

        @Override
        public BytesRef next() throws IOException {
            ord++;
            if (ord < storedFilterDatas.size())
            {
                current = storedFilterDatas.get(ord);
                return current.filterNameBytes;
            }
            else
            {
                current = null;
            }

            return null;
        }

        @Override
        public SeekStatus seekCeil(BytesRef text) throws IOException {
            return SeekStatus.NOT_FOUND;
        }

        @Override
        public void seekExact(long ord) throws IOException {
            this.ord = (int)ord;
        }

        @Override
        public BytesRef term() throws IOException {
            return null;
        }

        @Override
        public long ord() throws IOException {
            return ord;
        }

        @Override
        public int docFreq() throws IOException {
            return -1;
        }

        @Override
        public long totalTermFreq() throws IOException {
            return -1;
        }
    }
}
