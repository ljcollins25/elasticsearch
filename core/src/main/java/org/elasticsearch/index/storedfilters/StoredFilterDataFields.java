package org.elasticsearch.index.storedfilters;

import org.apache.lucene.index.*;
import org.apache.lucene.search.*;
import org.apache.lucene.util.BytesRef;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * Created by lancec on 11/5/2016.
 */
public class StoredFilterDataFields extends Fields {

    private final static StoredFilterData[] EMPTY_ARRAY = new StoredFilterData[0];

    // This list only contains a single element for StoredFilterUtils.STORED_FILTER_TERM_FIELD_NAME
    // It is only here to provide an iterator to pass out for iterator()
    private List<String> filterFieldName;
    private LeafReader reader;
    private IndexSearcher leafSearcher;
    private StoredFilterData[] storedFilterDatas;

    // TODO: Call this during merge by replacing fields (ie FieldsProducer) for segment with a multi field including this
    // TODO: Should the doc id set be cached between instances? No. A segment should only be merging in one place a time.
    // TODO: Track which segments have filter data written so that data isn't serialized into _stored_filter_docs stored field

    public StoredFilterDataFields(LeafReader reader, List<StoredFilterDocsProvider> storedFilterDatas)
    {
        filterFieldName = new ArrayList<>();
        filterFieldName.add(StoredFilterUtils.STORED_FILTER_TERM_FIELD_NAME);
        storedFilterDatas.sort((o1, o2) -> o1.filterTerm().compareTo(o2.filterTerm()));
        this.storedFilterDatas = storedFilterDatas.toArray(EMPTY_ARRAY);
        this.reader = reader;
        this.leafSearcher = new IndexSearcher(reader);
    }

    public RoaringDocIdSet getFilterDocs(Query filter) throws IOException {
        IndexReaderContext readerContext = reader.getContext();
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
        if (field != StoredFilterUtils.STORED_FILTER_TERM_FIELD_NAME)
        {
            return null;
        }

        return new FilterDataTerms();
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
            return storedFilterDatas.length;
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
        private int currentTermOrd = -1;
        private StoredFilterData current;

        @Override
        public PostingsEnum postings(PostingsEnum reuse, int flags) throws IOException {
            return new DocIdSetPostingsEnum(getFilterDocs(current.filter));
        }

        @Override
        public BytesRef next() throws IOException {
            currentTermOrd++;
            updateCurrent();
            return term();
        }

        private void updateCurrent()
        {
            if (currentTermOrd < storedFilterDatas.length)
            {
                current = storedFilterDatas[currentTermOrd];
            }
            else
            {
                current = null;
            }
        }

        // Seek methods copied (with some minor changes_ from DirectPostingsFormat
        // If non-negative, exact match; else, -currentTermOrd-1, where currentTermOrd
        // is where you would insert the term.
        private int findTerm(BytesRef term) {

            // Just do binary search: should be (constant factor)
            // faster than using the skip list:
            int low = 0;
            int high = storedFilterDatas.length-1;

            while (low <= high) {
                int mid = (low + high) >>> 1;
                int cmp = compare(mid, term);
                if (cmp < 0) {
                    low = mid + 1;
                } else if (cmp > 0) {
                    high = mid - 1;
                } else {
                    return mid; // key found
                }
            }

            return -(low + 1);  // key not found.
        }

        // Compares in unicode (UTF8) order:
        int compare(int ord, BytesRef other) {
            final byte[] otherBytes = other.bytes;

            StoredFilterData storedFilterData = storedFilterDatas[ord];
            BytesRef termBytes = storedFilterData.filterNameBytes;
            int upto = termBytes.offset;
            final int termLen = termBytes.length;
            int otherUpto = other.offset;

            final int stop = upto + Math.min(termLen, other.length);
            while (upto < stop) {
                int diff = (termBytes.bytes[upto++] & 0xFF) - (otherBytes[otherUpto++] & 0xFF);
                if (diff != 0) {
                    return diff;
                }
            }

            // One is a prefix of the other, or, they are equal:
            return termLen - other.length;
        }

        @Override
        public SeekStatus seekCeil(BytesRef term) {
            final int ord = findTerm(term);
            if (ord >= 0) {
                currentTermOrd = ord;
                updateCurrent();
                return SeekStatus.FOUND;
            } else if (ord == -storedFilterDatas.length-1) {
                return SeekStatus.END;
            } else {
                currentTermOrd = -ord - 1;
                updateCurrent();
                return SeekStatus.NOT_FOUND;
            }
        }

        @Override
        public boolean seekExact(BytesRef term) {
            final int ord = findTerm(term);
            if (ord >= 0) {
                currentTermOrd = ord;
                updateCurrent();
                return true;
            } else {
                return false;
            }
        }

        @Override
        public void seekExact(long ord) throws IOException {
            this.currentTermOrd = (int)ord;
            updateCurrent();
        }

        @Override
        public BytesRef term() throws IOException {
            if (current == null) return null;
            else return current.filterNameBytes;
        }

        @Override
        public long ord() throws IOException {
            return currentTermOrd;
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
