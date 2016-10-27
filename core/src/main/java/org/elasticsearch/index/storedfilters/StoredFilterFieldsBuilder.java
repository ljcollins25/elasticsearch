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

package org.elasticsearch.index.storedfilters;

import org.apache.lucene.codecs.FieldsProducer;
import org.apache.lucene.index.*;
import org.apache.lucene.search.*;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FlushInfo;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.util.*;
import org.elasticsearch.index.shard.CommitPoint;

import java.io.Closeable;
import java.io.IOException;
import java.util.*;

/**
 *
 **/
public class StoredFilterFieldsBuilder {
    static final BytesRef EMPTY_BYTES = new BytesRef(0);

    private Query filter;
    private IndexSearcher searcher;
    private BytesRef filterName;
    private Map<String, Terms> segmentTerms;

    private Map<String, IndexSearcher> segmentSearchers = new HashMap<>();
    private Map<String, SegmentReader> segmentReaders = new HashMap<>();
    private Map<String, Query> filtersByName = new HashMap<>();

    // The set of all segments that are in this commit. Data for segments
    // not in this list should be removed or not be stored in the stored filters
    // file
    private Set<String> activeSegments;

    // The set of segments that are new in this commit which will directly
    // store the filters
    private Set<String> newSegments;

    private Fields priorFields;

    private List<FilterData> filters = new ArrayList<>();

    public StoredFilterFieldsBuilder(String filterName, IndexSearcher searcher, Query filter) {
        this.filter = filter;
        this.filterName = new BytesRef(filterName);
        this.searcher = searcher;
        this.segmentTerms = new HashMap<>();
    }

    public void AddFilter(String name, Query filter)
    {

    }

    public void DeleteFilter(String name)
    {

    }

    private class FilterData implements Comparable<FilterData>
    {
        public String name;
        public BytesRef nameBytes;
        public Query query;
        public Set<String> excludedSegments;

        @Override
        public int compareTo(FilterData o) {
            return nameBytes.compareTo(o.nameBytes);
        }

    }

    public void writeFields(IndexWriter writer, Directory directory, CommitPoint commitPoint, SegmentInfos segments) throws IOException {
        DirectoryReader reader = DirectoryReader.open(writer);

        // Get set of active segments
        for (SegmentCommitInfo segmentInfo : segments)
        {
            activeSegments.add(segmentInfo.info.name);
        }

        Collections.sort(filters);


    }

    public SegmentWriteState createStoredFilterSegmentWriteState(IndexReader reader, IndexWriter writer, int maxDoc)
    {
        SegmentInfos infos = null;
        String segmentFileName = IndexFileNames.fileNameFromGeneration("storedfilters", "", infos.getLastGeneration());

        SegmentInfo segmentInfo = new SegmentInfo(writer.getDirectory(),
            Version.LATEST,
            segmentFileName,
            maxDoc,
            false,
            writer.getConfig().getCodec(),
            Collections.emptyMap(),
            StringHelper.randomId(),
            new HashMap<>(),
            null);

        FieldInfo[] fields = new FieldInfo[activeSegments.size()];
        int i = 0;
        for (String segmentName : activeSegments)
        {
            fields[i] = new FieldInfo(
                segmentName,
                i,
                false,
                true,
                false,
                IndexOptions.DOCS,
                DocValuesType.NONE,
                -1,
                Collections.emptyMap(),
                0, 0);
            i++;
        }

        FieldInfos fieldInfos = new FieldInfos(fields);
        return new SegmentWriteState(InfoStream.NO_OUTPUT, writer.getDirectory(), segmentInfo, fieldInfos, null,
            new IOContext(new FlushInfo(maxDoc, maxDoc * activeSegments.size())));
    }

    public Fields buildFields()
    {
        int maxDoc = 0;
        for (LeafReaderContext context : searcher.getIndexReader().leaves()) {
            SegmentReader reader = segmentReader(context.reader());
            maxDoc = Math.max(maxDoc, reader.maxDoc());
        }

        return new Fields() {
            @Override
            public Iterator<String> iterator() {
                return activeSegments.iterator();
            }

            @Override
            public Terms terms(String field) throws IOException {
                return buildTerms(field);
            }

            @Override
            public int size() {
                return segmentTerms.size();
            }
        };
    }

    public Terms buildTerms(String segmentName)
    {
        SegmentReader segmentReader = segmentReaders.getOrDefault(segmentName, null);
        if (segmentReader == null)
        {
            return null;
        }

        // TODO: Write logic for getting the terms for a segment



        return null;
    }


    public Terms buildNewFilterTerms(String segmentName, IndexSearcher segmentSearcher)
    {

        return null;
    }

    private class StoredFilterTerms extends Terms
    {

        @Override
        public TermsEnum iterator() throws IOException {
            return new StoredFilterTermsEnum();
        }

        @Override
        public long size() throws IOException {
            return filters.size();
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

    private class StoredFilterTermsEnum extends  TermsEnum
    {
        @Override
        public SeekStatus seekCeil(BytesRef text) throws IOException {
            return null;
        }

        @Override
        public void seekExact(long ord) throws IOException {

        }

        @Override
        public BytesRef term() throws IOException {
            return null;
        }

        @Override
        public long ord() throws IOException {
            return 0;
        }

        @Override
        public int docFreq() throws IOException {
            return 0;
        }

        @Override
        public long totalTermFreq() throws IOException {
            return 0;
        }

        @Override
        public PostingsEnum postings(PostingsEnum reuse, int flags) throws IOException {
            return null;
        }

        @Override
        public BytesRef next() throws IOException {
            return null;
        }
    }



    public Fields build() throws IOException {

        int maxDoc = 0;
        for (LeafReaderContext context : searcher.getIndexReader().leaves()) {
            SegmentReader reader = segmentReader(context.reader());
            maxDoc = Math.max(maxDoc, reader.maxDoc());
        }

        Map<String, RoaringDocIdSet.Builder> segmentDocs = new HashMap<>();
        searcher.search(filter, new Collector() {
            @Override
            public LeafCollector getLeafCollector(LeafReaderContext context) throws IOException {
                SegmentReader reader = segmentReader(context.reader());

                RoaringDocIdSet.Builder docIdSetBuilder = new RoaringDocIdSet.Builder(reader.maxDoc());
                segmentDocs.put(reader.getSegmentName(), docIdSetBuilder);

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

        for (String segment : segmentDocs.keySet())
        {
            Terms segmentTerm = createSegmentTerm(segmentDocs.get(segment));
            segmentTerms.put(segment, segmentTerm);
        }

        return new Fields() {
            @Override
            public Iterator<String> iterator() {
                return segmentTerms.keySet().iterator();
            }

            @Override
            public Terms terms(String field) throws IOException {
                return segmentTerms.getOrDefault(field, null);
            }

            @Override
            public int size() {
                return segmentTerms.size();
            }
        };
    }

    private Terms createSegmentTerm(RoaringDocIdSet.Builder builder) {
        RoaringDocIdSet segmentDocs = builder.build();
        return new SegmentTerms(segmentDocs);
    }

    /**
     * Tries to extract a segment reader from the given index reader.
     * If no SegmentReader can be extracted an {@link IllegalStateException} is thrown.
     */
    protected static SegmentReader segmentReader(LeafReader reader) {
        if (reader instanceof SegmentReader) {
            return (SegmentReader) reader;
        } else if (reader instanceof FilterLeafReader) {
            final FilterLeafReader fReader = (FilterLeafReader) reader;
            return segmentReader(FilterLeafReader.unwrap(fReader));
        }
        // hard fail - we can't get a SegmentReader
        throw new IllegalStateException("Can not extract segment reader from given index reader [" + reader + "]");
    }

    private class SegmentTerms extends Terms {

        private final RoaringDocIdSet segmentDocs;
        private final int docCount;

        public SegmentTerms(RoaringDocIdSet segmentDocs)
        {
            this.segmentDocs = segmentDocs;
            this.docCount = segmentDocs.cardinality();
        }

        @Override
        public TermsEnum iterator() throws IOException {

            return new TermsEnum() {
                int pos = -1;

                @Override
                public SeekStatus seekCeil(BytesRef text) throws IOException {
                    return TermsEnum.EMPTY.seekCeil(text);
                }

                @Override
                public void seekExact(long ord) throws IOException {
                    TermsEnum.EMPTY.seekExact(ord);
                }

                @Override
                public BytesRef term() throws IOException {
                    return pos == 0 ? filterName : null;
                }

                @Override
                public long ord() throws IOException {
                    return -1;
                }

                @Override
                public int docFreq() throws IOException {
                    return pos == 0 ? docCount : TermsEnum.EMPTY.docFreq();
                }

                @Override
                public long totalTermFreq() throws IOException {
                    return -1;
                }

                @Override
                public BytesRef next() throws IOException {
                    pos++;
                    return term();
                }

                @Override
                public PostingsEnum postings(PostingsEnum reuse, int flags) throws IOException {
                    if (pos == 0) {
                        return TermsEnum.EMPTY.postings(reuse, flags);
                    }

                    final DocIdSetIterator segmentDocsIter = SegmentTerms.this.segmentDocs.iterator();

                    return new PostingsEnum() {

                        @Override
                        public int freq() throws IOException {
                            return -1;
                        }

                        @Override
                        public int nextPosition() throws IOException {
                            return -1;
                        }

                        @Override
                        public int startOffset() throws IOException {
                            return -1;
                        }

                        @Override
                        public int endOffset() throws IOException {
                            return -1;
                        }

                        @Override
                        public BytesRef getPayload() throws IOException {
                            return null;
                        }

                        @Override
                        public int docID() {
                            return segmentDocsIter.docID();
                        }

                        @Override
                        public int nextDoc() throws IOException {
                            return segmentDocsIter.nextDoc();
                        }

                        @Override
                        public int advance(int target) throws IOException {
                            return segmentDocsIter.advance(target);
                        }

                        @Override
                        public long cost() {
                            return segmentDocsIter.cost();
                        }
                    };
                }
            };
        }

        @Override
        public long size() throws IOException {
            return 1;
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
            return docCount;
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
}
