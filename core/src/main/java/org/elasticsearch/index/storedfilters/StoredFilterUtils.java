package org.elasticsearch.index.storedfilters;

import org.apache.lucene.document.*;
import org.apache.lucene.index.*;
import org.apache.lucene.search.*;
import org.apache.lucene.store.GrowableByteArrayDataOutput;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.util.*;
import org.apache.lucene.util.packed.MonotonicBlockPackedReader;
import org.apache.lucene.util.packed.MonotonicBlockPackedWriter;
import org.apache.lucene.util.packed.PackedInts;
import org.apache.lucene.util.packed.PackedLongValues;
import org.elasticsearch.common.lucene.store.ByteArrayIndexInput;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.engine.EngineException;
import org.elasticsearch.index.mapper.ParseContext;
import org.elasticsearch.index.mapper.SeqNoFieldMapper;
import org.elasticsearch.index.mapper.StoredFilterFieldMapper;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;

import static org.apache.lucene.search.DocIdSetIterator.NO_MORE_DOCS;

/**
 * Created by lancec on 11/12/2016.
 */
public class StoredFilterUtils {

    // These are fields for the filter document

    // The field in the stored filter document specifying the filter name
    // This is assumed to be added to filter document before entering the
    // stored filter manager.
    // TODO: Maybe this should be added by the stored filter manager? Depends on
    // TODO: stored filters are added
    public static final String STORED_FILTER_NAME_FIELD_NAME = "_stored_filter_name";

    // The field storing sequence numbers of documents matching the stored filter
    public static final String STORED_FILTER_SEQ_NOS_FIELD_NAME = "_stored_filter_seq_nos";

    // The field under which the stored filter query is stored
    public static final String STORED_FILTER_QUERY_FIELD_NAME = "_stored_filter_query";

    private static final Set<String> STORED_FILTER_SEQ_NOS_FIELD_NAME_SET = Collections.singleton(STORED_FILTER_SEQ_NOS_FIELD_NAME);

    // TODO: Add method for taking RoaringDocIdSet (i.e. IEnumerable<long>) for sequence numbers and getting doc ids
    // Set PointsInSetQuery

    private static final int PackedIntsVersion = PackedInts.VERSION_MONOTONIC_WITHOUT_ZIGZAG;

    private static final int BlockSize = 1 << 12;

    public static void registerStoredFilter(Engine engine, Engine.Index index, boolean update) {
        String filterName = index.id();
        ParseContext.Document doc = index.docs().get(0);
        StoredFilterFieldMapper.StoredFilterQueryField filterField =
            (StoredFilterFieldMapper.StoredFilterQueryField)doc.getField(StoredFilterUtils.STORED_FILTER_QUERY_FIELD_NAME);

        registerStoredFilter(engine, filterName, filterField.query(), doc, update ? index.uid() : null);
    }

    public static void registerStoredFilter(Engine engine, String filterName, Query filter, ParseContext.Document document, Term uid) {
        // Add filter to map. StoredFilterQuery's after this point will return the
        // filter query until the filter is removed
        StoredFilterData filterData = new StoredFilterData(filterName, filter, document, uid);
        storeFilter(engine, filterData);
    }

    // New fields:
    // STORED_FILTER_SEQ_NOS_FIELD_NAME (_stored_filter_seq_nos) - the seq number set for the segments

    public static void storeFilter(Engine engine, StoredFilterData filterData) {

        filterData.document.add(new StringField(StoredFilterUtils.STORED_FILTER_NAME_FIELD_NAME, filterData.filterName.string(), Field.Store.YES));

        Engine.Searcher searcher = null;

        try {
            searcher = engine.acquireSearcher("storeFilter");

            StoredFilterUtils.addStoredFilterSequenceNumbersField(filterData, searcher.searcher());
        }
        catch (IOException ex) { }
        catch (EngineException ex) { }
        finally
        {
            if (searcher != null)
            {
                searcher.close();
            }
        }
    }

    public static LongIterator loadSequenceNumbersField(IndexSearcher searcher, int docId) throws IOException {
        Document document = searcher.doc(docId, STORED_FILTER_SEQ_NOS_FIELD_NAME_SET);
        IndexableField sequenceNoField = document.getField(StoredFilterUtils.STORED_FILTER_SEQ_NOS_FIELD_NAME);
        BytesRef bytes = sequenceNoField.binaryValue();
        LongIterator iterator = getLongIterator(bytes);

        return iterator;
    }

    private static LongIterator getLongIterator(BytesRef bytes) throws IOException {
        IndexInput input = new ByteArrayIndexInput(STORED_FILTER_SEQ_NOS_FIELD_NAME, bytes.bytes, bytes.offset, bytes.length);
        long valueCount = input.readVLong();
        MonotonicBlockPackedReader reader = MonotonicBlockPackedReader.of(input, PackedIntsVersion, BlockSize, valueCount, false);

        return new LongIterator() {
            private int index = -1;

            @Override
            public boolean moveNext() throws IOException {
                index++;
                return index < reader.size();
            }

            @Override
            public long longValue() {
                return reader.get(index);
            }

            @Override
            public LongIterator newIterator() throws IOException {
                return getLongIterator(bytes);
            }
        };
    }

    public static void addStoredFilterSequenceNumbersField(StoredFilterData filterData, IndexSearcher searcher) throws IOException {

        //SequenceNoCollector collector = new SequenceNoCollector();
        OrderedSequenceNoCollector collector = new OrderedSequenceNoCollector();

        searcher.search(filterData.filter, collector);

        PackedLongValues matchingSequenceNumbers = collector.build();

        GrowableByteArrayDataOutput docsOutput = new GrowableByteArrayDataOutput(4096);

        PackedLongValues.Iterator iterator = matchingSequenceNumbers.iterator();

        docsOutput.writeVLong(matchingSequenceNumbers.size());

        MonotonicBlockPackedWriter writer = new MonotonicBlockPackedWriter(docsOutput, BlockSize);

        while (iterator.hasNext()) {
            writer.add(iterator.next());
        }

        writer.finish();

        filterData.document.add(new StoredField(
            STORED_FILTER_SEQ_NOS_FIELD_NAME,
            new BytesRef(docsOutput.getBytes(), 0, docsOutput.getPosition())));
    }

    private static class OrderedSequenceNoCollector implements Collector
    {
        private List<Sub> subCollectors = new ArrayList<>();

        private PackedLongValues build() throws IOException {
            PriorityQueue<Sub> mergeQueue = new PriorityQueue<Sub>(subCollectors.size()) {
                @Override
                protected boolean lessThan(Sub a, Sub b) {
                    return a.longValue() < b.longValue();
                }
            };

            for (Sub sub : subCollectors)
            {
                sub.build();
                mergeQueue.add(sub);
            }

            int pageSize = 1 << 12;
            PackedLongValues.Builder builder = PackedLongValues.monotonicBuilder(pageSize, PackedInts.FAST);

            Sub top = mergeQueue.top();
            while (mergeQueue.size() != 0) {
                long topValue = top.longValue();
                int docID = top.nextDoc();
                if (docID == NO_MORE_DOCS) {
                    mergeQueue.pop();
                    top = mergeQueue.top();
                }
                else {
                    top = mergeQueue.updateTop();
                }

                // if top value is negative, then top was not initialized
                // so don't add it to the builder
                if (topValue >= 0) {
                    builder.add(topValue);
                }
            }

            return builder.build();
        }

        @Override
        public LeafCollector getLeafCollector(LeafReaderContext context) throws IOException {

            SortedDocValues sortedSeqNoDocValues = context.reader().getSortedDocValues(SeqNoFieldMapper.SEQ_NO_SORTED_NAME);
            FixedBitSet bitSet = new FixedBitSet(sortedSeqNoDocValues.getValueCount());
            int initialValue = Integer.MIN_VALUE + subCollectors.size();
            Sub subCollector = new Sub()
            {
                int cost = 0;
                DocIdSetIterator iterator;
                long currentValue = initialValue;

                @Override
                public void build() throws IOException{
                    iterator = new BitSetIterator(bitSet, cost);
                }

                @Override
                public void setScorer(Scorer scorer) throws IOException {
                }

                @Override
                public void collect(int doc) throws IOException {
                    if (sortedSeqNoDocValues.advanceExact(doc)) {
                        cost++;
                        bitSet.set(sortedSeqNoDocValues.ordValue());
                    }
                }

                @Override
                public int nextDoc() throws IOException {
                    int doc = iterator.nextDoc();
                    if (doc != DocIdSetIterator.NO_MORE_DOCS)
                    {
                        BytesRef bytesValue = sortedSeqNoDocValues.lookupOrd(iterator.docID());
                        currentValue = LongPoint.decodeDimension(bytesValue.bytes, 0);
                    }
                    else {
                        currentValue = -1;
                    }

                    return doc;
                }

                @Override
                public long longValue() {
                    return currentValue;
                }
            };

            subCollectors.add(subCollector);
            return subCollector;
        }

        @Override
        public boolean needsScores() {
            return false;
        }

        private static abstract class Sub implements LeafCollector
        {
            public abstract void build() throws IOException;

            public abstract int nextDoc() throws IOException;

            public abstract long longValue();
        }
    }


    private static class SequenceNoCollector implements Collector {
        private long lastSequenceNumber = -1;
        private long count = 0;
        private final int maxBufferSize = 100000;

        PackedLongValues.Builder matchingSequenceNumbers;
        LongList sequenceNumbersBuffer = new LongList();

        public SequenceNoCollector() {
            reset();
        }

        private void reset() {
            int pageSize = 1 << 12;
            matchingSequenceNumbers = PackedLongValues.monotonicBuilder(pageSize, PackedInts.FAST);
        }

        private PackedLongValues build() throws IOException {
            merge();
            return matchingSequenceNumbers.build();
        }

        @Override
        public LeafCollector getLeafCollector(LeafReaderContext context) throws IOException {
            NumericDocValues seqNos = context.reader().getNumericDocValues(SeqNoFieldMapper.NAME);

            return new LeafCollector() {
                @Override
                public void setScorer(Scorer scorer) throws IOException {
                }

                @Override
                public void collect(int doc) throws IOException {
                    if (seqNos.advanceExact(doc)) {
                        long seqNo = seqNos.longValue();
                        if (seqNo > lastSequenceNumber) {
                            add(seqNo);
                        } else if (seqNo != lastSequenceNumber) {
                            if (sequenceNumbersBuffer.size() >= maxBufferSize) {
                                merge();
                            }

                            sequenceNumbersBuffer.add(seqNo);
                        }
                    }
                }
            };
        }

        private void add(long sequenceNumber) {
            if (sequenceNumber != lastSequenceNumber) {
                matchingSequenceNumbers.add(sequenceNumber);
                count++;
                lastSequenceNumber = sequenceNumber;
            }
        }

        private void merge() throws IOException {
            if (sequenceNumbersBuffer.size() != 0) {
                merge(new PackedLongIterator(matchingSequenceNumbers.build()), sequenceNumbersBuffer.iterator());
                sequenceNumbersBuffer.clear();
            }
        }

        private void merge(LongIterator iterator1, LongIterator iterator2) throws IOException {
            reset();

            boolean initialize = true;

            LongIterator remainingIterator = null;

            while (true) {
                if (!initialize) {
                    if (iterator1.longValue() < iterator2.longValue()) {
                        add(iterator1.longValue());
                        if (!iterator1.moveNext()) {
                            add(iterator2.longValue());
                            remainingIterator = iterator2;
                            break;
                        }

                        continue;
                    } else if (iterator2.longValue() < iterator1.longValue()) {
                        add(iterator2.longValue());
                        if (!iterator2.moveNext()) {
                            add(iterator1.longValue());
                            remainingIterator = iterator1;
                            break;
                        }

                        continue;
                    } else {
                        add(iterator1.longValue());
                    }
                }

                initialize = false;
                if (!iterator1.moveNext()) {
                    remainingIterator = iterator2;
                    break;
                } else if (!iterator2.moveNext()) {
                    add(iterator1.longValue());
                    remainingIterator = iterator1;
                    break;
                }
            }

            while (remainingIterator.moveNext()) {
                add(remainingIterator.longValue());
            }
        }

        @Override
        public boolean needsScores() {
            return false;
        }

    }
}
