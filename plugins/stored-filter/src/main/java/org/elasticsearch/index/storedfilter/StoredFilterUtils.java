package org.elasticsearch.index.storedfilter;

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
import org.elasticsearch.index.mapper.SeqNoFieldMapper;

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

    // TODO: Add method for taking RoaringDocIdSet (i.e. IEnumerable<long>) for sequence numbers and getting doc ids
    // Set PointsInSetQuery

    private static final int PackedIntsVersion = PackedInts.VERSION_MONOTONIC_WITHOUT_ZIGZAG;

    private static final int BlockSize = 1 << 12;

    public static LongIterator getLongIterator(BytesRef bytes) throws IOException {
        IndexInput input = new ByteArrayIndexInput("StoredFilterBytes", bytes.bytes, bytes.offset, bytes.length);
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

    public static LongIterator getLongIterator(PackedLongValues packedLongValues) throws IOException {
        PackedLongValues.Iterator packedLongValuesIterator = packedLongValues.iterator();
        return new LongIterator() {
            private long currentValue = 0;

            @Override
            public boolean moveNext() throws IOException {
                if (packedLongValuesIterator.hasNext()) {
                    currentValue = packedLongValuesIterator.next();
                    return true;
                }
                return false;
            }

            @Override
            public long longValue() {
                return currentValue;
            }

            @Override
            public LongIterator newIterator() throws IOException {
                return getLongIterator(packedLongValues);
            }
        };
    }

    public static BytesRef getLongValuesBytes(LongIterator iterator) throws IOException {
        GrowableByteArrayDataOutput docsOutput = new GrowableByteArrayDataOutput(4096);

        // Write dummy count 0 which is be replaced after writing values is complete
        docsOutput.writeInt(0);

        MonotonicBlockPackedWriter writer = new MonotonicBlockPackedWriter(docsOutput, BlockSize);

        int count = 0;
        while (iterator.moveNext()) {
            count++;
            writer.add(iterator.longValue());
        }

        writer.finish();

        // Capture end position in order before reset
        int end = docsOutput.getPosition();

        // Reset and write actual count
        docsOutput.reset();
        docsOutput.writeInt(count);

        return new BytesRef(docsOutput.getBytes(), 0, end);
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
