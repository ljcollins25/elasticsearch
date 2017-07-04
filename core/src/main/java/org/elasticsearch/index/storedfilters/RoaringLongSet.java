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


import org.apache.lucene.search.DocIdSet;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.store.DataInput;
import org.apache.lucene.store.DataOutput;
import org.apache.lucene.util.BitDocIdSet;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.FixedBitSet;
import org.apache.lucene.util.RamUsageEstimator;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * BitSet implementation inspired from http://roaringbitmap.org/
 * <p>
 * The space is divided into blocks of 2^16 bits and each block is encoded
 * independently. In each block, if less than 2^12 bits are set, then
 * documents are simply stored in a short[]. If more than 2^16-2^12 bits are
 * set, then the inverse of the set is encoded in a simple short[]. Otherwise
 * a {@link FixedBitSet} is used.
 *
 */
public class RoaringLongSet {

    // Number of documents in a block
    private static final int BLOCK_SIZE = 1 << 16;
    // The maximum length for an array, beyond that point we switch to a bitset
    private static final int MAX_ARRAY_LENGTH = 1 << 12;
    private static final long BASE_RAM_BYTES_USED = RamUsageEstimator.shallowSizeOfInstance(RoaringLongSet.class);

    private enum BitSetType
    {
        NONE,
        SHORT_ARRAY,
        BIT
    }

    private final Bits[] bitSets;
    private final int cardinality;
    private final long minDoc;

    private RoaringLongSet(Bits[] docIdSets, int cardinality, long minDoc) {
        this.bitSets = docIdSets;
        this.minDoc = minDoc;
        this.cardinality = cardinality;
    }

    public void write(DataOutput output) throws IOException
    {
        output.writeVInt(cardinality);
        output.writeVInt(bitSets.length);
        output.writeVLong(minDoc);

        for (Bits set : bitSets)
        {
            if (set == null)
            {
                output.writeByte((byte) BitSetType.NONE.ordinal());
            }
            else if(set instanceof ShortArrayBitSet)
            {
                output.writeByte((byte) BitSetType.SHORT_ARRAY.ordinal());
                ShortArrayBitSet shortSet = (ShortArrayBitSet)set;
                shortSet.write(output);
            }
            else
            {
                output.writeByte((byte) BitSetType.BIT.ordinal());
                BitDocIdSet bitDocIdSet = (BitDocIdSet)set;
                FixedBitSet fixedBits = (FixedBitSet)set;
                long[] longs = fixedBits.getBits();

                output.writeVLong(bitDocIdSet.iterator().cost());
                output.writeVInt(fixedBits.length());
                output.writeVInt(longs.length);

                for (long l : longs)
                {
                    output.writeLong(l);
                }
            }
        }
    }

    public static RoaringLongSet read(DataInput input) throws  IOException
    {
        int cardinality = input.readVInt();
        int bitSetsLength = input.readVInt();
        long minDoc = input.readVLong();

        Bits[] bitSets = new Bits[bitSetsLength];
        for (int i = 0; i < bitSetsLength; i++) {
            BitSetType type = BitSetType.values()[input.readByte()];
            if (type == BitSetType.NONE)
            {
                bitSets[i] = null;
            }
            else if (type == BitSetType.SHORT_ARRAY)
            {
                bitSets[i] = ShortArrayBitSet.read(input);
            }
            else
            {
                long cost = input.readVLong();
                int numBits = input.readVInt();
                int longsLength = input.readVInt();

                long[] longs = new long[longsLength];
                for (int j = 0; j < longsLength; j++) {
                    longs[j] = input.readLong();
                }

                bitSets[i] = new FixedBitSet(longs, numBits);
            }
        }

        return new RoaringLongSet(bitSets, cardinality, minDoc);
    }

    public boolean get(long index) throws IOException {
        if (index < minDoc)
        {
            return false;
        }

        index -= minDoc;

        int block = (int)(index >>> 16);
        if (block >= bitSets.length)
        {
            return false;
        }

        Bits subBits = bitSets[block];
        if (subBits == null)
        {
            return false;
        }

        return subBits.get((int)(index & 0xFFFF));
    }

    /**
     * Return the exact number of documents that are contained in this set.
     */
    public int cardinality() {
        return cardinality;
    }

    @Override
    public String toString() {
        return "RoaringDocIdSet(cardinality=" + cardinality + ")";
    }

    /**
     * A builder of {@link RoaringLongSet}s.
     */
    public static class Builder {

        private long minDoc;
        private List<Bits> sets;

        private int cardinality;
        private long lastDocId;
        private int currentBlock;
        private int currentBlockCardinality;

        // We start by filling the buffer and when it's full we copy the content of
        // the buffer to the FixedBitSet and put further documents in that bitset
        private final short[] buffer;
        private FixedBitSet denseBuffer;

        /**
         * Sole constructor.
         */
        public Builder() {
            lastDocId = -1;
            currentBlock = -1;
            minDoc = -1;
            sets = new ArrayList<>();
            buffer = new short[MAX_ARRAY_LENGTH];
        }

        private void flush() {
            if (minDoc < 0)
            {
                // Empty
                minDoc = 0;
                return;
            }

            while (currentBlock >= sets.size())
            {
                sets.add(null);
            }

            assert currentBlockCardinality <= BLOCK_SIZE;
            if (currentBlockCardinality <= MAX_ARRAY_LENGTH) {
                // Use sparse encoding
                assert denseBuffer == null;
                if (currentBlockCardinality > 0) {
                    sets.set(currentBlock, new ShortArrayBitSet(Arrays.copyOf(buffer, currentBlockCardinality), false));
                }
            } else {
                assert denseBuffer != null;
                assert denseBuffer.cardinality() == currentBlockCardinality;
                if (denseBuffer.length() == BLOCK_SIZE && BLOCK_SIZE - currentBlockCardinality < MAX_ARRAY_LENGTH) {
                    // Doc ids are very dense, inverse the encoding
                    final short[] excludedDocs = new short[BLOCK_SIZE - currentBlockCardinality];
                    denseBuffer.flip(0, denseBuffer.length());
                    int excludedDoc = -1;
                    for (int i = 0; i < excludedDocs.length; ++i) {
                        excludedDoc = denseBuffer.nextSetBit(excludedDoc + 1);
                        assert excludedDoc != DocIdSetIterator.NO_MORE_DOCS;
                        excludedDocs[i] = (short) excludedDoc;
                    }
                    assert excludedDoc + 1 == denseBuffer.length() || denseBuffer.nextSetBit(excludedDoc + 1) == DocIdSetIterator.NO_MORE_DOCS;
                    sets.set(currentBlock, new ShortArrayBitSet(excludedDocs, true));
                } else {
                    // Neither sparse nor super dense, use a fixed bit set
                    sets.set(currentBlock, denseBuffer);
                }
                denseBuffer = null;
            }

            cardinality += currentBlockCardinality;
            denseBuffer = null;
            currentBlockCardinality = 0;
        }

        /**
         * Add a new doc-id to this builder.
         * NOTE: doc ids must be added in order.
         */
        public Builder add(long docId) {
            if (minDoc < 0)
            {
                minDoc = docId;
            }

            docId -= minDoc;

            if (docId <= lastDocId) {
                throw new IllegalArgumentException("Doc ids must be added in-order, got " + docId + " which is <= lastDocID=" + lastDocId);
            }
            final int block = (int)(docId >>> 16);
            if (block != currentBlock) {
                // we went to a different block, let's flush what we buffered and start from fresh
                flush();
                currentBlock = block;
            }

            if (currentBlockCardinality < MAX_ARRAY_LENGTH) {
                buffer[currentBlockCardinality] = (short) docId;
            } else {
                if (denseBuffer == null) {
                    // the buffer is full, let's move to a fixed bit set
                    denseBuffer = new FixedBitSet(1 << 16);
                    for (short doc : buffer) {
                        denseBuffer.set(doc & 0xFFFF);
                    }
                }
                denseBuffer.set((int)(docId & 0xFFFF));
            }

            lastDocId = docId;
            currentBlockCardinality += 1;
            return this;
        }

        /**
         * Add the content of the provided {@link DocIdSetIterator}.
         */
        public Builder add(DocIdSetIterator disi) throws IOException {
            for (int doc = disi.nextDoc(); doc != DocIdSetIterator.NO_MORE_DOCS; doc = disi.nextDoc()) {
                add(doc);
            }
            return this;
        }

        /**
         * Build an instance.
         */
        public RoaringLongSet build() {
            flush();

            return new RoaringLongSet(sets.toArray(new Bits[sets.size()]), cardinality, minDoc);
        }
    }

    /**
     * {@link Bits} implementation that can store documents up to 2^16-1 in a short[].
     */
    private static class ShortArrayBitSet implements Bits {

        private static final long BASE_RAM_BYTES_USED = RamUsageEstimator.shallowSizeOfInstance(ShortArrayBitSet.class);

        private final short[] docIDs;
        private final boolean invert;

        private ShortArrayBitSet(short[] docIDs, boolean invert) {
            // Normalize so that values are in order by absolute value
            // As is, values above Short.MAX_VALUE would become negative short values
            // So binary search would not work
            for (int i = 0; i < docIDs.length; ++i) {
                docIDs[i] = (short)(((int)docIDs[i] & 0xFFFF) + (int)Short.MIN_VALUE);
            }

            this.docIDs = docIDs;
            this.invert = invert;
        }

        public void write(DataOutput output) throws IOException
        {
            output.writeByte((byte)(invert ? 1 : 0));
            output.writeVInt(docIDs.length);

            short last = 0;
            for (short id : docIDs)
            {
                output.writeVInt(id - last);
                last = id;
            }
        }

        public static ShortArrayBitSet read(DataInput input) throws IOException
        {
            boolean invert = input.readByte() == 1;
            int docIDsLength = input.readVInt();
            short[] docIDs = new short[docIDsLength];

            short last = 0;
            for (int i = 0; i < docIDsLength; i++) {
                last += (short)input.readVInt();
                docIDs[i] = last;
            }

            return new ShortArrayBitSet(docIDs, invert);
        }

        @Override
        public boolean get(int index) {

            // Map from 0 -> 65535 to -32,768 -> 32,767 represented by shorts
            index += Short.MIN_VALUE;

            int result = Arrays.binarySearch(docIDs, (short)index);
            if (result >= 0)
            {
                return !invert;
            }
            else
            {
                return invert;
            }
        }

        @Override
        public int length() {
            return 0;
        }
    }
}
