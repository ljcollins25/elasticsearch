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

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import org.apache.lucene.index.FilterLeafReader;
import org.apache.lucene.index.PostingsEnum;
import org.apache.lucene.index.SegmentWriteState;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.search.DocIdSet;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.DocIdSetBuilder;
import org.apache.lucene.util.FixedBitSet;
import org.apache.lucene.util.IOUtils;
import org.apache.lucene.util.packed.MonotonicBlockPackedReader;
import org.apache.lucene.util.packed.MonotonicBlockPackedWriter;
import org.apache.lucene.util.packed.PackedInts;

/**
 * Used to build auto-prefix terms and their associated inverted lists from a {@link TermsEnum}.
 * This is done in two pass, the first pass builds a compact prefix tree.
 * Since the terms enum is sorted the prefixes are flushed on the fly depending on the input.
 * For each prefix we build its corresponding inverted lists using a {@link DocIdSetBuilder}.
 * The first pass visits each term of the specified {@link TermsEnum} only once.
 * When a prefix is flushed from the prefix tree its inverted lists is dumped into a temporary file for further use.
 * This is necessary since the prefixes are not sorted when they are removed from the tree.
 * The selected auto prefixes are sorted at the end of the first pass.
 * The second pass is a sorted scan of the prefixes and the temporary file is used to read the corresponding inverted lists.
 **/
public class AutoPrefixTermsBuilder implements Closeable {
    static final BytesRef EMPTY_BYTES = new BytesRef(0);

    private final Directory directory;
    private final Terms terms;
    private final int minTermsInAutoPrefix;
    private final long maxAutoTermSize;
    private final int maxDoc;

    private List<PrefixTerm> prefixes = new ArrayList<>();
    // The root of the compact prefix tree
    private AutoPrefixNode root;
    private final FixedBitSet docsSeen;
    private PostingsEnum reusePE;

    private final String tempFileNamePrefix;
    private IndexInput input;
    // keep track of the temporary file name for deletion
    private String name;

    public AutoPrefixTermsBuilder(SegmentWriteState state, Terms terms)  {
        this(state, terms, 2, Integer.MAX_VALUE);
    }

    public AutoPrefixTermsBuilder(SegmentWriteState state, Terms terms, int minTermsInAutoPrefix, int maxAutoTermSize) {
        this.directory = state.directory;
        this.terms = terms;
        this.maxDoc = state.segmentInfo.maxDoc();
        this.docsSeen = new FixedBitSet(maxDoc);
        this.tempFileNamePrefix = state.segmentInfo.name;
        this.minTermsInAutoPrefix = minTermsInAutoPrefix;
        this.maxAutoTermSize = maxAutoTermSize;
    }

    public Terms build() throws IOException {
        boolean success = false;
        // First pass
        // we push the terms into the compact prefix tree and flush the prefixes on the fly.
        try (IndexOutput output = directory.createTempOutput(tempFileNamePrefix, "autoprefix", IOContext.DEFAULT)) {
            name = output.getName();
            TermsEnum termsEnum = terms.iterator();
            while (termsEnum.next() != null) {
                reusePE = termsEnum.postings(reusePE, 0);
                DocIdSetBuilder docIdSetB = newDocIdSetBuilder();
                while (reusePE.nextDoc() != DocIdSetIterator.NO_MORE_DOCS) {
                    docIdSetB.grow(1).add(reusePE.docID());
                }
                BytesRef term = termsEnum.term();
                int max = (int) Math.min(term.length, maxAutoTermSize+1);
                term.length = max;
                pushTerm(term, docIdSetB.build(), output);
            }
            if (root != null) {
                flushPrefix(EMPTY_BYTES, root, output);
            }
            success = true;
        } finally {
            if (success == false && name != null) {
                IOUtils.deleteFiles(directory, name);
            }
        }
        // End of the first pass
        // Sort the selected prefixes
        Collections.sort(prefixes);
        // Now we can read the sorted prefixes and their inverted lists
        input = directory.openInput(name, IOContext.READONCE);
        // The second pass is the enumeration of the sorted prefixes
        return new TermsReader();
    }

    @Override
    public void close() throws IOException {
        // close the temporary file and delete it if needed.
        if (input != null) {
            input.close();
            input = null;
        }
        if (name != null) {
            IOUtils.deleteFilesIgnoringExceptions(directory, name);
            name = null;
        }
    }

    /**
     * Adds the specified term in the compact prefix tree and flushes the prefixes smaller than the term.
     * @warning The terms must be added in sorted order.
     */
    private void pushTerm(BytesRef term, DocIdSet docIdSet, IndexOutput output) throws IOException {
        if (root == null) {
            this.root = new AutoPrefixNode(null, BytesRef.deepCopyOf(term), 1, or(docIdSet, newDocIdSetBuilder()));
            return;
        }
        innerPushTerm(root, EMPTY_BYTES, term, docIdSet, output);
    }

    private void innerPushTerm(AutoPrefixNode node, BytesRef prefix, BytesRef suffix,
                               DocIdSet docIdSet, IndexOutput output) throws IOException {
        int prefixLen = findLargestPrefix(suffix, node.prefix);
        assert prefixLen != suffix.length : "Found duplicate entry";
        if (prefixLen == 0) {
            // Nothing in common, we can flush this node and replace it with the suffix
            assert suffix.compareTo(node.prefix) > 0 : "Not sorted";
            flushPrefix(prefix, node, output);
            node.prefix = BytesRef.deepCopyOf(suffix);
            node.children.clear();
            node.numTerms = 1;
            node.docIdSetB = or(docIdSet, newDocIdSetBuilder());
            return;
        }


        if (prefixLen < suffix.length) {
            node.numTerms += 1;
            // The suffix candidate and the prefix located in this node share a prefix
            BytesRef nextSuffix = new BytesRef(suffix.bytes, suffix.offset + prefixLen, suffix.length - prefixLen);
            if (node.prefix.length == prefixLen) {
                // The prefix located in this node is a prefix of the suffix candidate so we need to check if
                // there's a child that can possibly share the remaining suffix.
                boolean found = false;
                BytesRef newPrefix = concatenate(prefix, node.prefix);
                Iterator<AutoPrefixNode> it = node.children.iterator();
                while (it.hasNext()) {
                    AutoPrefixNode child = it.next();
                    if (child.prefix.bytes[child.prefix.offset] == nextSuffix.bytes[nextSuffix.offset]) {
                        assert found == false : "Found twice";
                        innerPushTerm(child, newPrefix, nextSuffix, docIdSet, output);
                        found = true;
                    } else {
                        // does not match the remaining part, we can flush this part of the tree
                        assert nextSuffix.compareTo(child.prefix) > 0 : "Not sorted";
                        flushPrefix(newPrefix, child, output);
                        it.remove();
                    }
                }
                if (found) {
                    // found a child that matches the remaining suffix
                    return;
                }
                // No child exist with any prefix so we add a new one
                assert node.children.isEmpty();
                node.children.add(new AutoPrefixNode(node, BytesRef.deepCopyOf(nextSuffix), 1,
                    or(docIdSet, newDocIdSetBuilder())));
                return;
            }
            // The candidate suffix and the prefix located in this node share a prefix so we need to split the node
            assert suffix.compareTo(node.prefix) > 0 : "Not sorted";
            BytesRef splitPrefix = new BytesRef(node.prefix.bytes, node.prefix.offset, prefixLen);
            BytesRef splitSuffix = new BytesRef(suffix.bytes, suffix.offset + prefixLen, suffix.length - prefixLen);

            // first we can flush the current node
            flushPrefix(prefix, node, output);

            // and then we split the node with the remaining part of the suffix candidate.
            node.prefix = BytesRef.deepCopyOf(splitPrefix);
            node.children.clear();
            AutoPrefixNode newNode = new AutoPrefixNode(node, BytesRef.deepCopyOf(splitSuffix), 1,
                or(docIdSet, newDocIdSetBuilder()));
            node.children.add(newNode);
        }
    }


    private void flushPrefix(BytesRef prefix, AutoPrefixNode node, IndexOutput output) throws IOException {
        DocIdSet docIdSet = innerFlushPrefix(prefix, node, output);
        // propagates the doc ids matching the flushed prefix to its parent
        AutoPrefixNode parent = node.parent;
        if (parent != null) {
            if (parent.docIdSetB == null) {
                parent.docIdSetB = newDocIdSetBuilder();
            }
            or(docIdSet, parent.docIdSetB);
        }
    }

    private DocIdSet innerFlushPrefix(BytesRef prefix, AutoPrefixNode node, IndexOutput output) throws IOException {
        if(node.docIdSetB == null) {
            node.docIdSetB = newDocIdSetBuilder();
        }
        BytesRef newPrefix = concatenate(prefix, node.prefix);
        for (AutoPrefixNode child : node.children) {
            DocIdSet childDocIdSetB = innerFlushPrefix(newPrefix, child, output);
            or(childDocIdSetB, node.docIdSetB);
        }
        DocIdSet docIdSet = node.docIdSetB.build();
        node.docIdSetB = null;
        if (node.numTerms >= minTermsInAutoPrefix) {
            DocIdSetIterator it = docIdSet.iterator();
            PrefixTerm prefixTerm = flushInvertedList(newPrefix, output, it);
            prefixes.add(prefixTerm);
        }
        return docIdSet;
    }

    /**
     * flushes the {@link DocIdSetIterator} in the temporary file and record the poiinter for further retrieval in the
     * returned {@link PrefixTerm}.
     */
    private PrefixTerm flushInvertedList(BytesRef newPrefix,
                                         IndexOutput output, DocIdSetIterator docIdSet) throws IOException {
        long filePtr = output.getFilePointer();
        int size = 0;
        if (docIdSet.cost() >= Integer.MAX_VALUE) {
            MonotonicBlockPackedWriter writer = new MonotonicBlockPackedWriter(output, 64);
            while (docIdSet.nextDoc() != DocIdSetIterator.NO_MORE_DOCS) {
                docsSeen.set(docIdSet.docID());
                writer.add(docIdSet.docID());
                size ++;
            }
            writer.finish();
        } else {
            int lastDoc = 0;
            while (docIdSet.nextDoc() != DocIdSetIterator.NO_MORE_DOCS) {
                docsSeen.set(docIdSet.docID());
                int doc = docIdSet.docID() - lastDoc;
                output.writeVInt(doc);
                lastDoc = docIdSet.docID();
                size ++;
            }
        }
        return new PrefixTerm(BytesRef.deepCopyOf(newPrefix), size, filePtr);
    }

    private DocIdSetBuilder newDocIdSetBuilder() throws IOException {
        /**
         * NOCOMMIT Fake Terms#getDocCount and Terms#getSumDocFreq to make the {@link DocIdSetBuilder} aware that it needs to deduplicate the doc ids.
         */
        Terms wrappedTerms = new FilterLeafReader.FilterTerms(terms) {
            @Override
            public int getDocCount() throws IOException {
                return -1;
            }

            @Override
            public long getSumDocFreq() throws IOException {
                return -1;
            }
        };
        return new DocIdSetBuilder(maxDoc, wrappedTerms);
    }

    private static DocIdSetBuilder or(DocIdSet docIdSet, DocIdSetBuilder builder) throws IOException {
        DocIdSetIterator it = docIdSet.iterator();
        while (it.nextDoc() != DocIdSetIterator.NO_MORE_DOCS) {
            builder.grow(1).add(it.docID());
        }
        return builder;
    }

    private static int findLargestPrefix(BytesRef a, BytesRef b) {
        int size = Math.min(a.length, b.length);
        int len = 0;
        for (; len < size; ++len) {
            if (a.bytes[len + a.offset] != b.bytes[len + b.offset]) {
                break;
            }
        }
        return len;
    }

    private static BytesRef concatenate(BytesRef prefix, BytesRef suffix) {
        byte[] newPrefix = new byte[prefix.length + suffix.length];
        System.arraycopy(prefix.bytes, prefix.offset, newPrefix, 0, prefix.length);
        System.arraycopy(suffix.bytes, suffix.offset, newPrefix, 0 + prefix.length, suffix.length);
        return new BytesRef(newPrefix);
    }

    private static class PrefixTerm implements Comparable<PrefixTerm> {
        final BytesRef prefix;
        final int docCount;
        final long ptr;

        PrefixTerm(BytesRef prefix, int docCount, long ptr) {
            this.prefix = prefix;
            this.docCount = docCount;
            this.ptr = ptr;
        }

        @Override
        public int compareTo(PrefixTerm other) {
            return prefix.compareTo(other.prefix);
        }
    }

    private class TermsReader extends Terms implements Closeable {
        int docCount = docsSeen.cardinality();

        @Override
        public void close() throws IOException {
            AutoPrefixTermsBuilder.this.close();
        }

        @Override
        public TermsEnum iterator() throws IOException {
            final Iterator<PrefixTerm> it = prefixes.iterator();
            return new TermsEnum() {
                PrefixTerm current;

                @Override
                public SeekStatus seekCeil(BytesRef text) throws IOException {
                    throw new IllegalArgumentException("");
                }

                @Override
                public void seekExact(long ord) throws IOException {
                    throw new IllegalArgumentException("");
                }

                @Override
                public BytesRef term() throws IOException {
                    return current.prefix;
                }

                @Override
                public long ord() throws IOException {
                    return -1;
                }

                @Override
                public int docFreq() throws IOException {
                    return current.docCount;
                }

                @Override
                public long totalTermFreq() throws IOException {
                    return -1;
                }

                @Override
                public PostingsEnum postings(PostingsEnum reuse, int flags) throws IOException {
                    input.seek(current.ptr);
                    final MonotonicBlockPackedReader reader;
                    if (current.docCount >= Integer.MAX_VALUE) {
                        reader = MonotonicBlockPackedReader.of(input, PackedInts.VERSION_CURRENT, 64, current.docCount, false);
                    } else {
                        reader = null;
                    }
                    return new PostingsEnum() {
                        int doc;
                        int lastDoc = 0;
                        long pos = 0;

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
                            return doc;
                        }

                        @Override
                        public int nextDoc() throws IOException {
                            if (pos >= current.docCount) {
                                return NO_MORE_DOCS;
                            }
                            if (reader != null) {
                                doc = (int) reader.get(pos);
                            } else {
                                doc = input.readVInt() + lastDoc;
                                lastDoc = doc;
                            }
                            ++pos;
                            return doc;
                        }

                        @Override
                        public int advance(int target) throws IOException {
                            throw new IllegalArgumentException();
                        }

                        @Override
                        public long cost() {
                            return current.docCount;
                        }
                    };
                }

                @Override
                public BytesRef next() throws IOException {
                    if (it.hasNext()) {
                        current = it.next();
                        return current.prefix;
                    }
                    current = null;
                    return null;
                }
            };
        }

        @Override
        public long size() throws IOException {
            return prefixes.size();
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
