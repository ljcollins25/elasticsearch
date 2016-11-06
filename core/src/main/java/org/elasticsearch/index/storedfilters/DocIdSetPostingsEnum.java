package org.elasticsearch.index.storedfilters;

import org.apache.lucene.index.PostingsEnum;
import org.apache.lucene.search.DocIdSet;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.util.BitSet;
import org.apache.lucene.util.BitSetIterator;
import org.apache.lucene.util.BytesRef;

import java.io.IOException;

/**
 * Created by lancec on 10/29/2016.
 */
public class DocIdSetPostingsEnum extends PostingsEnum {
    private final DocIdSet bits;
    private DocIdSetIterator in;

    DocIdSetPostingsEnum(DocIdSet bits) {
        this.bits = bits;
        reset();
    }

    @Override
    public int freq() throws IOException {
        return 1;
    }

    @Override
    public int docID() {
        if (in == null) {
            return -1;
        } else {
            return in.docID();
        }
    }

    @Override
    public int nextDoc() throws IOException {
        if (in == null) {
            in = bits.iterator();
        }
        return in.nextDoc();
    }

    @Override
    public int advance(int target) throws IOException {
        return in.advance(target);
    }

    @Override
    public long cost() {
        return in.cost();
    }

    void reset() {
        in = null;
    }

    @Override
    public BytesRef getPayload() {
        return null;
    }

    @Override
    public int nextPosition() {
        throw new UnsupportedOperationException();
    }

    @Override
    public int startOffset() {
        throw new UnsupportedOperationException();
    }

    @Override
    public int endOffset() {
        throw new UnsupportedOperationException();
    }
}
