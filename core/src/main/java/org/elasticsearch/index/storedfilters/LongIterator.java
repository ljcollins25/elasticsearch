package org.elasticsearch.index.storedfilters;

import java.io.IOException;

/**
 * Created by lancec on 7/15/2017.
 */
public interface LongIterator {
    boolean moveNext() throws IOException;

    long longValue();

    LongIterator newIterator() throws IOException;
}
