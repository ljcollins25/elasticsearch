package org.elasticsearch.index.storedfilters;

import org.apache.lucene.index.LeafReader;
import org.apache.lucene.search.DocIdSet;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.util.BytesRef;

import java.io.IOException;

/**
 * Created by lancec on 5/20/2017.
 */
public interface StoredFilterDocsProvider {

    public BytesRef filterTerm();

    /*
     * Gets the associated docs for the stored filter
     */
    public DocIdSet getStoredFilterDocs(IndexSearcher leafSearcher) throws IOException;
}
