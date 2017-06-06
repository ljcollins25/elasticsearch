package org.elasticsearch.index.storedfilters;

import org.apache.lucene.search.DocIdSet;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.util.BytesRef;

import java.io.IOException;

/**
 * Created by lancec on 5/20/2017.
 */
public class StoredFilterDocs implements StoredFilterDocsProvider {

    private final BytesRef filterTerm;
    private final RoaringDocIdSet filterDocs;

    public StoredFilterDocs(BytesRef filterTerm, RoaringDocIdSet filterDocs)
    {
        this.filterTerm = filterTerm;
        this.filterDocs = filterDocs;
    }

    @Override
    public BytesRef filterTerm() {
        return filterTerm;
    }

    @Override
    public DocIdSet getStoredFilterDocs(IndexSearcher leafSearcher) throws IOException {
        return filterDocs;
    }
}
