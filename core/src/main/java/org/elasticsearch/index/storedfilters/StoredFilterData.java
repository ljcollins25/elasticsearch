package org.elasticsearch.index.storedfilters;

import org.apache.lucene.search.Query;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.text.Text;

/**
 * Information about a stored filter
 */
public class StoredFilterData
{
    public final Text filterName;
    public final BytesRef filterNameBytes;
    public final Query filter;

    // the maximum index allowed for outstanding merges before this filter can be removed.
    // if any outstanding merges have indices less than this value, the filter must remain
    // until they are completed
    public int maxOutstandingMergeIndex = Integer.MAX_VALUE;

    public StoredFilterData(String filterName, Query filter)
    {
        this.filterName = new Text(filterName);
        this.filterNameBytes = this.filterName.bytes().toBytesRef();
        this.filter = filter;
    }

    public void setMaxOutstandingMergeIndex(int maxOutstandingMergeIndex)
    {
        this.maxOutstandingMergeIndex = maxOutstandingMergeIndex;
    }
}
