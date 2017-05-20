package org.elasticsearch.index.storedfilters;

import org.apache.lucene.search.Query;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.text.Text;
import org.elasticsearch.index.mapper.Mapper;
import org.elasticsearch.index.mapper.ParseContext;

import java.util.concurrent.atomic.AtomicReference;

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
    public int mergeCursor = Integer.MAX_VALUE;

    AtomicReference<State> atomicState = new AtomicReference<>(State.CREATED);

    public final ParseContext.Document document;

    public StoredFilterData(String filterName, Query filter, ParseContext.Document document)
    {
        this.filterName = new Text(filterName);
        this.filterNameBytes = this.filterName.bytes().toBytesRef();
        this.filter = filter;
        this.document = document;
    }

    public void changeState(State fromState, State toState, int mergeCursor)
    {
        this.mergeCursor = mergeCursor;
        boolean stateChanged = tryChangeState(fromState, toState);
        assert stateChanged;
    }

    public boolean tryChangeState(State fromState, State toState)
    {
        return atomicState.compareAndSet(fromState, toState);
    }

    public static enum State
    {
        CREATED,

        ADDED,

        STORING,

        STORED,

        REMOVING,

        REMOVED
    }
}
