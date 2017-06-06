package org.elasticsearch.index.storedfilters;

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.*;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.text.Text;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.mapper.Mapper;
import org.elasticsearch.index.mapper.ParseContext;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Information about a stored filter
 */
public class StoredFilterData implements StoredFilterDocsProvider
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

    public final Term uid;

    public StoredFilterData(String filterName, Query filter, ParseContext.Document document, Term uid)
    {
        this.filterName = new Text(filterName);
        this.filterNameBytes = this.filterName.bytes().toBytesRef();
        this.filter = filter;
        this.document = document;
        this.uid = uid;
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

    @Override
    public BytesRef filterTerm() {
        return filterNameBytes;
    }

    @Override
    public DocIdSet getStoredFilterDocs(IndexSearcher leafSearcher) throws IOException {
        IndexReader reader = leafSearcher.getIndexReader();
        RoaringDocIdSet.Builder docIdSetBuilder = new RoaringDocIdSet.Builder(reader.maxDoc());

        leafSearcher.search(filter, new Collector() {
            @Override
            public LeafCollector getLeafCollector(LeafReaderContext context) throws IOException {
                return new LeafCollector() {
                    @Override
                    public void setScorer(Scorer scorer) throws IOException {
                    }

                    @Override
                    public void collect(int doc) throws IOException {
                        docIdSetBuilder.add(doc);
                    }
                };
            }

            @Override
            public boolean needsScores() {
                return false;
            }
        });

        return docIdSetBuilder.build();
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
