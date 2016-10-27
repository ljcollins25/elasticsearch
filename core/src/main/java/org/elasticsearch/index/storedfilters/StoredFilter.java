package org.elasticsearch.index.storedfilters;

import org.apache.lucene.codecs.FieldsConsumer;
import org.apache.lucene.index.FilterLeafReader;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.SegmentReader;
import org.apache.lucene.search.Collector;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.LeafCollector;
import org.apache.lucene.search.Query;

import java.io.IOException;

/**
 * Created by lancec on 10/15/2016.
 */
public class StoredFilter {

    public void StoreFilter(Query filter, IndexSearcher searcher, FieldsConsumer fieldsConsumer) throws IOException {


        searcher.search(filter, new Collector() {
            @Override
            public LeafCollector getLeafCollector(LeafReaderContext context) throws IOException {
                SegmentReader reader = segmentReader(context.reader());
                reader.getSegmentName();
                return null;
            }

            @Override
            public boolean needsScores() {
                return false;
            }
        });
    }

    /**
     * Tries to extract a segment reader from the given index reader.
     * If no SegmentReader can be extracted an {@link IllegalStateException} is thrown.
     */
    protected static SegmentReader segmentReader(LeafReader reader) {
        if (reader instanceof SegmentReader) {
            return (SegmentReader) reader;
        } else if (reader instanceof FilterLeafReader) {
            final FilterLeafReader fReader = (FilterLeafReader) reader;
            return segmentReader(FilterLeafReader.unwrap(fReader));
        }
        // hard fail - we can't get a SegmentReader
        throw new IllegalStateException("Can not extract segment reader from given index reader [" + reader + "]");
    }
}
