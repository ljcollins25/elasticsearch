package org.elasticsearch.index.storedfilters;

import org.apache.lucene.index.*;
import org.apache.lucene.search.*;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by lancec on 11/12/2016.
 */
public class StoredFilterUtils {

    // These fields are 'attached' to normal documents



    // The name of the field containing stored filter terms for normal documents
    // which have been merged into the segment
    public static final String STORED_FILTER_TERM_FIELD_NAME = "_stored_filter_term";

    // These are fields for the filter document

    // The field in the stored filter document specifying the filter name
    // This is assumed to be added to filter document before entering the
    // stored filter manager.
    // TODO: Maybe this should be added by the stored filter manager? Depends on
    // TODO: stored filters are added
    public static final String STORED_FILTER_NAME_FIELD_NAME = "_stored_filter_name";

    // The field for the name of the segment
    public static final String STORED_FILTER_SEGMENTS_FIELD_NAME = "_stored_filter_segments";

    // The field for the document filter for a segment
    public static final String STORED_FILTER_DOCS_FIELD_NAME = "_stored_filter_docs";

    /**
     * Tries to extract a segment reader from the given index reader.
     * If no SegmentReader can be extracted an {@link IllegalStateException} is thrown.
     */
    public static SegmentReader segmentReader(LeafReader reader) {
        if (reader instanceof SegmentReader) {
            return (SegmentReader) reader;
        } else if (reader instanceof FilterLeafReader) {
            final FilterLeafReader fReader = (FilterLeafReader) reader;
            return segmentReader(FilterLeafReader.unwrap(fReader));
        } else if (reader instanceof StoredFilterManager.StoredFilterCodecReader) {
            final StoredFilterManager.StoredFilterCodecReader fReader = (StoredFilterManager.StoredFilterCodecReader) reader;
            return segmentReader(fReader.unwrap());
        }
        // hard fail - we can't get a SegmentReader
        throw new IllegalStateException("Can not extract segment reader from given index reader [" + reader + "]");
    }

    public static Map<String, RoaringDocIdSet.Builder> getSegmentDocIdSetsForFilterAndSearcher(StoredFilterData filterData, IndexSearcher searcher) throws IOException
    {
        Map<String, RoaringDocIdSet.Builder> docsBySegmentMap = new HashMap<>();

        searcher.search(filterData.filter, new Collector() {
            @Override
            public LeafCollector getLeafCollector(LeafReaderContext context) throws IOException {

                SegmentReader segmentReader = segmentReader(context.reader());
                String segmentName = segmentReader.getSegmentName();
                RoaringDocIdSet.Builder docIdSetBuilder = new RoaringDocIdSet.Builder(segmentReader.maxDoc());
                docsBySegmentMap.put(segmentName, docIdSetBuilder);

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

        return docsBySegmentMap;
    }

    public static RoaringDocIdSet getDocIdSetForFilterAndSearcher(Query filter, IndexSearcher segmentSearcher) throws IOException {
        IndexReaderContext readerContext = segmentSearcher.getTopReaderContext();
        RoaringDocIdSet.Builder docIdSetBuilder = new RoaringDocIdSet.Builder(segmentSearcher.getIndexReader().maxDoc());

        segmentSearcher.search(filter, new Collector() {
            @Override
            public LeafCollector getLeafCollector(LeafReaderContext context) throws IOException {
                if (context != readerContext) {
                    return null;
                }


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

}
