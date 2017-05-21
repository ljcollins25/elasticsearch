package org.elasticsearch.index.storedfilters;

import org.apache.lucene.document.Document;
import org.apache.lucene.index.*;
import org.apache.lucene.queryparser.surround.query.AndQuery;
import org.apache.lucene.search.*;
import org.apache.lucene.store.ByteArrayDataInput;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.text.Text;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
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

    // The type for stored filter documents
    public static final String STORED_FILTER_TYPE = "stored_filter";

    // The field under which the stored filter query is stored
    public static final String STORED_FILTER_QUERY_FIELD_NAME = "_query";

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

    public static void addSegmentStoredFilterDocs(String segmentName, IndexSearcher searcher, List<StoredFilterDocsProvider> storedFilterDatas) throws IOException {

        // Find all stored filter documents where docs are stored for the segment
        Query storedFiltersForSegmentQuery = new TermQuery(new Term(StoredFilterUtils.STORED_FILTER_SEGMENTS_FIELD_NAME, segmentName));

        searcher.search(storedFiltersForSegmentQuery, new Collector() {
            @Override
            public LeafCollector getLeafCollector(LeafReaderContext context) throws IOException {
                final int docBase = context.docBase;

                return new LeafCollector() {
                    @Override
                    public void setScorer(Scorer scorer) throws IOException {
                    }

                    @Override
                    public void collect(int doc) throws IOException {
                        Document storedFilterDocument = searcher.doc(doc);
                        IndexableField filterNameField = storedFilterDocument.getField(StoredFilterUtils.STORED_FILTER_NAME_FIELD_NAME);
                        Text filterName = new Text(filterNameField.stringValue());
                        IndexableField[] segmentsFields = storedFilterDocument.getFields(StoredFilterUtils.STORED_FILTER_SEGMENTS_FIELD_NAME);
                        IndexableField[] docsFields = storedFilterDocument.getFields(StoredFilterUtils.STORED_FILTER_DOCS_FIELD_NAME);
                        for (int i = 0; i < segmentsFields.length; i++) {
                            String documentSegmentName = segmentsFields[i].stringValue();
                            if (documentSegmentName.equals(segmentName))
                            {
                                BytesRef bytes = docsFields[i].binaryValue();
                                RoaringDocIdSet docIdSet = RoaringDocIdSet.read(new ByteArrayDataInput(bytes.bytes, bytes.offset, bytes.length));
                                storedFilterDatas.add(new StoredFilterDocs(filterName.bytes().toBytesRef(), docIdSet));
                            }
                        }
                    }
                };
            }

            @Override
            public boolean needsScores() {
                return false;
            }
        });
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
}
