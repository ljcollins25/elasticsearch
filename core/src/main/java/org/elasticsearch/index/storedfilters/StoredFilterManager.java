package org.elasticsearch.index.storedfilters;

import org.apache.lucene.codecs.FieldsProducer;
import org.apache.lucene.codecs.compressing.GrowableByteArrayDataOutput;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.StoredField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.*;
import org.apache.lucene.search.Query;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.engine.EngineException;
import org.elasticsearch.index.mapper.ParseContext;
import org.elasticsearch.index.mapper.StoredFilterQueryFieldMapper;
import org.elasticsearch.index.merge.OnGoingMerge;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created by lancec on 11/5/2016.
 */
public class StoredFilterManager {
    // the index of the last merge registered
    private AtomicInteger nextMergeIndex = new AtomicInteger(0);

    private AtomicInteger maxOutstandingMergeId = new AtomicInteger(0);
    private AtomicInteger minOutstandingMergeId = new AtomicInteger(0);

    private ConcurrentHashMap<OnGoingMerge, Integer> mergeToIndexMap = new ConcurrentHashMap<>();

    private ConcurrentHashMap<String, StoredFilterData> filterDataMap = new ConcurrentHashMap<>();

    private IndexWriter indexWriter;
    private Engine engine;
    private final StoredFilterRegistry storedFilterRegistry;

    public StoredFilterManager(IndexWriter indexWriter, Engine engine)
    {
        this.storedFilterRegistry = engine.config().getStoredFilterRegistry();
        this.indexWriter = indexWriter;
        this.engine = engine;
        storedFilterRegistry.registerManager(engine.config().getShardId(), this);
    }

    // Create doc id sets for filter based on current commit

    // Store filter

    // Update doc id sets as new commits/flushes are completed

    // Get current max merge age

    // As merges complete check if any outstanding merges have ages less than max merge age

    // if not add new document to index containing filter doc id sets

    public void registerStoredFilter(Engine.Index index) {
        String filterName = index.id();
        ParseContext.Document doc = index.docs().get(0);
        StoredFilterQueryFieldMapper.StoredFilterQueryField filterField =
            (StoredFilterQueryFieldMapper.StoredFilterQueryField)doc.getField(StoredFilterUtils.STORED_FILTER_QUERY_FIELD_NAME);

        registerStoredFilter(filterName, filterField.query(), doc);
    }

    public void registerStoredFilter(String filterName, Query filter, ParseContext.Document document) {
        // Add filter to map. StoredFilterQuery's after this point will return the
        // filter query until the filter is removed
        StoredFilterData filterData = new StoredFilterData(filterName, filter, document);
        filterDataMap.put(filterName, filterData);

        changeFilterState(filterData, StoredFilterData.State.CREATED, StoredFilterData.State.ADDED);

        updateFilter(filterData);
    }

    private void updateFilter(StoredFilterData filterData) {

        boolean retry = true;
        while (retry) {
            retry = false;
            int mergeCursor = filterData.mergeCursor;
            int earliestMergeIndex = minOutstandingMergeId.get();

            if (earliestMergeIndex >= mergeCursor) {
                // There are no outstanding merges or the earliest outstanding merge
                // started after the filter was added

                // filter is in initial added state so store the filter and update the state
                if (filterData.tryChangeState(StoredFilterData.State.ADDED, StoredFilterData.State.STORING)) {
                    storeFilter(filterData);
                    retry = true;
                    continue;
                }

                // filter is in stored state so removed the filter and update the state
                if (filterData.tryChangeState(StoredFilterData.State.STORED, StoredFilterData.State.REMOVING)) {
                    removeFilter(filterData);
                }
            }
        }
    }

    public void updateFilters(boolean updateMergeId) {
        if (updateMergeId) {
            updateMaxOutstandingMergeId();
        }

        for (StoredFilterData filterData : filterDataMap.values()) {
            updateFilter(filterData);
        }
    }

    public void removeFilter(StoredFilterData filterData) {

        filterDataMap.remove(filterData.filterName.string());

        changeFilterState(filterData, StoredFilterData.State.REMOVING, StoredFilterData.State.REMOVED);
    }

    // New fields:
    // STORED_FILTER_SEGMENTS_FIELD_NAME (_stored_filter_segments) - the segments with matching docs
    // STORED_FILTER_DOCS_FIELD_NAME (_stored_filter_docs) - the doc id sets for the segments
    // STORED_FILTER_NODOCS_SEGMENTS_FIELD_NAME (_stored_filter_nodocs_segments) - the segments with no matching docs

    public void storeFilter(StoredFilterData filterData) {

        filterData.document.add(new StringField(StoredFilterUtils.STORED_FILTER_NAME_FIELD_NAME, filterData.filterName.string(), Field.Store.YES));

        Engine.Searcher searcher = null;

        try {
            searcher = engine.acquireSearcher("storeFilter");

            GrowableByteArrayDataOutput docsOutput = new GrowableByteArrayDataOutput(1024);

            Map<String, RoaringDocIdSet.Builder> docsBySegmentMap = StoredFilterUtils.getSegmentDocIdSetsForFilterAndSearcher(
                filterData,
                searcher.searcher());

            for (String segment : docsBySegmentMap.keySet()) {
                RoaringDocIdSet.Builder docsBuilder = docsBySegmentMap.get(segment);
                RoaringDocIdSet docs = docsBuilder.build();
                if (docs.cardinality() == 0)
                {
                    filterData.document.add(new StringField(StoredFilterUtils.STORED_FILTER_NODOCS_SEGMENTS_FIELD_NAME, segment, Field.Store.YES));
                    continue;
                }

                docsOutput.length = 0;
                docs.write(docsOutput);

                // copy the bytes since doc outputs is reused as buffer for writing docs for each segment
                byte[] docBytes = Arrays.copyOfRange(docsOutput.bytes, 0, docsOutput.length);

                filterData.document.add(new StringField(StoredFilterUtils.STORED_FILTER_SEGMENTS_FIELD_NAME, segment, Field.Store.YES));
                filterData.document.add(new StoredField(StoredFilterUtils.STORED_FILTER_DOCS_FIELD_NAME, docBytes));
            }

            indexWriter.addDocument(filterData.document);

        }
        catch (IOException ex) { }
        catch (EngineException ex) { }
        finally
        {
            changeFilterState(filterData, StoredFilterData.State.STORING, StoredFilterData.State.STORED);
            if (searcher != null)
            {
                searcher.close();
            }
        }
    }

    public void changeFilterState(StoredFilterData filterData, StoredFilterData.State fromState, StoredFilterData.State toState) {
        filterData.changeState(fromState, toState, nextMergeIndex.get());
    }

    public void updateMaxOutstandingMergeId() {
        int maxOutstandingMergeId = -1;
        int minOutstandingMergeId = Integer.MAX_VALUE;

        synchronized (mergeToIndexMap) {
            for (Integer mergeIndex : mergeToIndexMap.values()) {
                maxOutstandingMergeId = Math.max(maxOutstandingMergeId, mergeIndex);
                minOutstandingMergeId = Math.min(minOutstandingMergeId, mergeIndex);
            }

            this.maxOutstandingMergeId.set(maxOutstandingMergeId);
            this.minOutstandingMergeId.set(minOutstandingMergeId);
        }
    }

    public Query tryGetStoredFilter(String filterName) {
        StoredFilterData filterData = filterDataMap.getOrDefault(filterName, null);
        if (filterData == null) return null;
        else return filterData.filter;
    }

    public MergePolicy.OneMerge wrapMerge(MergePolicy.OneMerge merge) {
        return new StoredFilterMerge(merge);
    }

    public void mergeStarted(OnGoingMerge onGoingMerge) {
        mergeToIndexMap.put(onGoingMerge, nextMergeIndex.getAndIncrement());
        updateMaxOutstandingMergeId();
    }

    public void mergeCompleted(OnGoingMerge onGoingMerge) {
        mergeToIndexMap.remove(onGoingMerge);
        updateFilters(true);
    }

    public class StoredFilterMerge extends MergePolicy.OneMerge {
        private MergePolicy.OneMerge in;

        public StoredFilterMerge(MergePolicy.OneMerge in) {
            super(in.segments);
            this.in = in;
        }

        @Override
        public CodecReader wrapForMerge(CodecReader reader) throws IOException {
            return new StoredFilterCodecReader(reader);
        }

        @Override
        public int hashCode() {
            return in.hashCode();
        }

        @Override
        public boolean equals(Object obj) {
            return super.equals(obj) || in.equals(obj);
        }
    }

    public class StoredFilterCodecReader extends FilterCodecReader {
        final String segmentName;
        public StoredFilterCodecReader(CodecReader in) {
            super(in);

            SegmentReader segmentReader = StoredFilterUtils.segmentReader(in);
            segmentName = segmentReader.getSegmentName();
        }

        public CodecReader unwrap() {
            return in;
        }

        @Override
        public FieldInfos getFieldInfos() {
            return super.getFieldInfos();
        }

        @Override
        public FieldsProducer getPostingsReader() {
            // TODO: Create fields for stored filters which have already been stored
            FieldsProducer postingsReader = super.getPostingsReader();

            List<Fields> fields = new ArrayList<>();
            fields.add(postingsReader);
            HashSet<BytesRef> terms = new HashSet<>();
            List<StoredFilterDocsProvider> storedFilterDatas = new ArrayList<>();

            Engine.Searcher searcher = null;

            try {
                searcher = engine.acquireSearcher("store_filter_fields");
                StoredFilterUtils.addSegmentStoredFilterDocs(segmentName, searcher.searcher(), storedFilterDatas);
            }
            catch (IOException ex) { }
            catch (EngineException ex) { }
            finally
            {
                if (searcher != null)
                {
                    searcher.close();
                }
            }

            for (StoredFilterDocsProvider filterData : storedFilterDatas) {
                terms.add(filterData.filterTerm());
            }

            for (StoredFilterData filterData : filterDataMap.values()) {
                if (terms.add(filterData.filterTerm())) {
                    storedFilterDatas.add(filterData);
                }
            }

            if (storedFilterDatas.size() == 0)
            {
                return postingsReader;
            }

            // Add store filter datas
            fields.add(new StoredFilterDataFields(this.in, storedFilterDatas));

            return new ParallelFieldsProducer(postingsReader, fields, in.maxDoc());
        }
    }
}
