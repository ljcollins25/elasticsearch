package org.elasticsearch.index.storedfilters;

import org.apache.lucene.codecs.FieldsProducer;
import org.apache.lucene.codecs.compressing.GrowableByteArrayDataOutput;
import org.apache.lucene.document.BinaryDocValuesField;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.StoredField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.*;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.store.DataOutput;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.engine.EngineException;
import org.elasticsearch.index.mapper.ParseContext;
import org.elasticsearch.index.merge.OnGoingMerge;
import sun.reflect.generics.reflectiveObjects.NotImplementedException;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
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
    private List<String> filterFieldName = Arrays.asList("_stored_filter");
    private List<String> emptyfilterFieldName = new ArrayList<>();

    private IndexWriter indexWriter;
    private Engine engine;

    public StoredFilterManager(IndexWriter indexWriter, Engine engine)
    {
        this.indexWriter = indexWriter;
        this.engine = engine;
    }

    // Create doc id sets for filter based on current commit

    // Store filter

    // Update doc id sets as new commits/flushes are completed

    // Get current max merge age

    // As merges complete check if any outstanding merges have ages less than max merge age

    // if not add new document to index containing filter doc id sets

    public void registerStoredFilter(String filterName, Query filter, ParseContext.Document document, IndexWriter indexWriter, IndexReader indexReader) {
        // Add filter to map. StoredFilterQuery's after this point will return the
        // filter query until the filter is removed
        StoredFilterData filterData = new StoredFilterData(filterName, filter, document);
        filterDataMap.put(filterName, filterData);

        changeFilterState(filterData, StoredFilterData.State.CREATED, StoredFilterData.State.ADDED);

        // await BeforeNextCommitOrFlushOrOutstandingMergesComplete();

        // Add fields to filter document (NOTE: the values of the fields must have a one to one correspondence
        // STORED_FILTER_SEGMENTS_FIELD_NAME (_stored_filter_segments) - the segments
        // STORED_FILTER_DOCS_FIELD_NAME (_stored_filter_docs) - the doc id sets for the segments

        // TODO: Add _stored_filter_docs field to document

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

        filterDataMap.remove(filterData.filterName);

        changeFilterState(filterData, StoredFilterData.State.REMOVING, StoredFilterData.State.REMOVED);
    }

    // Two new fields:
    // _stored_filter [StoredFilterUtils.STORED_FILTER_SEGMENTS_FIELD_NAME] (terms are filter names)
    // IndexOptions.DOCS (not stored)
    // _stored_filter_docs [StoredFilterUtils.STORED_FILTER_DOCS_FIELD_NAME] (no terms) - per segment document?
    // IndexOptions.NONE (stored)

    public void storeFilter(StoredFilterData filterData) {

        Engine.Searcher searcher = null;

        try {
            searcher = engine.acquireSearcher("store_filter");

            GrowableByteArrayDataOutput docsOutput = new GrowableByteArrayDataOutput(1024);

            Map<String, RoaringDocIdSet.Builder> docsBySegmentMap = StoredFilterUtils.getSegmentDocIdSetsForFilterAndSearcher(
                filterData,
                searcher.searcher());

            for (String segment : docsBySegmentMap.keySet()) {
                RoaringDocIdSet.Builder docsBuilder = docsBySegmentMap.get(segment);
                RoaringDocIdSet docs = docsBuilder.build();

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
        public StoredFilterMerge(MergePolicy.OneMerge in) {
            super(in.segments);
        }

        @Override
        public CodecReader wrapForMerge(CodecReader reader) throws IOException {
            return new StoredFilterCodecReader(reader);
        }
    }

    public class StoredFilterCodecReader extends FilterCodecReader {
        public StoredFilterCodecReader(CodecReader in) {
            super(in);
        }

        public CodecReader unwrap() {
            return in;
        }

        @Override
        public FieldsProducer getPostingsReader() {
            FieldsProducer postingsReader = getPostingsReader();
            if (filterDataMap.size() == 0)
            {
                return postingsReader;
            }


            List<Fields> fields = new ArrayList<>();
            fields.add(postingsReader);

            for (StoredFilterData filterData : filterDataMap.values()) {
                // TODO: Add
                fields.add(new StoredFilterDataFields());
            }

            return super.getPostingsReader();
        }
    }
}
