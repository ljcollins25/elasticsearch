package org.elasticsearch.index.storedfilters;

import org.apache.lucene.index.Fields;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.search.Query;
import org.elasticsearch.index.mapper.ParseContext;
import org.elasticsearch.index.merge.OnGoingMerge;

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
    private AtomicInteger mergeIndex = new AtomicInteger(0);

    private ConcurrentHashMap<OnGoingMerge, Integer> mergeToIndexMap = new ConcurrentHashMap<>();

    private Map<String, StoredFilterData> filterDataMap = new ConcurrentHashMap<>();
    private List<String> filterFieldName = Arrays.asList("_stored_filter");
    private List<String> emptyfilterFieldName = new ArrayList<>();

    // Create doc id sets for filter based on current commit

    // Store filter

    // Update doc id sets as new commits/flushes are completed

    // Get current max merge age

    // As merges complete check if any outstanding merges have ages less than max merge age

    // if not add new document to index containing filter doc id sets

    public void registerStoredFilter(String filterName, Query filter, ParseContext.Document document, IndexWriter indexWriter, IndexReader indexReader)
    {
        StoredFilterData filterData = new StoredFilterData(filterName, filter);
        filterDataMap.put(filterName, filterData);

        // TODO: Add _stored_filter_docs field to document
    }

    public Fields getFilterFields()
    {
        return null;
    }

    public void mergeStarted(OnGoingMerge onGoingMerge)
    {
        mergeToIndexMap.put(onGoingMerge, mergeIndex.getAndIncrement());
    }

    public void mergeCompleted(OnGoingMerge onGoingMerge)
    {
        mergeToIndexMap.remove(onGoingMerge);
    }

    public void getReader()
    {

    }

    public void commit()
    {

    }
}
