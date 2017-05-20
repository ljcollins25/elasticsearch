package org.elasticsearch.index.storedfilters;

import org.apache.lucene.codecs.FieldsProducer;
import org.apache.lucene.codecs.compressing.GrowableByteArrayDataOutput;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.StoredField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.*;
import org.apache.lucene.search.Query;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.engine.EngineException;
import org.elasticsearch.index.mapper.ParseContext;
import org.elasticsearch.index.mapper.StoredFilterQueryFieldMapper;
import org.elasticsearch.index.merge.OnGoingMerge;

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
public class StoredFilterRegistry {
    private ConcurrentHashMap<String, StoredFilterData> filterDataMap = new ConcurrentHashMap<>();

    public StoredFilterRegistry()
    {
    }

    public void registerStoredFilter(StoredFilterData filterData) {
        // Add filter to map. StoredFilterQuery's after this point will return the
        // filter query until the filter is removed
        filterDataMap.put(filterData.filterName.string(), filterData);
    }

    public Iterable<StoredFilterData> filters()
    {
        return filterDataMap.values();
    }

    public void removeFilter(StoredFilterData filterData) {

        filterDataMap.remove(filterData.filterName);
    }
}
