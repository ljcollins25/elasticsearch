package org.elasticsearch.index.storedfilters;

import org.apache.lucene.codecs.FieldsConsumer;
import org.apache.lucene.index.*;
import org.apache.lucene.search.Collector;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.LeafCollector;
import org.apache.lucene.search.Query;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefHash;
import org.elasticsearch.common.lucene.search.MatchNoDocsQuery;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Created by lancec on 10/15/2016.
 */
public class StoredFilterService {

    public static final String STORED_FILTER_FIELD_NAME = "_stored_filter";

    private Map<String, Query> filtersByName = new ConcurrentHashMap<>();

    public Query getFilter(String filterName)
    {
        return filtersByName.getOrDefault(filterName, null);
    }

    public void write(SegmentWriteState segmentWriteState, String field, Terms terms) throws IOException {
        if (field != STORED_FILTER_FIELD_NAME)
        {
            return;
        }

        TermsEnum termsEnum = terms.iterator();

        BytesRef term;
        while((term = termsEnum.term()) != null)
        {
            String filterName = term.utf8ToString();
            Query filter = getFilter(filterName);
            if (filter == null) {
                continue;
            }


        }
    }
}
