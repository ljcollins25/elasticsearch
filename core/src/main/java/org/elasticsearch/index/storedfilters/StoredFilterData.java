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
public class StoredFilterData
{
    public final Text filterName;
    public final BytesRef filterNameBytes;
    public final Query filter;

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
}
