package org.elasticsearch.index.storedfilter;

import org.apache.lucene.index.Term;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.text.Text;
import org.elasticsearch.index.mapper.ParseContext;

/**
 * Information about a stored filter
 */
public class StoredFilterData
{
    public final Text filterName;
    public final BytesRef filterNameBytes;
    public final LongIterator sequenceNumbers;

    public final ParseContext.Document document;

    public final Term uid;

    public StoredFilterData(String filterName, LongIterator sequenceNumbers, ParseContext.Document document, Term uid)
    {
        this.filterName = new Text(filterName);
        this.filterNameBytes = this.filterName.bytes().toBytesRef();
        this.sequenceNumbers = sequenceNumbers;
        this.document = document;
        this.uid = uid;
    }
}
