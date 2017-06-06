package org.elasticsearch.index.storedfilters;

import org.apache.lucene.codecs.FieldsProducer;
import org.apache.lucene.index.*;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * Augments the fields producer for a segment with additional fields
 */
public class ParallelFieldsProducer extends FieldsProducer {

    private Fields mergedFields;
    private FieldsProducer delegateFieldsProducer;
    private List<Fields> fieldsList = new ArrayList<>();

    public ParallelFieldsProducer(FieldsProducer delegateFieldsProducer, List<Fields> fields, int maxDoc)
    {
        // The fields producer must be the first element in the fields list
        assert fields.get(0) == delegateFieldsProducer;

        this.delegateFieldsProducer = delegateFieldsProducer;

        final ReaderSlice[] slices = new ReaderSlice[fields.size()];

        for (int readerIndex = 0; readerIndex < slices.length; readerIndex++) {
            final Fields f = fields.get(readerIndex);
            slices[readerIndex] = new ReaderSlice(0, maxDoc, readerIndex);
        }

        mergedFields = new MultiFields(fields.toArray(Fields.EMPTY_ARRAY), slices);
    }

    @Override
    public void close() throws IOException {
        delegateFieldsProducer.close();
    }

    @Override
    public void checkIntegrity() throws IOException {
        delegateFieldsProducer.checkIntegrity();
    }

    @Override
    public Iterator<String> iterator() {
        return mergedFields.iterator();
    }

    @Override
    public Terms terms(String field) throws IOException {
        return mergedFields.terms(field);
    }

    @Override
    public int size() {
        return mergedFields.size();
    }

    @Override
    public long ramBytesUsed() {
        return delegateFieldsProducer.ramBytesUsed();
    }

}
