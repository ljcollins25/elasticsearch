package org.elasticsearch.index.storedfilter;

import org.apache.lucene.util.packed.PackedLongValues;

import java.io.IOException;

/**
 * Created by lancec on 7/15/2017.
 */
public class PackedLongIterator implements LongIterator{
    private long current = -1;
    private PackedLongValues values;
    private PackedLongValues.Iterator valuesIterator;

    public PackedLongIterator(PackedLongValues values)
    {
        this.values = values;
        valuesIterator = values.iterator();
    }

    @Override
    public boolean moveNext() throws IOException {
        if(valuesIterator.hasNext())
        {
            current = valuesIterator.next();
            return true;
        }
        else
        {
            current = -1;
        }

        return false;
    }

    @Override
    public long longValue() {
        return current;
    }

    @Override
    public LongIterator newIterator() {
        return new PackedLongIterator(values);
    }
}
