package org.elasticsearch.index.storedfilters;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

/**
 * Growable array of longs without boxing
 */
public class LongList {

    private long[] items;
    private int size;

    public LongList()
    {
        items = new long[32];
        size = 0;
    }

    public void add(long value) {
        if (size >= items.length)
        {
            items = Arrays.copyOf(items, items.length * 2);
        }

        items[size] = value;
        size++;
    }

    public long get(int index)
    {
        if (index >= size) throw new IndexOutOfBoundsException();
        return items[index];
    }

    public void sort()
    {
        Arrays.sort(items, 0, size);
    }

    public int size()
    {
        return size;
    }

    public LongIterator iterator()
    {
        return new LongIterator() {
            private int index = -1;

            @Override
            public boolean moveNext() throws IOException {
                index++;
                return index < size;
            }

            @Override
            public long longValue() {
                return get(index);
            }

            @Override
            public LongIterator newIterator() {
                return iterator();
            }
        };
    }

    public void clear() {
        size = 0;
    }
}
