package org.elasticsearch.index.storedfilter;

import java.io.IOException;

/**
 * Created by lancec on 7/15/2017.
 */
public interface LongIterator {
    boolean moveNext() throws IOException;

    long longValue();

    LongIterator newIterator() throws IOException;

    static LongIterator empty() {
        return  new LongIterator() {
            @Override
            public boolean moveNext() throws IOException {
                return false;
            }

            @Override
            public long longValue() {
                return 0;
            }

            @Override
            public LongIterator newIterator() throws IOException {
                return this;
            }
        };
    }

    static LongIterator sortedUnion(LongIterator iterator1, LongIterator iterator2) {

        return new LongIterator() {
            boolean initialized = false;
            long currentValue;
            LongIterator remainingIterator = null;

            LongIterator greaterIterator = null;
            LongIterator lesserIterator = null;

            @Override
            public boolean moveNext() throws IOException {
                if (remainingIterator != null) {
                    return remainingIterator.moveNext();
                }

                if (!initialized) {
                    initialized = true;
                    boolean moveNext1 = iterator1.moveNext();
                    boolean moveNext2 = iterator2.moveNext();

                    if (!moveNext1 && !moveNext2) {
                        remainingIterator = iterator1;
                        return false;
                    }
                    else if (moveNext1 && !moveNext2) {
                        remainingIterator = iterator1;
                        return true;
                    } else if (moveNext2 && !moveNext1) {
                        remainingIterator = iterator2;
                        return true;
                    }

                    if (iterator1.longValue() > iterator2.longValue()) {
                        greaterIterator = iterator1;
                        lesserIterator = iterator2;
                    } else {
                        greaterIterator = iterator2;
                        lesserIterator = iterator1;
                    }

                    currentValue = lesserIterator.longValue();
                    return true;
                }

                while (lesserIterator.moveNext()) {
                    if (iterator1.longValue() > iterator2.longValue()) {
                        greaterIterator = iterator1;
                        lesserIterator = iterator2;
                    } else {
                        greaterIterator = iterator2;
                        lesserIterator = iterator1;
                    }

                    if (currentValue != lesserIterator.longValue()) {
                        currentValue = lesserIterator.longValue();
                        return true;
                    }
                }

                remainingIterator = greaterIterator;
                return remainingIterator.moveNext();
            }

            @Override
            public long longValue() {
                return remainingIterator != null ? remainingIterator.longValue() : currentValue;
            }

            @Override
            public LongIterator newIterator() throws IOException {
                return LongIterator.sortedUnion(iterator1.newIterator(), iterator2.newIterator());
            }
        };
    }

    static LongIterator sortedUnique(LongIterator iterator) {

        return new LongIterator() {
            boolean initialized = false;
            long lastValue;

            @Override
            public boolean moveNext() throws IOException {
                if (!iterator.moveNext()) {
                    return false;
                }

                while (initialized && lastValue == iterator.longValue()) {
                    if (!iterator.moveNext()) {
                        return false;
                    }
                }

                lastValue = iterator.longValue();
                return true;
            }

            @Override
            public long longValue() {
                return iterator.longValue();
            }

            @Override
            public LongIterator newIterator() throws IOException {
                return LongIterator.sortedUnique(iterator.newIterator());
            }
        };
    }

    static LongIterator sortedExcept(LongIterator iterator1, LongIterator iterator2) {

        return new LongIterator() {
            boolean initialized = false;
            long currentValue;
            LongIterator remainingIterator = null;

            @Override
            public boolean moveNext() throws IOException {
                if (remainingIterator != null) {
                    return remainingIterator.moveNext();
                }

                if (!initialized) {
                    initialized = true;
                    boolean moveNext1 = iterator1.moveNext();
                    boolean moveNext2 = iterator2.moveNext();

                    if (!moveNext1 && !moveNext2) {
                        remainingIterator = iterator1;
                        return false;
                    }
                    else if (moveNext1 && !moveNext2) {
                        remainingIterator = iterator1;
                        return true;
                    } else if (moveNext2 && !moveNext1) {
                        remainingIterator = iterator1;
                        return false;
                    }
                }

                if (!iterator1.moveNext()) {
                    remainingIterator = iterator1;
                    return false;
                }

                while (iterator1.longValue() == iterator2.longValue()) {
                    if (!iterator1.moveNext()) {
                        remainingIterator = iterator1;
                        return false;
                    }
                }

                currentValue = iterator1.longValue();

                while (currentValue > iterator2.longValue()) {
                    if (!iterator2.moveNext()) {
                        remainingIterator = iterator1;
                        return true;
                    }
                }

                return true;
            }

            @Override
            public long longValue() {
                return remainingIterator != null ? remainingIterator.longValue() : iterator1.longValue();
            }

            @Override
            public LongIterator newIterator() throws IOException {
                return LongIterator.sortedExcept(iterator1.newIterator(), iterator2.newIterator());
            }
        };
    }
}
