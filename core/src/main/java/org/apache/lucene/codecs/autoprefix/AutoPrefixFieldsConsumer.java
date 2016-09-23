/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.lucene.codecs.autoprefix;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.lucene.codecs.FieldsConsumer;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.Fields;
import org.apache.lucene.index.SegmentWriteState;
import org.apache.lucene.index.Terms;
import org.apache.lucene.util.IOUtils;

/**
 * A fields consumer that detects prefixes that match "enough" terms and write auto-prefix terms into their own virtual field.
 */
public class AutoPrefixFieldsConsumer extends FieldsConsumer {
    private final int minTermsInAutoPrefix;
    private final int maxAutoPrefixSize;
    private final FieldsConsumer delegate;
    private final SegmentWriteState state;

    public AutoPrefixFieldsConsumer(SegmentWriteState state, FieldsConsumer delegate)  {
        this(state, delegate, 2, Integer.MAX_VALUE);
    }

    public AutoPrefixFieldsConsumer(SegmentWriteState state, FieldsConsumer delegate, int minTermsInAutoPrefix, int maxAutoPrefixSize) {
        this.state = state;
        this.delegate = delegate;
        this.minTermsInAutoPrefix = minTermsInAutoPrefix;
        this.maxAutoPrefixSize = maxAutoPrefixSize;
    }

    @Override
    public void write(Fields fields) throws IOException {
        List<String> fieldNames = new ArrayList<>();
        for (FieldInfo fieldInfo : state.fieldInfos) {
            fieldNames.add(fieldInfo.name);
        }
        CloseableFields allFields = new CloseableFields() {
            AutoPrefixTermsBuilder writer;

            @Override
            public Iterator<String> iterator() {
                return fields.iterator();
            }

            @Override
            public Terms terms(String field) throws IOException {
                if (writer != null) {
                    writer.close();
                    writer = null;
                }

                final Terms terms = fields.terms(field);
                writer = new AutoPrefixTermsBuilder(state, terms, minTermsInAutoPrefix, maxAutoPrefixSize);
                return writer.build();
            }

            @Override
            public int size() {
                return fields.size();
            }

            @Override
            public void close() throws IOException {
                if (writer != null) {
                    writer.close();
                }
            }
        };
        try {
            delegate.write(allFields);
        } finally {
            allFields.close();
        }
    }

    @Override
    public void close() throws IOException {
        IOUtils.close(delegate);
    }

    static abstract class CloseableFields extends Fields implements Closeable {}
}
