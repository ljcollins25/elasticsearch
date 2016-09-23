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

import java.io.IOException;

import org.apache.lucene.codecs.FieldsConsumer;
import org.apache.lucene.codecs.FieldsProducer;
import org.apache.lucene.codecs.PostingsFormat;
import org.apache.lucene.codecs.PostingsReaderBase;
import org.apache.lucene.codecs.PostingsWriterBase;
import org.apache.lucene.codecs.blocktree.BlockTreeTermsReader;
import org.apache.lucene.codecs.blocktree.BlockTreeTermsWriter;
import org.apache.lucene.codecs.lucene50.Lucene50PostingsReader;
import org.apache.lucene.codecs.lucene50.Lucene50PostingsWriter;
import org.apache.lucene.index.SegmentReadState;
import org.apache.lucene.index.SegmentWriteState;
import org.apache.lucene.util.IOUtils;

/**
 * A PostingsFormat optimized for prefix queries on text fields.
 * It detects prefixes that match "enough" terms and writes auto-prefix terms into their own virtual field.
 * At search time the virtual field is used to speed up prefix queries that match "enough" terms.
 * See {@link AutoPrefixQuery}.
 */
public class AutoPrefixPostingsFormat extends PostingsFormat {
    private final int minTermsInAutoPrefix;
    private final int maxAutoPrefixSize;

    public AutoPrefixPostingsFormat() {
        this(2, Integer.MAX_VALUE);
    }

    public AutoPrefixPostingsFormat(int minTermsInAutoPrefix, int maxAutoPrefixSize) {
        super("AutoPrefix");
        this.minTermsInAutoPrefix = minTermsInAutoPrefix;
        this.maxAutoPrefixSize = maxAutoPrefixSize;
    }

    @Override
    public FieldsConsumer fieldsConsumer(SegmentWriteState state) throws IOException {
        PostingsWriterBase postingsWriter = new Lucene50PostingsWriter(state);
        boolean success = false;
        try {
            FieldsConsumer termsWriter = new BlockTreeTermsWriter(state,
                postingsWriter,
                BlockTreeTermsWriter.DEFAULT_MIN_BLOCK_SIZE,
                BlockTreeTermsWriter.DEFAULT_MAX_BLOCK_SIZE);
            success = true;
            return new AutoPrefixFieldsConsumer(state, termsWriter, minTermsInAutoPrefix, maxAutoPrefixSize);
        } finally {
            if (!success) {
                IOUtils.closeWhileHandlingException(postingsWriter);
            }
        }
    }

    @Override
    public FieldsProducer fieldsProducer(SegmentReadState state) throws IOException {
        PostingsReaderBase postingsReader = new Lucene50PostingsReader(state);
        boolean success = false;
        try {
            FieldsProducer ret = new BlockTreeTermsReader(postingsReader, state);
            success = true;
            return ret;
        } finally {
            if (!success) {
                IOUtils.closeWhileHandlingException(postingsReader);
            }
        }
    }
}
