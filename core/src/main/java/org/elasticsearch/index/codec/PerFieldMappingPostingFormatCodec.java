/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.index.codec;

import org.apache.logging.log4j.Logger;
import org.apache.lucene.codecs.Codec;
import org.apache.lucene.codecs.FieldsConsumer;
import org.apache.lucene.codecs.FieldsProducer;
import org.apache.lucene.codecs.PostingsFormat;
import org.apache.lucene.codecs.lucene50.Lucene50StoredFieldsFormat;
import org.apache.lucene.codecs.lucene62.Lucene62Codec;
import org.apache.lucene.index.Fields;
import org.apache.lucene.index.SegmentReadState;
import org.apache.lucene.index.SegmentWriteState;
import org.apache.lucene.index.Terms;
import org.apache.lucene.util.IOUtils;
import org.elasticsearch.common.lucene.Lucene;
import org.elasticsearch.index.mapper.CompletionFieldMapper;
import org.elasticsearch.index.mapper.CompletionFieldMapper2x;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.MapperService;

import static org.apache.lucene.index.FilterLeafReader.FilterFields;

import java.io.Closeable;
import java.io.IOException;

/**
 * {@link PerFieldMappingPostingFormatCodec This postings format} is the default
 * {@link PostingsFormat} for Elasticsearch. It utilizes the
 * {@link MapperService} to lookup a {@link PostingsFormat} per field. This
 * allows users to change the low level postings format for individual fields
 * per index in real time via the mapping API. If no specific postings format is
 * configured for a specific field the default postings format is used.
 */
// LUCENE UPGRADE: make sure to move to a new codec depending on the lucene version
public class PerFieldMappingPostingFormatCodec extends Lucene62Codec {
    private final Logger logger;
    private final MapperService mapperService;
    private final PostingsFormat defaultPostingsFormat;
    private final MappingPostingsFormat mappingPostingsFormat;

    static {
        assert Codec.forName(Lucene.LATEST_CODEC).getClass().isAssignableFrom(PerFieldMappingPostingFormatCodec.class) : "PerFieldMappingPostingFormatCodec must subclass the latest lucene codec: " + Lucene.LATEST_CODEC;
    }

    public PerFieldMappingPostingFormatCodec(Lucene50StoredFieldsFormat.Mode compressionMode, MapperService mapperService, Logger logger) {
        super(compressionMode);
        this.mapperService = mapperService;
        this.logger = logger;

        defaultPostingsFormat = super.getPostingsFormatForField(null);
        mappingPostingsFormat = new MappingPostingsFormat(defaultPostingsFormat);
    }

    @Override
    public PostingsFormat getPostingsFormatForField(String field) {
        final MappedFieldType fieldType = mapperService.fullName(field);
        if (fieldType == null) {
            logger.warn("no index mapper found for field: [{}] returning default postings format", field);
        } else if (fieldType instanceof CompletionFieldMapper.CompletionFieldType) {
            return CompletionFieldMapper.CompletionFieldType.postingsFormat();
        } else if (fieldType instanceof CompletionFieldMapper2x.CompletionFieldType) {
            return ((CompletionFieldMapper2x.CompletionFieldType) fieldType).postingsFormat(
                super.getPostingsFormatForField(field));
        }

        final PostingsFormat fieldPostingsFormat = super.getPostingsFormatForField(field);
        if (fieldPostingsFormat == defaultPostingsFormat)
        {
            return mappingPostingsFormat;
        }

        return fieldPostingsFormat;
    }

    private class MappingPostingsFormat extends PostingsFormat {
        private final PostingsFormat delegate;

        public MappingPostingsFormat(PostingsFormat delegate) {
            super(delegate.getName());

            this.delegate = delegate;
        }

        @Override
        public FieldsConsumer fieldsConsumer(SegmentWriteState state) throws IOException {
            return new MappingFieldsConsumer(delegate.fieldsConsumer(state), state);
        }

        @Override
        public FieldsProducer fieldsProducer(SegmentReadState state) throws IOException {
            return delegate.fieldsProducer(state);
        }
    }

    private class MappingFieldsConsumer extends FieldsConsumer {
        private final SegmentWriteState state;
        private final FieldsConsumer delegate;

        public MappingFieldsConsumer(FieldsConsumer delegate, SegmentWriteState state)
        {
            this.delegate = delegate;
            this.state = state;
        }

        @Override
        public void write(Fields fields) throws IOException {
            CloseableFields allFields = new CloseableFields(fields) {
                Closeable closableTerms;

                @Override
                public Terms terms(String field) throws IOException {
                    if (closableTerms != null) {
                        closableTerms.close();
                        closableTerms = null;
                    }

                    final MappedFieldType fieldType = mapperService.fullName(field);

                    Terms terms = super.terms(field);
                    if (fieldType != null) {
                        terms = fieldType.extendFieldTerms(terms, state);
                        if (terms instanceof Closeable)
                        {
                            closableTerms = (Closeable) terms;
                        }
                    }

                    return terms;
                }

                @Override
                public void close() throws IOException {
                    if (closableTerms != null) {
                        closableTerms.close();
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
    }

    static abstract class CloseableFields extends FilterFields implements Closeable {
        public CloseableFields(Fields in) {
            super(in);
        }
    }
}
