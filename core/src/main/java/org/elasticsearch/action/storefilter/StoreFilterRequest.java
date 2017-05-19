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

package org.elasticsearch.action.storefilter;

import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.support.broadcast.BroadcastRequest;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;

/**
 * A storefilter request
 *
 * @see StoreFilterResponse
 */
public class StoreFilterRequest extends BroadcastRequest<StoreFilterRequest> {

    private IndexRequest indexRequest;

    public StoreFilterRequest(String... indices) {
        super(indices);
    }

    public StoreFilterRequest(IndexRequest indexRequest) {
        super(new String[] { indexRequest.index() });
        this.indexRequest = indexRequest;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        indexRequest.writeTo(out);
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);
        indexRequest = new IndexRequest();
        indexRequest.readFrom(in);
    }

    public IndexRequest getIndexRequest() {
        return indexRequest;
    }

    public void setIndexRequest(IndexRequest indexRequest) {
        this.indexRequest = indexRequest;
    }
}
