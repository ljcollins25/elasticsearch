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

import org.elasticsearch.action.ShardOperationFailedException;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.ActiveShardCount;
import org.elasticsearch.action.support.replication.BasicReplicationRequest;
import org.elasticsearch.action.support.replication.ReplicationResponse;
import org.elasticsearch.action.support.replication.TransportBroadcastReplicationAction;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.stream.ByteBufferStreamInput;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.storedfilters.StoredFilterUtils;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;

/**
 * StoreFilter action.
 */
public class TransportStoreFilterAction extends TransportBroadcastReplicationAction<StoreFilterRequest, StoreFilterResponse, IndexRequest, IndexResponse> {

    @Inject
    public TransportStoreFilterAction(Settings settings, ThreadPool threadPool, ClusterService clusterService,
                                  TransportService transportService, ActionFilters actionFilters,
                                  IndexNameExpressionResolver indexNameExpressionResolver,
                                  TransportShardStoreFilterAction shardStoreFilterAction) {
        super(StoreFilterAction.NAME, StoreFilterRequest::new, settings, threadPool, clusterService, transportService, actionFilters, indexNameExpressionResolver, shardStoreFilterAction);
    }

    @Override
    protected IndexResponse newShardResponse() {
        return new IndexResponse();
    }

    @Override
    protected IndexRequest newShardRequest(StoreFilterRequest request, ShardId shardId) {
        IndexRequest indexRequest = new IndexRequest();
        BytesStreamOutput stream = new BytesStreamOutput();
        try
        {
            // Copy the index request by writing to a stream and reading into new index request
            request.getIndexRequest().writeTo(stream);
            indexRequest.readFrom(new ByteBufferStreamInput(ByteBuffer.wrap(stream.bytes().toBytesRef().bytes)));
        }
        catch (IOException ex)
        {
        }

        Map<String, Object> sourceMap = indexRequest.sourceAsMap();
        sourceMap.put(StoredFilterUtils.STORED_FILTER_NAME_FIELD_NAME, indexRequest.id());
        indexRequest.source(sourceMap);

        //indexRequest.id(UUIDs.base64UUID());
        indexRequest.setShardId(shardId);
        indexRequest.waitForActiveShards(ActiveShardCount.NONE);
        return indexRequest;
    }

    @Override
    protected StoreFilterResponse newResponse(int successfulShards, int failedShards, int totalNumCopies, List<ShardOperationFailedException> shardFailures) {
        return new StoreFilterResponse(totalNumCopies, successfulShards, failedShards, shardFailures);
    }
}
