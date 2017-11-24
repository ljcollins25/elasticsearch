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

package org.elasticsearch.plugin;

import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.elasticsearch.index.storedfilter.StoredFilterQueryBuilder;
import org.elasticsearch.ingest.Processor;
import org.elasticsearch.ingest.storedfilter.BinarySequenceProcessor;
import org.elasticsearch.plugins.IngestPlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.plugins.SearchPlugin;

public class StoredFilterPlugin extends Plugin implements IngestPlugin, SearchPlugin {

    @Override
    public Map<String, Processor.Factory> getProcessors(Processor.Parameters parameters) {
        return Collections.singletonMap(BinarySequenceProcessor.TYPE, new BinarySequenceProcessor.Factory());
    }

    @Override
    public List<QuerySpec<?>> getQueries() {
        return Collections.singletonList(new QuerySpec<>(StoredFilterQueryBuilder.NAME, StoredFilterQueryBuilder::new, StoredFilterQueryBuilder::fromXContent));
    }
}
