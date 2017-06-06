package org.elasticsearch.index.storedfilters;

import org.apache.lucene.codecs.FieldsProducer;
import org.apache.lucene.codecs.compressing.GrowableByteArrayDataOutput;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.StoredField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.*;
import org.apache.lucene.search.Query;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.engine.EngineException;
import org.elasticsearch.index.mapper.ParseContext;
import org.elasticsearch.index.mapper.StoredFilterQueryFieldMapper;
import org.elasticsearch.index.merge.OnGoingMerge;
import org.elasticsearch.index.shard.ShardId;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created by lancec on 11/5/2016.
 */
public class StoredFilterRegistry {
    private ConcurrentHashMap<Integer, StoredFilterManager> shardManagers = new ConcurrentHashMap<>();

    public StoredFilterRegistry()
    {
    }

    public void registerManager(ShardId shardId, StoredFilterManager shardManager)
    {
        shardManagers.put(shardId.getId(), shardManager);
    }

    public StoredFilterManager getShardManager(int shardId) {
        return shardManagers.get(shardId);
    }
}
