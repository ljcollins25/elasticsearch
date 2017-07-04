package org.elasticsearch.index.storedfilters;

import org.elasticsearch.index.shard.ShardId;

import java.util.concurrent.ConcurrentHashMap;

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
