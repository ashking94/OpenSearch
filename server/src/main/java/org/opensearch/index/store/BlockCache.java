/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.store;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.common.cache.Cache;
import org.opensearch.common.cache.CacheBuilder;
import org.opensearch.common.cache.RemovalReason;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.core.common.unit.ByteSizeUnit;
import org.opensearch.core.common.unit.ByteSizeValue;
import org.opensearch.core.index.shard.ShardId;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Cache for blocks fetched from primary shards.
 * Uses LRU eviction policy to limit memory usage.
 */
public class BlockCache {
    private static final Logger logger = LogManager.getLogger(BlockCache.class);

    // Settings for cache configuration
    public static final Setting<ByteSizeValue> BLOCK_CACHE_SIZE_SETTING = Setting.byteSizeSetting(
        "index.store.block_cache.size",
        new ByteSizeValue(50, ByteSizeUnit.MB), // Default 50MB
        new ByteSizeValue(1, ByteSizeUnit.MB),  // Min 1MB
        new ByteSizeValue(1, ByteSizeUnit.GB),  // Max 1GB
        Setting.Property.NodeScope
    );

    public static final Setting<TimeValue> BLOCK_CACHE_EXPIRE_AFTER_ACCESS_SETTING = Setting.timeSetting(
        "index.store.block_cache.expire_after_access",
        new TimeValue(60, TimeUnit.SECONDS), // Default 60 seconds
        new TimeValue(1, TimeUnit.SECONDS),  // Min 1 second
        Setting.Property.NodeScope
    );

    private final Cache<BlockCacheKey, BytesReference> cache;
    private final long maxCacheSizeBytes;
    private final AtomicLong cacheSizeBytes = new AtomicLong(0);
    private final AtomicLong hits = new AtomicLong(0);
    private final AtomicLong misses = new AtomicLong(0);

    /**
     * Create a new block cache with the given settings
     */
    public BlockCache(Settings settings) {
        this.maxCacheSizeBytes = BLOCK_CACHE_SIZE_SETTING.get(settings).getBytes();
        TimeValue expireAfterAccess = BLOCK_CACHE_EXPIRE_AFTER_ACCESS_SETTING.get(settings);

        this.cache = CacheBuilder.<BlockCacheKey, BytesReference>builder()
            .setMaximumWeight(maxCacheSizeBytes)
            .weigher((key, value) -> value.length())
            .setExpireAfterAccess(expireAfterAccess)
            .removalListener(r -> {
                BlockCacheKey key = r.getKey();
                BytesReference value = r.getValue();
                RemovalReason cause = r.getRemovalReason();
                if (value != null) {
                    cacheSizeBytes.addAndGet(-value.length());
                    logger.info(
                        "Block removed from cache: {} (size: {} bytes, reason: {}), current cache size: {} bytes",
                        key,
                        value.length(),
                        cause,
                        cacheSizeBytes.get()
                    );
                }
            })
            .build();

        logger.info("Created block cache with max size: {} bytes, expire after access: {}", maxCacheSizeBytes, expireAfterAccess);
    }

    /**
     * Get a block from the cache
     *
     * @param shardId  Shard ID
     * @param fileName File name
     * @param blockId  Block ID
     * @return The cached block or null if not found
     */
    public BytesReference getBlock(ShardId shardId, String fileName, long blockId) {
        BlockCacheKey key = new BlockCacheKey(shardId, fileName, blockId);
        BytesReference block = cache.get(key);

        if (block != null) {
            hits.incrementAndGet();
            double hitRatePercent = getHitRate() * 100.0;
            logger.debug(
                "Cache HIT for block {} of file {} for shard {}, hit rate: {}%",
                blockId,
                fileName,
                shardId,
                String.format("%.2f", hitRatePercent)
            );
        } else {
            misses.incrementAndGet();
            double hitRatePercent = getHitRate() * 100.0;
            logger.debug(
                "Cache MISS for block {} of file {} for shard {}, hit rate: {}%",
                blockId,
                fileName,
                shardId,
                String.format("%.2f", hitRatePercent)
            );
        }

        return block;
    }

    /**
     * Put a block in the cache
     *
     * @param shardId  Shard ID
     * @param fileName File name
     * @param blockId  Block ID
     * @param block    Block data
     */
    public void putBlock(ShardId shardId, String fileName, long blockId, BytesReference block) {
        BlockCacheKey key = new BlockCacheKey(shardId, fileName, blockId);

        // Only cache if it fits in the cache
        if (block.length() <= maxCacheSizeBytes) {
            cache.put(key, block);
            cacheSizeBytes.addAndGet(block.length());
            double usagePercent = (cacheSizeBytes.get() * 100.0) / maxCacheSizeBytes;

            logger.debug(
                "Added block {} of file {} to cache for shard {}, size: {} bytes, total cache size: {} bytes, usage: {}%",
                blockId,
                fileName,
                shardId,
                block.length(),
                cacheSizeBytes.get(),
                String.format("%.2f", usagePercent)
            );
        } else {
            logger.debug(
                "Block {} of file {} too large to cache: {} bytes (max: {} bytes)",
                blockId,
                fileName,
                block.length(),
                maxCacheSizeBytes
            );
        }
    }

    /**
     * Get the hit rate of the cache (hits / (hits + misses))
     */
    public double getHitRate() {
        long totalHits = hits.get();
        long totalMisses = misses.get();
        long total = totalHits + totalMisses;

        if (total == 0) {
            return 0;
        }

        return (double) totalHits / total;
    }

    /**
     * Get the number of cache hits
     */
    public long getHits() {
        return hits.get();
    }

    /**
     * Get the number of cache misses
     */
    public long getMisses() {
        return misses.get();
    }

    /**
     * Get the current size of the cache in bytes
     */
    public long getCurrentSizeBytes() {
        return cacheSizeBytes.get();
    }

    /**
     * Clear the cache
     */
    public void clear() {
        cache.invalidateAll();
        cacheSizeBytes.set(0);
        logger.info("Block cache cleared");
    }

    /**
     * Key for block cache entries
     */
    private record BlockCacheKey(ShardId shardId, String fileName, long blockId) {

        @Override
        public String toString() {
            return "BlockCacheKey{" + "shardId=" + shardId + ", fileName='" + fileName + '\'' + ", blockId=" + blockId + '}';
        }
    }
}
