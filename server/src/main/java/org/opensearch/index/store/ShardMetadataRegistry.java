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
import org.opensearch.core.index.shard.ShardId;

import java.util.Collections;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Registry for maintaining segment file metadata across checkpoint updates
 */
public class ShardMetadataRegistry {

    private static final Logger logger = LogManager.getLogger(ShardMetadataRegistry.class);

    // Using ConcurrentHashMap for thread safety
    private static final Map<ShardId, Map<String, StoreFileMetadata>> REGISTRY = new ConcurrentHashMap<>();

    private ShardMetadataRegistry() {
        // Private constructor to prevent instantiation
    }

    /**
     * Update the metadata for a shard
     */
    public static void updateMetadata(ShardId shardId, Map<String, StoreFileMetadata> metadata) {
        REGISTRY.put(shardId, new ConcurrentHashMap<>(metadata));
        logger.debug("Updated metadata for shard {} with {} files", shardId, metadata.size());
    }

    /**
     * Get the metadata for a shard
     */
    public static Map<String, StoreFileMetadata> getMetadata(ShardId shardId) {
        Map<String, StoreFileMetadata> result = REGISTRY.get(shardId);
        return result != null ? result : Collections.emptyMap();
    }

    /**
     * Remove a shard's metadata when it's closed
     */
    public static void removeShard(ShardId shardId) {
        REGISTRY.remove(shardId);
        logger.debug("Removed metadata for shard {}", shardId);
    }
}
