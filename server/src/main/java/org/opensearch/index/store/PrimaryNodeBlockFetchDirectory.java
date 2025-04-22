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
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FilterDirectory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.transport.TransportService;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Map;
import java.util.function.Supplier;

/**
 * Directory implementation that fetches blocks from the primary shard for remote store index
 */
public class PrimaryNodeBlockFetchDirectory extends FilterDirectory {

    private static final Logger logger = LogManager.getLogger(PrimaryNodeBlockFetchDirectory.class);

    private final ShardId shardId;
    private final TransportService transportService;
    private final ClusterService clusterService;
    private final Supplier<String> primaryNodeIdSupplier;
    private final int blockSize;

    public PrimaryNodeBlockFetchDirectory(
        Directory delegate,
        ShardId shardId,
        TransportService transportService,
        ClusterService clusterService,
        Supplier<String> primaryNodeIdSupplier,
        int blockSize
    ) {
        super(delegate);
        this.shardId = shardId;
        this.transportService = transportService;
        this.clusterService = clusterService;
        this.primaryNodeIdSupplier = primaryNodeIdSupplier;
        this.blockSize = blockSize;

        logger.debug("Created PrimaryNodeBlockFetchDirectory for shard {} with blockSize={}", shardId, blockSize);
    }

    @Override
    public IndexInput openInput(String name, IOContext context) throws IOException {
        try {
            // Try to open from local filesystem first
            return in.openInput(name, context);
        } catch (IOException e) {
            logger.info("File [{}] not found locally, using block-level fetch", name);

            // Get file metadata from registry
            Map<String, StoreFileMetadata> metadata = ShardMetadataRegistry.getMetadata(shardId);
            StoreFileMetadata fileMetadata = metadata.get(name);

            if (fileMetadata == null) {
                throw new FileNotFoundException("File [" + name + "] not found in metadata registry");
            }

            // Create a block-fetching IndexInput
            return new BlockFetchIndexInput(
                name,
                fileMetadata.length(),
                transportService,
                clusterService,
                primaryNodeIdSupplier.get(),
                shardId,
                blockSize
            );
        }
    }
}
