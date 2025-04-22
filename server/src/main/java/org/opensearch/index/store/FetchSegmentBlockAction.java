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
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.opensearch.action.ActionType;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.single.shard.TransportSingleShardAction;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.metadata.IndexNameExpressionResolver;
import org.opensearch.cluster.routing.ShardsIterator;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.inject.Inject;
import org.opensearch.core.common.bytes.BytesArray;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.core.common.io.stream.Writeable;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.index.shard.IndexShard;
import org.opensearch.indices.IndicesService;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.TransportService;

import java.io.EOFException;
import java.io.IOException;

/**
 * Transport action for fetching blocks of segment files from primary shards.
 * This is used for direct block-level fetching during segment replication and search.
 */
public class FetchSegmentBlockAction extends ActionType<FetchSegmentBlockResponse> {

    public static final String NAME = "indices:data/read/segment/block";
    public static final FetchSegmentBlockAction INSTANCE = new FetchSegmentBlockAction();

    private static final Logger logger = LogManager.getLogger(FetchSegmentBlockAction.class);

    private FetchSegmentBlockAction() {
        super(NAME, FetchSegmentBlockResponse::new);
    }

    /**
     * Transport implementation of the fetch segment block action
     */
    public static class TransportFetchSegmentBlockAction extends TransportSingleShardAction<
        FetchSegmentBlockRequest,
        FetchSegmentBlockResponse> {

        private final IndicesService indicesService;

        @Inject
        public TransportFetchSegmentBlockAction(
            ThreadPool threadPool,
            ClusterService clusterService,
            TransportService transportService,
            ActionFilters actionFilters,
            IndexNameExpressionResolver indexNameExpressionResolver,
            IndicesService indicesService
        ) {
            super(
                NAME,
                threadPool,
                clusterService,
                transportService,
                actionFilters,
                indexNameExpressionResolver,
                FetchSegmentBlockRequest::new,
                ThreadPool.Names.GET
            );
            this.indicesService = indicesService;
        }

        @Override
        protected FetchSegmentBlockResponse shardOperation(FetchSegmentBlockRequest request, ShardId shardId) throws IOException {
            IndexShard shard = indicesService.getShardOrNull(shardId);
            if (shard == null) {
                throw new IllegalStateException("Shard [" + shardId + "] not found");
            }

            if (!shard.isPrimaryMode()) {
                throw new IllegalStateException("Segment blocks can only be fetched from primary shards");
            }

            BytesReference blockData = readBlock(
                shard.store().directory(),
                request.getFileName(),
                request.getOffset(),
                request.getLength()
            );

            return new FetchSegmentBlockResponse(blockData);
        }

        @Override
        protected Writeable.Reader<FetchSegmentBlockResponse> getResponseReader() {
            return FetchSegmentBlockResponse::new;
        }

        private BytesReference readBlock(Directory dir, String fileName, long offset, int length) throws IOException {
            try (IndexInput input = dir.openInput(fileName, IOContext.DEFAULT)) {
                if (offset >= input.length()) {
                    throw new EOFException("Offset " + offset + " is beyond end of file " + fileName + " (" + input.length() + ")");
                }

                // Adjust length if needed
                length = (int) Math.min(length, input.length() - offset);

                byte[] buffer = new byte[length];
                input.seek(offset);
                input.readBytes(buffer, 0, length);

                return new BytesArray(buffer);
            }
        }

        @Override
        protected boolean resolveIndex(FetchSegmentBlockRequest request) {
            return false; // ShardId is already provided in the request
        }

        @Override
        protected ShardsIterator shards(ClusterState state, InternalRequest request) {
            return state.routingTable().shardRoutingTable(request.request().getShardId()).primaryShardIt();
        }
    }
}
