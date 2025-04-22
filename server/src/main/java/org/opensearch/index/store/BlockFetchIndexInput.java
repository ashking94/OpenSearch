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
import org.apache.lucene.store.BufferedIndexInput;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.util.BytesRef;
import org.opensearch.action.ActionListenerResponseHandler;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.transport.TransportService;

import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * An IndexInput implementation that fetches blocks on demand from the primary node
 * when files aren't available locally on the replica.
 */
public class BlockFetchIndexInput extends BufferedIndexInput {
    private static final Logger logger = LogManager.getLogger(BlockFetchIndexInput.class);

    private final String fileName;
    private final long fileLength;
    private final TransportService transportService;
    private final ClusterService clusterService;
    private final String primaryNodeId;
    private final ShardId shardId;
    private final int blockSize;
    private final BlockCache blockCache;

    // Statistics
    private long totalBytesRead = 0;
    private long totalBlocksFetched = 0;
    private long totalBlocksFromCache = 0;
    private long totalFetchTimeNanos = 0;

    // Request timeout in seconds
    private static final int REQUEST_TIMEOUT_SECONDS = 30;

    public BlockFetchIndexInput(
        String fileName,
        long fileLength,
        TransportService transportService,
        ClusterService clusterService,
        String primaryNodeId,
        ShardId shardId,
        int blockSize,
        BlockCache blockCache
    ) {
        super("BlockFetchIndexInput(file=\"" + fileName + "\")", blockSize);
        this.fileName = fileName;
        this.fileLength = fileLength;
        this.transportService = transportService;
        this.clusterService = clusterService;
        this.primaryNodeId = primaryNodeId;
        this.shardId = shardId;
        this.blockSize = blockSize;
        this.blockCache = blockCache;

        logger.info(
            "[{}] Created BlockFetchIndexInput for file '{}' (length: {}, blockSize: {}) from primary node [{}]",
            shardId,
            fileName,
            fileLength,
            blockSize,
            primaryNodeId
        );
    }

    @Override
    protected void readInternal(ByteBuffer dest) throws IOException {
        long startPosition = getFilePointer();
        int bytesToRead = dest.remaining();
        int initialPosition = dest.position();

        logger.info(
            "[{}] Starting read operation for file '{}' at position {} for {} bytes",
            shardId,
            fileName,
            startPosition,
            bytesToRead
        );

        if (startPosition >= fileLength) {
            logger.warn(
                "[{}] Attempt to read beyond EOF in file '{}' at position {} (fileLength: {})",
                shardId,
                fileName,
                startPosition,
                fileLength
            );
            throw new EOFException("Cannot read at position " + startPosition + " (file length: " + fileLength + ")");
        }

        int bytesRead = 0;
        long readStartTime = System.nanoTime();
        int blocksNeeded = 0;
        int blocksFetched = 0;
        int blocksFromCache = 0;

        try {
            while (bytesRead < bytesToRead) {
                // Calculate current block
                long currentBlockOffset = startPosition + bytesRead;
                long blockId = currentBlockOffset / blockSize;
                int offsetInBlock = (int) (currentBlockOffset % blockSize);

                blocksNeeded++;
                logger.info(
                    "[{}] Reading block {} for file '{}' (offset in file: {}, offset in block: {})",
                    shardId,
                    blockId,
                    fileName,
                    currentBlockOffset,
                    offsetInBlock
                );

                // Try to get block from cache first, then fetch if needed
                BytesReference blockData;
                boolean fromCache = false;

                // Check cache first
                blockData = blockCache.getBlock(shardId, fileName, blockId);
                if (blockData != null) {
                    // Cache hit
                    fromCache = true;
                    blocksFromCache++;
                    logger.info(
                        "[{}] Cache HIT for block {} of file '{}' (size: {} bytes)",
                        shardId,
                        blockId,
                        fileName,
                        blockData.length()
                    );
                } else {
                    // Cache miss - fetch from primary
                    long blockFetchStart = System.nanoTime();
                    blockData = fetchBlockFromPrimary(blockId);
                    long blockFetchEnd = System.nanoTime();

                    blocksFetched++;
                    long blockFetchTimeMs = TimeUnit.NANOSECONDS.toMillis(blockFetchEnd - blockFetchStart);
                    logger.info(
                        "[{}] Cache MISS - fetched block {} for file '{}' in {} ms (size: {} bytes)",
                        shardId,
                        blockId,
                        fileName,
                        blockFetchTimeMs,
                        blockData.length()
                    );

                    // Store in cache
                    blockCache.putBlock(shardId, fileName, blockId, blockData);
                }

                // Calculate how many bytes to copy from this block
                int bytesToCopy = Math.min(
                    bytesToRead - bytesRead,
                    Math.min(blockSize - offsetInBlock, blockData.length() - offsetInBlock)
                );

                // Avoid issues if blockData is smaller than expected
                if (bytesToCopy <= 0) {
                    logger.warn(
                        "[{}] No bytes to copy from block {} for file '{}'. Block size: {}, offset: {}, bytes needed: {}",
                        shardId,
                        blockId,
                        fileName,
                        blockData.length(),
                        offsetInBlock,
                        bytesToRead - bytesRead
                    );
                    break;
                }

                // Copy the data to the ByteBuffer - using BytesRef since BytesReference doesn't expose array directly
                BytesRef bytesRef = blockData.toBytesRef();

                if (offsetInBlock == 0 && bytesToCopy == bytesRef.length) {
                    // Can use the whole BytesRef directly
                    logger.info(
                        "[{}] Copying entire block {} ({} bytes) to destination buffer for file '{}' (from {})",
                        shardId,
                        blockId,
                        bytesToCopy,
                        fileName,
                        fromCache ? "cache" : "primary"
                    );
                    dest.put(ByteBuffer.wrap(bytesRef.bytes, bytesRef.offset, bytesRef.length));
                } else {
                    // Need to use a slice of the BytesRef
                    logger.info(
                        "[{}] Copying partial block {} ({} bytes from offset {}) to destination buffer for file '{}' (from {})",
                        shardId,
                        blockId,
                        bytesToCopy,
                        offsetInBlock,
                        fileName,
                        fromCache ? "cache" : "primary"
                    );
                    dest.put(ByteBuffer.wrap(bytesRef.bytes, bytesRef.offset + offsetInBlock, bytesToCopy));
                }

                bytesRead += bytesToCopy;

                // Break if we've reached the end of the file
                if (startPosition + bytesRead >= fileLength) {
                    logger.info("[{}] Reached end of file '{}' after reading {} bytes", shardId, fileName, bytesRead);
                    break;
                }
            }

            long readEndTime = System.nanoTime();
            long readDurationMs = TimeUnit.NANOSECONDS.toMillis(readEndTime - readStartTime);

            // Update statistics
            totalBytesRead += bytesRead;
            totalBlocksFetched += blocksFetched;
            totalBlocksFromCache += blocksFromCache;
            totalFetchTimeNanos += (readEndTime - readStartTime);

            float mbPerSecond = bytesRead > 0 ? (bytesRead / 1024f / 1024f) / (readDurationMs / 1000f) : 0;

            logger.info(
                "[{}] Completed read of {} bytes from file '{}' in {} ms ({} MB/s). "
                    + "Actually read {} bytes, blocks: {}/{} from cache, {}/{} from primary.",
                shardId,
                bytesToRead,
                fileName,
                readDurationMs,
                String.format("%.2f", mbPerSecond),
                bytesRead,
                blocksFromCache,
                blocksNeeded,
                blocksFetched,
                blocksNeeded
            );

            // Log cumulative statistics
            double cacheHitRate = (totalBlocksFromCache + totalBlocksFetched) > 0
                ? (totalBlocksFromCache * 100.0 / (totalBlocksFromCache + totalBlocksFetched))
                : 0;

            logger.info(
                "[{}] Cumulative stats for file '{}': {} bytes read, {} blocks from cache, {} blocks from primary, "
                    + "cache hit rate: {}%, avg {} ms/block",
                shardId,
                fileName,
                totalBytesRead,
                totalBlocksFromCache,
                totalBlocksFetched,
                String.format("%.2f", cacheHitRate),
                totalBlocksFetched > 0
                    ? String.format("%.2f", TimeUnit.NANOSECONDS.toMillis(totalFetchTimeNanos) / (double) totalBlocksFetched)
                    : "0.00"
            );

        } catch (Exception e) {
            int actualBytesRead = dest.position() - initialPosition;
            logger.error(
                "[{}] Error reading file '{}' at position {}, requested {} bytes, got {} bytes",
                shardId,
                fileName,
                startPosition,
                bytesToRead,
                actualBytesRead,
                e
            );
            throw e;
        }
    }

    private BytesReference fetchBlockFromPrimary(long blockId) throws IOException {
        // Calculate the offset and length for this block
        long offset = blockId * blockSize;
        int blockLength = (int) Math.min(blockSize, fileLength - offset);

        if (blockLength <= 0) {
            logger.warn(
                "[{}] Attempted to read past end of file '{}' at offset {} (fileLength: {})",
                shardId,
                fileName,
                offset,
                fileLength
            );
            throw new EOFException("Attempted to read past end of file " + fileName + " at offset " + offset);
        }

        // Create the request
        FetchSegmentBlockRequest request = new FetchSegmentBlockRequest(shardId, fileName, offset, blockLength);
        String primaryNodeName = clusterService.state().nodes().get(primaryNodeId).getName();

        logger.info(
            "[{}] Requesting block {} for file '{}' from primary node [{}] (offset: {}, length: {})",
            shardId,
            blockId,
            fileName,
            primaryNodeName,
            offset,
            blockLength
        );

        CompletableFuture<BytesReference> future = new CompletableFuture<>();
        long startTime = System.nanoTime();

        // TODO - Replace this with RetryableTransportClient
        try {
            transportService.sendRequest(
                clusterService.state().nodes().get(primaryNodeId),
                FetchSegmentBlockAction.NAME,
                request,
                new ActionListenerResponseHandler<>(ActionListener.wrap(response -> {
                    long endTime = System.nanoTime();
                    long durationMs = TimeUnit.NANOSECONDS.toMillis(endTime - startTime);

                    logger.info(
                        "[{}] Successfully received block {} for file '{}' from primary node [{}] in {} ms (size: {} bytes)",
                        shardId,
                        blockId,
                        fileName,
                        primaryNodeName,
                        durationMs,
                        response.getBlockData().length()
                    );

                    future.complete(response.getBlockData());
                }, e -> {
                    long endTime = System.nanoTime();
                    long durationMs = TimeUnit.NANOSECONDS.toMillis(endTime - startTime);

                    logger.error(
                        "[{}] Error fetching block {} for file '{}' from primary node [{}] after {} ms",
                        shardId,
                        blockId,
                        fileName,
                        primaryNodeName,
                        durationMs,
                        e
                    );
                    future.completeExceptionally(e);
                }), FetchSegmentBlockResponse::new)
            );

            // Wait for the response with timeout
            logger.info(
                "[{}] Waiting up to {} seconds for response for block {} of file '{}'",
                shardId,
                REQUEST_TIMEOUT_SECONDS,
                blockId,
                fileName
            );

            BytesReference result = future.get(REQUEST_TIMEOUT_SECONDS, TimeUnit.SECONDS);

            // Verify the response length
            if (result.length() != blockLength) {
                logger.warn(
                    "[{}] Received block {} with incorrect length for file '{}'. Expected: {}, Got: {}",
                    shardId,
                    blockId,
                    fileName,
                    blockLength,
                    result.length()
                );
                throw new IOException("Block fetch returned incorrect length for block " + blockId);
            }

            return result;
        } catch (TimeoutException te) {
            logger.error(
                "[{}] Timeout waiting for block {} of file '{}' from primary node [{}] after {} seconds",
                shardId,
                blockId,
                fileName,
                primaryNodeName,
                REQUEST_TIMEOUT_SECONDS
            );
            throw new IOException("Timeout waiting for block " + blockId + " from primary node " + primaryNodeId, te);
        } catch (Exception e) {
            logger.error(
                "[{}] Failed to fetch block {} for file '{}' from primary node [{}]",
                shardId,
                blockId,
                fileName,
                primaryNodeName,
                e
            );
            throw new IOException("Failed to fetch block " + blockId + " for file " + fileName, e);
        }
    }

    @Override
    protected void seekInternal(long pos) {
        logger.info("[{}] Seeking to position {} in file '{}' (file length: {})", shardId, pos, fileName, fileLength);
    }

    @Override
    public void close() {
        logger.info(
            "[{}] Closing BlockFetchIndexInput for file '{}'. Total bytes read: {}, blocks: {} from cache, {} from primary",
            shardId,
            fileName,
            totalBytesRead,
            totalBlocksFromCache,
            totalBlocksFetched
        );
    }

    @Override
    public long length() {
        return fileLength;
    }

    @Override
    public IndexInput slice(String sliceDescription, long offset, long length) throws IOException {
        if (offset < 0 || length < 0 || offset + length > this.fileLength) {
            logger.warn(
                "[{}] Invalid slice parameters for file '{}': sliceDescription='{}', offset={}, length={}, fileLength={}",
                shardId,
                fileName,
                sliceDescription,
                offset,
                length,
                fileLength
            );
            throw new IllegalArgumentException(
                "slice(" + sliceDescription + ", " + offset + ", " + length + ") out of bounds: " + this.fileLength
            );
        }

        logger.info("[{}] Creating slice '{}' of file '{}' (offset: {}, length: {})", shardId, sliceDescription, fileName, offset, length);

        return new SlicedBlockFetchIndexInput(sliceDescription, this, offset, length);
    }

    @Override
    public BlockFetchIndexInput clone() {
        logger.info("[{}] Cloning BlockFetchIndexInput for file '{}'", shardId, fileName);
        return (BlockFetchIndexInput) super.clone();
    }

    /**
     * Sliced version of BlockFetchIndexInput for efficient random access
     */
    private static class SlicedBlockFetchIndexInput extends BufferedIndexInput {
        private final BlockFetchIndexInput base;
        private final long offset;
        private final long length;
        private final Logger logger = LogManager.getLogger(SlicedBlockFetchIndexInput.class);

        SlicedBlockFetchIndexInput(String sliceDescription, BlockFetchIndexInput base, long offset, long length) {
            super("SlicedBlockFetchIndexInput(" + sliceDescription + " in " + base.fileName + ")", base.getBufferSize());
            this.base = base;
            this.offset = offset;
            this.length = length;

            logger.info(
                "[{}] Created SlicedBlockFetchIndexInput '{}' for file '{}' (offset: {}, length: {})",
                base.shardId,
                sliceDescription,
                base.fileName,
                offset,
                length
            );
        }

        @Override
        protected void readInternal(ByteBuffer dest) throws IOException {
            long pos = getFilePointer();

            logger.info(
                "[{}] SlicedBlockFetchIndexInput '{}' reading at position {} (max length: {}, bytes requested: {})",
                base.shardId,
                toString(),
                pos,
                length,
                dest.remaining()
            );

            if (pos >= length) {
                logger.warn(
                    "[{}] SlicedBlockFetchIndexInput '{}' attempt to read past EOF at position {} (length: {})",
                    base.shardId,
                    toString(),
                    pos,
                    length
                );
                throw new EOFException("Read past EOF: " + this);
            }

            int available = (int) Math.min(dest.remaining(), length - pos);

            logger.info(
                "[{}] SlicedBlockFetchIndexInput '{}' reading {} bytes at position {} (absolute position in file: {})",
                base.shardId,
                toString(),
                available,
                pos,
                offset + pos
            );

            // Create a view of the buffer with limited space
            int oldLimit = dest.limit();
            dest.limit(dest.position() + available);

            // Read into this limited view
            try {
                base.seek(offset + pos);
                base.readInternal(dest);

                logger.info(
                    "[{}] SlicedBlockFetchIndexInput '{}' successfully read {} bytes",
                    base.shardId,
                    toString(),
                    dest.position() - (dest.limit() - available)
                );
            } catch (Exception e) {
                logger.error(
                    "[{}] SlicedBlockFetchIndexInput '{}' error reading {} bytes at position {}",
                    base.shardId,
                    toString(),
                    available,
                    pos,
                    e
                );
                throw e;
            } finally {
                // Restore original limit
                dest.limit(oldLimit);
            }
        }

        @Override
        protected void seekInternal(long pos) {
            logger.info(
                "[{}] SlicedBlockFetchIndexInput '{}' seeking to position {} (max length: {})",
                base.shardId,
                toString(),
                pos,
                length
            );
        }

        @Override
        public void close() {
            logger.info("[{}] Closing SlicedBlockFetchIndexInput '{}' for file '{}'", base.shardId, toString(), base.fileName);
        }

        @Override
        public long length() {
            return length;
        }

        @Override
        public IndexInput slice(String sliceDescription, long sliceOffset, long sliceLength) throws IOException {
            if (sliceOffset < 0 || sliceLength < 0 || sliceOffset + sliceLength > this.length) {
                logger.warn(
                    "[{}] Invalid sub-slice parameters: sliceDescription='{}', offset={}, length={}, maxLength={}",
                    base.shardId,
                    sliceDescription,
                    sliceOffset,
                    sliceLength,
                    this.length
                );
                throw new IllegalArgumentException(
                    "slice(" + sliceDescription + ", " + sliceOffset + ", " + sliceLength + ") out of bounds: " + this
                );
            }

            logger.info(
                "[{}] Creating sub-slice '{}' of slice '{}' (offset: {}, length: {}, absolute offset in file: {})",
                base.shardId,
                sliceDescription,
                toString(),
                sliceOffset,
                sliceLength,
                this.offset + sliceOffset
            );

            return base.slice(sliceDescription, this.offset + sliceOffset, sliceLength);
        }

        @Override
        public SlicedBlockFetchIndexInput clone() {
            logger.info("[{}] Cloning SlicedBlockFetchIndexInput '{}' for file '{}'", base.shardId, toString(), base.fileName);

            // Create a new instance with a cloned base
            return new SlicedBlockFetchIndexInput(toString(), base.clone(), offset, length);
        }
    }
}
