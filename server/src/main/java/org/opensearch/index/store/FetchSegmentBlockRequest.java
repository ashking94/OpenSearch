/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.store;

import org.opensearch.action.ActionRequestValidationException;
import org.opensearch.action.ValidateActions;
import org.opensearch.action.support.single.shard.SingleShardRequest;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.index.shard.ShardId;

import java.io.IOException;

/**
 * Request for fetching a specific block of a segment file from a primary shard.
 * Used by block-level fetch operations during segment replication and search.
 */
public class FetchSegmentBlockRequest extends SingleShardRequest<FetchSegmentBlockRequest> {

    private final ShardId shardId; // Explicit shardId since internalShardId is package-private
    private final String fileName;
    private final long offset;
    private final int length;

    /**
     * Creates a new request to fetch a block of a segment file
     *
     * @param shardId  The shard ID to fetch from
     * @param fileName The name of the segment file
     * @param offset   The offset in the file to start reading from
     * @param length   The number of bytes to read
     */
    public FetchSegmentBlockRequest(ShardId shardId, String fileName, long offset, int length) {
        super(shardId.getIndexName());
        this.shardId = shardId;
        this.fileName = fileName;
        this.offset = offset;
        this.length = length;
    }

    /**
     * Constructor from a stream
     */
    public FetchSegmentBlockRequest(StreamInput in) throws IOException {
        super(in);
        this.shardId = in.readOptionalWriteable(ShardId::new);
        this.fileName = in.readString();
        this.offset = in.readLong();
        this.length = in.readInt();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeOptionalWriteable(shardId);
        out.writeString(fileName);
        out.writeLong(offset);
        out.writeInt(length);
    }

    @Override
    public ActionRequestValidationException validate() {
        ActionRequestValidationException validationException = validateNonNullIndex();

        if (fileName == null || fileName.isEmpty()) {
            validationException = ValidateActions.addValidationError("fileName is required", validationException);
        }

        if (offset < 0) {
            validationException = ValidateActions.addValidationError("offset must be non-negative", validationException);
        }

        if (length <= 0) {
            validationException = ValidateActions.addValidationError("length must be positive", validationException);
        }

        return validationException;
    }

    /**
     * Returns the shard ID to fetch from
     */
    public ShardId getShardId() {
        return shardId;
    }

    /**
     * Returns the name of the segment file
     */
    public String getFileName() {
        return fileName;
    }

    /**
     * Returns the offset in the file to start reading from
     */
    public long getOffset() {
        return offset;
    }

    /**
     * Returns the number of bytes to read
     */
    public int getLength() {
        return length;
    }

    @Override
    public String toString() {
        return "FetchSegmentBlockRequest{"
            + "shardId="
            + shardId
            + ", fileName='"
            + fileName
            + '\''
            + ", offset="
            + offset
            + ", length="
            + length
            + "}";
    }
}
