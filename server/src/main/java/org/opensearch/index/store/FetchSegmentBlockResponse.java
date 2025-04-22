/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.store;

import org.opensearch.core.action.ActionResponse;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;

import java.io.IOException;

/**
 * Response containing the fetched block of data from a segment file.
 * Used in block-level fetch operations during segment replication and search.
 */
public class FetchSegmentBlockResponse extends ActionResponse {

    private final BytesReference blockData;

    /**
     * Constructor for new response with block data
     *
     * @param blockData The block data fetched from the segment file
     */
    public FetchSegmentBlockResponse(BytesReference blockData) {
        this.blockData = blockData;
    }

    /**
     * Constructor from a stream
     */
    public FetchSegmentBlockResponse(StreamInput in) throws IOException {
        super(in);
        blockData = in.readBytesReference();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeBytesReference(blockData);
    }

    /**
     * Returns the fetched block data
     */
    public BytesReference getBlockData() {
        return blockData;
    }

    @Override
    public String toString() {
        return "FetchSegmentBlockResponse{" + "blockSize=" + (blockData != null ? blockData.length() : 0) + " bytes}";
    }
}
