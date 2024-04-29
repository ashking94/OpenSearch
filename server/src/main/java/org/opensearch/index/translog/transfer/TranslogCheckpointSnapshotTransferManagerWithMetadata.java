/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.translog.transfer;

import org.opensearch.action.LatchedActionListener;
import org.opensearch.common.blobstore.BlobPath;
import org.opensearch.common.blobstore.stream.write.WritePriority;
import org.opensearch.core.action.ActionListener;
import org.opensearch.index.translog.transfer.FileSnapshot.TransferFileSnapshot;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class TranslogCheckpointSnapshotTransferManagerWithMetadata implements TranslogCheckpointSnapshotTransferManager {

    private final TransferService transferService;

    public TranslogCheckpointSnapshotTransferManagerWithMetadata(TransferService transferService) {
        this.transferService = transferService;
    }

    @Override
    public void transferTranslogCheckpointSnapshot(
        TransferSnapshot transferSnapshot,
        Set<TranslogCheckpointSnapshot> toUpload,
        Map<Long, BlobPath> blobPathMap,
        LatchedActionListener<TranslogCheckpointSnapshot> latchedActionListener,
        WritePriority writePriority
    ) throws Exception {
        Set<TransferFileSnapshot> filesToUpload = new HashSet<>();
        Map<TransferFileSnapshot, TranslogCheckpointSnapshot> map = new HashMap<>();
        for (TranslogCheckpointSnapshot translogCheckpointSnapshot : toUpload) {
            TransferFileSnapshot transferFileSnapshot = translogCheckpointSnapshot.getTranslogFileSnapshotWithMetadata();
            map.put(transferFileSnapshot, translogCheckpointSnapshot);
            filesToUpload.add(transferFileSnapshot);
        }
        ActionListener<TransferFileSnapshot> actionListener = ActionListener.wrap(res -> {
            // TODO - We should also do add to transfer listener here and not earlier.
            latchedActionListener.onResponse(map.get(res));
        }, latchedActionListener::onFailure);
        transferService.uploadBlobs(filesToUpload, blobPathMap, actionListener, WritePriority.HIGH);
    }
}
