/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.shard;

import org.apache.logging.log4j.Logger;
import org.apache.lucene.index.SegmentInfos;
import org.apache.lucene.search.ReferenceManager;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FilterDirectory;
import org.opensearch.common.concurrent.GatedCloseable;
import org.opensearch.common.logging.Loggers;
import org.opensearch.index.remote.RemoteRefreshSegmentTracker;
import org.opensearch.index.store.RemoteSegmentStoreDirectory;

import java.io.IOException;
import java.util.Collection;

/**
 * This listener updates the remote segment tracker with the segment files of the most recent refresh. This is helpful in
 * determining the lag and hence applying rejection on lagging remote uploads.
 *
 * @opensearch.internal
 */
public class RemoteSegmentTrackerListener implements ReferenceManager.RefreshListener {

    private final Logger logger;
    private final IndexShard indexShard;
    private final RemoteRefreshSegmentTracker segmentTracker;
    private final RemoteSegmentStoreDirectory remoteDirectory;
    private final Directory storeDirectory;
    private long primaryTerm;

    public RemoteSegmentTrackerListener(IndexShard indexShard, RemoteRefreshSegmentTracker segmentTracker) {
        this.indexShard = indexShard;
        this.segmentTracker = segmentTracker;
        logger = Loggers.getLogger(getClass(), indexShard.shardId());
        storeDirectory = indexShard.store().directory();
        remoteDirectory = (RemoteSegmentStoreDirectory) ((FilterDirectory) ((FilterDirectory) indexShard.remoteStore().directory())
            .getDelegate()).getDelegate();
        if (indexShard.routingEntry().primary()) {
            try {
                this.remoteDirectory.init();
            } catch (IOException e) {
                logger.error("Exception while initialising RemoteSegmentStoreDirectory", e);
            }
        }
        this.primaryTerm = remoteDirectory.getPrimaryTermAtInit();
    }

    @Override
    public void beforeRefresh() throws IOException {}

    @Override
    public void afterRefresh(boolean didRefresh) throws IOException {
        if (didRefresh
            || this.primaryTerm != indexShard.getOperationPrimaryTerm()
            || remoteDirectory.getSegmentsUploadedToRemoteStore().isEmpty()) {
            updateLocalRefreshTimeAndSeqNo();
            try {
                if (this.primaryTerm != indexShard.getOperationPrimaryTerm()) {
                    this.primaryTerm = indexShard.getOperationPrimaryTerm();
                }
                try (GatedCloseable<SegmentInfos> segmentInfosGatedCloseable = indexShard.getSegmentInfosSnapshot()) {
                    Collection<String> localSegmentsPostRefresh = segmentInfosGatedCloseable.get().files(true);
                    updateLocalSizeMapAndTracker(localSegmentsPostRefresh);
                }
            } catch (Throwable t) {
                logger.error("Exception in RemoteSegmentTrackerListener.afterRefresh()", t);
            }
        }
    }

    /**
     * Updates map of file name to size of the input segment files in the segment tracker. Uses {@code storeDirectory.fileLength(file)} to get the size.
     *
     * @param segmentFiles list of segment files that are part of the most recent local refresh.
     */
    private void updateLocalSizeMapAndTracker(Collection<String> segmentFiles) {
        segmentTracker.updateLatestLocalFileNameLengthMap(segmentFiles, storeDirectory::fileLength);
    }

    /**
     * Updates the last refresh time and refresh seq no which is seen by local store.
     */
    private void updateLocalRefreshTimeAndSeqNo() {
        segmentTracker.updateLocalRefreshClockTimeMs(System.currentTimeMillis());
        segmentTracker.updateLocalRefreshTimeMs(System.nanoTime() / 1_000_000L);
        segmentTracker.updateLocalRefreshSeqNo(segmentTracker.getLocalRefreshSeqNo() + 1);
    }
}
