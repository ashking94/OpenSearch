/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.shard;

import org.apache.lucene.index.SegmentInfos;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FilterDirectory;
import org.junit.Before;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.routing.ShardRouting;
import org.opensearch.common.concurrent.GatedCloseable;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.index.engine.NRTReplicationEngineFactory;
import org.opensearch.index.remote.RemoteRefreshSegmentTracker;
import org.opensearch.index.store.RemoteDirectory;
import org.opensearch.index.store.RemoteSegmentStoreDirectory;
import org.opensearch.index.store.RemoteSegmentStoreDirectory.MetadataFilenameUtils;
import org.opensearch.index.store.Store;
import org.opensearch.index.store.lockmanager.RemoteStoreLockManager;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.test.RemoteStoreTestUtils;
import org.opensearch.threadpool.ThreadPool;

import java.io.IOException;
import java.util.Collections;
import java.util.concurrent.atomic.AtomicLong;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.opensearch.test.RemoteStoreTestUtils.getDummyMetadata;

public class RemoteSegmentTrackerListenerTests extends IndexShardTestCase {

    private IndexShard indexShard;

    @Before
    public void setup() throws IOException {
        Settings indexSettings = Settings.builder().put(IndexMetadata.SETTING_VERSION_CREATED, org.opensearch.Version.CURRENT).build();
        indexShard = newStartedShard(false, indexSettings, new NRTReplicationEngineFactory());
    }

    @Override
    public void tearDown() throws Exception {
        super.tearDown();
        closeShards(indexShard);
    }

    public void testRemoteDirectoryInitThrowsIOException() throws IOException {
        // Methods used in the constructor of RemoteSegmentTrackerListener have been mocked to reproduce specific exceptions
        // to test the failure modes possible during construction of RemoteSegmentTrackerListener object.

        // Mocking the IndexShard methods and dependent classes.
        ShardId shardId = new ShardId("index1", "_na_", 1);
        IndexShard shard = mock(IndexShard.class);
        Store store = mock(Store.class);
        Directory directory = mock(Directory.class);
        ShardRouting shardRouting = mock(ShardRouting.class);
        when(shard.store()).thenReturn(store);
        when(store.directory()).thenReturn(directory);
        when(shard.shardId()).thenReturn(shardId);
        when(shard.routingEntry()).thenReturn(shardRouting);
        when(shardRouting.primary()).thenReturn(true);

        // Mock the Store, Directory and RemoteSegmentStoreDirectory classes
        Store remoteStore = mock(Store.class);
        when(shard.remoteStore()).thenReturn(remoteStore);
        RemoteDirectory remoteMetadataDirectory = mock(RemoteDirectory.class);
        AtomicLong listFilesCounter = new AtomicLong();

        // Below we are trying to get the IOException thrown in the constructor of the RemoteSegmentStoreDirectory.
        doAnswer(invocation -> {
            if (listFilesCounter.incrementAndGet() <= 1) {
                return Collections.singletonList("dummy string");
            }
            throw new IOException();
        }).when(remoteMetadataDirectory).listFilesByPrefixInLexicographicOrder(MetadataFilenameUtils.METADATA_PREFIX, 1);

        SegmentInfos segmentInfos;
        try (Store indexShardStore = indexShard.store()) {
            segmentInfos = indexShardStore.readLastCommittedSegmentsInfo();
        }

        when(remoteMetadataDirectory.openInput(any(), any())).thenAnswer(
            I -> RemoteStoreTestUtils.createMetadataFileBytes(getDummyMetadata("_0", 1), 1, 12, segmentInfos)
        );
        RemoteSegmentStoreDirectory remoteSegmentStoreDirectory = new RemoteSegmentStoreDirectory(
            mock(RemoteDirectory.class),
            remoteMetadataDirectory,
            mock(RemoteStoreLockManager.class),
            mock(ThreadPool.class)
        );
        FilterDirectory remoteStoreFilterDirectory = new RemoteStoreRefreshListenerTests.TestFilterDirectory(
            new RemoteStoreRefreshListenerTests.TestFilterDirectory(remoteSegmentStoreDirectory)
        );
        when(remoteStore.directory()).thenReturn(remoteStoreFilterDirectory);

        // Since the thrown IOException is caught in the constructor, ctor should be invoked successfully.
        new RemoteSegmentTrackerListener(shard, mock(RemoteRefreshSegmentTracker.class));

        // Validate that the openInput method of remoteMetadataDirectory has been opened only once and the
        // listFilesByPrefixInLexicographicOrder has been called twice.
        verify(remoteMetadataDirectory, times(1)).openInput(any(), any());
        verify(remoteMetadataDirectory, times(2)).listFilesByPrefixInLexicographicOrder(MetadataFilenameUtils.METADATA_PREFIX, 1);
    }

    public void testAfterRefresh() throws IOException {

        // Case 1 - Happy case where local tracker is updated due to didRefresh being true
        long fileSize = OpenSearchTestCase.randomIntBetween(10, 10000);
        IndexShard shard = mockIndexShardForAfterRefreshRun(fileSize);
        ShardId shardId = shard.shardId();
        RemoteRefreshSegmentTracker tracker = new RemoteRefreshSegmentTracker(shardId, 20, 20, 20);
        RemoteSegmentTrackerListener listener = new RemoteSegmentTrackerListener(shard, tracker);

        // Initial values in tracker before the afterRefresh method is triggered
        long localRefreshClockTimeMs = tracker.getLocalRefreshClockTimeMs();
        long localRefreshTimeMs = tracker.getLocalRefreshTimeMs();
        long localRefreshSeqNo = tracker.getLocalRefreshSeqNo();

        waitUntilTimePasses(1);

        listener.afterRefresh(true);
        validateLocalRefreshStats(localRefreshClockTimeMs, localRefreshTimeMs, localRefreshSeqNo, tracker);

        // Case 2 - Happy case where local tracker is updated due to primary term being updated
        shard = mockIndexShardForAfterRefreshRun(fileSize);
        tracker = new RemoteRefreshSegmentTracker(shardId, 20, 20, 20);
        listener = new RemoteSegmentTrackerListener(shard, tracker);

        localRefreshClockTimeMs = tracker.getLocalRefreshClockTimeMs();
        localRefreshTimeMs = tracker.getLocalRefreshTimeMs();
        localRefreshSeqNo = tracker.getLocalRefreshSeqNo();

        waitUntilTimePasses(1);

        listener.afterRefresh(false);
        validateLocalRefreshStats(localRefreshClockTimeMs, localRefreshTimeMs, localRefreshSeqNo, tracker);

        // Case 3 - Happy case where local tracker is updated due to segments uploaded to remote store being empty
        localRefreshClockTimeMs = tracker.getLocalRefreshClockTimeMs();
        localRefreshTimeMs = tracker.getLocalRefreshTimeMs();
        localRefreshSeqNo = tracker.getLocalRefreshSeqNo();

        waitUntilTimePasses(1);

        listener.afterRefresh(false);
        validateLocalRefreshStats(localRefreshClockTimeMs, localRefreshTimeMs, localRefreshSeqNo, tracker);
    }

    public void testAfterRefreshThrowsException() throws IOException {
        IndexShard shard = mockIndexShard();
        ShardId shardId = shard.shardId();
        RemoteRefreshSegmentTracker tracker = new RemoteRefreshSegmentTracker(shardId, 20, 20, 20);
        RemoteSegmentTrackerListener listener = new RemoteSegmentTrackerListener(shard, tracker);
        listener.afterRefresh(true);
        assertTrue(tracker.getLatestLocalFileNameLengthMap().isEmpty());
    }

    private void waitUntilTimePasses(int waitTimeMs) {
        long currentTimeMs = System.currentTimeMillis();
        long currentTimeNs = System.nanoTime();
        while (System.currentTimeMillis() <= currentTimeMs + waitTimeMs || System.nanoTime() <= currentTimeNs + waitTimeMs * 1_000_000L) {
            // Waiting for the time to proceed for the time to reflect incremented
        }
    }

    private void validateLocalRefreshStats(
        long localRefreshClockTimeMs,
        long localRefreshTimeMs,
        long localRefreshSeqNo,
        RemoteRefreshSegmentTracker tracker
    ) {
        assertTrue(tracker.getLocalRefreshClockTimeMs() > localRefreshClockTimeMs);
        assertTrue(tracker.getLocalRefreshTimeMs() > localRefreshTimeMs);
        assertTrue(tracker.getLocalRefreshSeqNo() > localRefreshSeqNo);
        assertTrue(tracker.getLatestLocalFileNameLengthMap().containsKey("segments_3"));
        assertTrue(tracker.getLatestLocalFileNameLengthMap().get("segments_3") > 0);
    }

    private IndexShard mockIndexShardForAfterRefreshRun(long fileSize) throws IOException {
        IndexShard shard = mockIndexShard();

        // Mock segmentInfos to be able to read segment files post refresh and also mock reading the content length of the segment file
        GatedCloseable<SegmentInfos> gatedCloseable = mock(GatedCloseable.class);
        SegmentInfos segmentInfos;
        try (Store indexShardStore = indexShard.store()) {
            segmentInfos = indexShardStore.readLastCommittedSegmentsInfo();
        }
        when(shard.getSegmentInfosSnapshot()).thenReturn(gatedCloseable);
        when(gatedCloseable.get()).thenReturn(segmentInfos);
        Directory storeDirectory = shard.store().directory();
        when(storeDirectory.fileLength(anyString())).thenReturn(fileSize);

        return shard;
    }

    private IndexShard mockIndexShard() throws IOException {
        // Mocking the IndexShard methods and dependent classes.
        ShardId shardId = new ShardId("index1", "_na_", 1);
        IndexShard shard = mock(IndexShard.class);
        Store store = mock(Store.class);
        Directory directory = mock(Directory.class);
        ShardRouting shardRouting = mock(ShardRouting.class);
        when(shard.store()).thenReturn(store);
        when(store.directory()).thenReturn(directory);
        when(shard.shardId()).thenReturn(shardId);
        when(shard.routingEntry()).thenReturn(shardRouting);
        when(shardRouting.primary()).thenReturn(OpenSearchTestCase.randomBoolean());
        when(shard.getOperationPrimaryTerm()).thenReturn((long) (OpenSearchTestCase.randomBoolean() ? 0 : -1));

        // Mock the Store, Directory and RemoteSegmentStoreDirectory classes
        Store remoteStore = mock(Store.class);
        when(shard.remoteStore()).thenReturn(remoteStore);
        RemoteDirectory remoteMetadataDirectory = mock(RemoteDirectory.class);

        when(remoteMetadataDirectory.listFilesByPrefixInLexicographicOrder(anyString(), anyInt())).thenReturn(Collections.emptyList());
        RemoteSegmentStoreDirectory remoteSegmentStoreDirectory = new RemoteSegmentStoreDirectory(
            mock(RemoteDirectory.class),
            remoteMetadataDirectory,
            mock(RemoteStoreLockManager.class),
            mock(ThreadPool.class)
        );
        FilterDirectory remoteStoreFilterDirectory = new RemoteStoreRefreshListenerTests.TestFilterDirectory(
            new RemoteStoreRefreshListenerTests.TestFilterDirectory(remoteSegmentStoreDirectory)
        );
        when(remoteStore.directory()).thenReturn(remoteStoreFilterDirectory);
        return shard;
    }
}
