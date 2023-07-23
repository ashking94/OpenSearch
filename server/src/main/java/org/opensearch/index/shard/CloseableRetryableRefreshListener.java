/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.shard;

import org.apache.logging.log4j.Logger;
import org.apache.lucene.search.ReferenceManager;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.threadpool.ThreadPool;

import java.io.Closeable;
import java.io.IOException;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * RefreshListener that runs afterRefresh method if and only if there is a permit available. Once the listener
 * is closed, all the permits are acquired and there are no available permits to afterRefresh. This abstract class provides
 * necessary abstract methods to schedule retry.
 */
public abstract class CloseableRetryableRefreshListener implements ReferenceManager.RefreshListener, Closeable {

    private static final int TOTAL_PERMITS = Integer.MAX_VALUE;

    private final AtomicBoolean closed = new AtomicBoolean(false);

    private final Semaphore semaphore = new Semaphore(TOTAL_PERMITS);

    private final ThreadPool threadPool;

    /**
     * This boolean is used to ensure that there is only 1 retry scheduled/running at any time.
     */
    private final AtomicBoolean retryScheduled = new AtomicBoolean(false);

    public CloseableRetryableRefreshListener() {
        this.threadPool = null;
    }

    public CloseableRetryableRefreshListener(ThreadPool threadPool) {
        this.threadPool = threadPool;
    }

    @Override
    public final void afterRefresh(boolean didRefresh) throws IOException {
        if (closed.get()) {
            return;
        }
        run(didRefresh, () -> {});
    }

    protected String getRetryThreadPoolName() {
        return null;
    }

    protected TimeValue getNextRetryInterval() {
        return null;
    }

    private void scheduleRetry(TimeValue interval, String retryThreadPoolName, boolean didRefresh) {
        // If the underlying listener has closed, then we do not allow even the retry to be scheduled
        if (closed.get()) {
            return;
        }

        if (this.threadPool == null
            || interval == null
            || retryThreadPoolName == null
            || ThreadPool.THREAD_POOL_TYPES.containsKey(retryThreadPoolName) == false
            || interval == TimeValue.MINUS_ONE
            || retryScheduled.compareAndSet(false, true) == false) {
            return;
        }

        boolean scheduled = false;
        try {
            this.threadPool.schedule(() -> run(didRefresh, () -> retryScheduled.set(false)), interval, retryThreadPoolName);
            scheduled = true;
            getLogger().info("Scheduled retry with didRefresh={}", didRefresh);
        } finally {
            if (scheduled == false) {
                retryScheduled.set(false);
            }
        }
    }

    private void run(boolean didRefresh, Runnable runFinally) {
        boolean successful;
        boolean permitAcquired = semaphore.tryAcquire();
        try {
            successful = permitAcquired && performAfterRefresh(didRefresh);
        } finally {
            if (permitAcquired) {
                semaphore.release();
            }
            runFinally.run();
        }
        assert permitAcquired;
        scheduleRetry(successful, didRefresh);
    }

    /**
     * Schedules the retry based on the {@code afterRefreshSuccessful} value.
     *
     * @param afterRefreshSuccessful is sent true if the performAfterRefresh(..) is successful.
     * @param didRefresh             if the refresh did open a new reference then didRefresh will be true
     */
    private void scheduleRetry(boolean afterRefreshSuccessful, boolean didRefresh) {
        if (afterRefreshSuccessful == false) {
            scheduleRetry(getNextRetryInterval(), getRetryThreadPoolName(), didRefresh);
        }
    }

    /**
     * This method needs to be overridden and be provided with what needs to be run on after refresh.
     *
     * @param didRefresh true if the refresh opened a new reference
     * @return true if a retry is needed else false.
     */
    protected abstract boolean performAfterRefresh(boolean didRefresh);

    @Override
    public final void close() throws IOException {
        try {
            if (semaphore.tryAcquire(TOTAL_PERMITS, 10, TimeUnit.MINUTES)) {
                boolean result = closed.compareAndSet(false, true);
                assert result && semaphore.availablePermits() == 0;
            } else {
                throw new TimeoutException("timeout while closing gated refresh listener");
            }
        } catch (InterruptedException | TimeoutException e) {
            throw new RuntimeException("Failed to close the closeable retryable listener", e);
        }
    }

    protected abstract Logger getLogger();
}
