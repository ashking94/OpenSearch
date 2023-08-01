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
import java.util.Objects;
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
        assert Objects.nonNull(threadPool);
        this.threadPool = threadPool;
    }

    @Override
    public final void afterRefresh(boolean didRefresh) throws IOException {
        if (closed.get()) {
            return;
        }
        runAfterRefreshExactlyOnce(didRefresh);
        runAfterRefreshWithPermit(didRefresh, () -> {});
    }

    /**
     * The code in this method is executed exactly once. This is done for running non-idempotent function which needs to be
     * executed immediately when afterRefresh method is invoked.
     *
     * @param didRefresh if the refresh did open a new reference then didRefresh will be true
     */
    protected void runAfterRefreshExactlyOnce(boolean didRefresh) {
        // No-op: The implementor would be providing the code
    }

    /**
     * The implementor has the option to override the retry thread pool name. This will be used for scheduling the retries.
     * The method would be invoked each time when a retry is required. By default, it uses the same threadpool for retry.
     *
     * @return the name of the retry thread pool.
     */
    protected String getRetryThreadPoolName() {
        return ThreadPool.Names.SAME;
    }

    /**
     * By default, the retry interval is returned as 1s. The implementor has the option to override the retry interval.
     * This is used for scheduling the next retry. The method would be invoked each time when a retry is required. The
     * implementor can choose any retry strategy and return the next retry interval accordingly.
     *
     * @return the interval for the next retry.
     */
    protected TimeValue getNextRetryInterval() {
        return TimeValue.timeValueSeconds(1);
    }

    /**
     * This method is used to schedule retry which internally calls the performAfterRefresh method under the available permits.
     *
     * @param interval            interval after which the retry would be invoked
     * @param retryThreadPoolName the thread pool name to be used for retry
     * @param didRefresh          if didRefresh is true
     */
    private void scheduleRetry(TimeValue interval, String retryThreadPoolName, boolean didRefresh) {
        // If the underlying listener has closed, then we do not allow even the retry to be scheduled
        if (closed.get() || isRetryEnabled() == false) {
            return;
        }

        assert Objects.nonNull(interval) && ThreadPool.THREAD_POOL_TYPES.containsKey(retryThreadPoolName);
        if (retryScheduled.compareAndSet(false, true) == false) {
            return;
        }

        boolean scheduled = false;
        try {
            this.threadPool.schedule(
                () -> runAfterRefreshWithPermit(didRefresh, () -> retryScheduled.set(false)),
                interval,
                retryThreadPoolName
            );
            scheduled = true;
            getLogger().info("Scheduled retry with didRefresh={}", didRefresh);
        } finally {
            if (scheduled == false) {
                retryScheduled.set(false);
            }
        }
    }

    /**
     * This returns if the retry is enabled or not. By default, the retries are not enabled.
     * @return true if retry is enabled.
     */
    protected boolean isRetryEnabled() {
        return false;
    }

    /**
     * Runs the performAfterRefresh method under permit. If there are no permits available, then it is no-op. It also hits
     * the scheduleRetry method with the result value of the performAfterRefresh method invocation.
     */
    private void runAfterRefreshWithPermit(boolean didRefresh, Runnable runFinally) {
        boolean successful;
        boolean permitAcquired = semaphore.tryAcquire();
        try {
            successful = permitAcquired && performAfterRefreshWithPermit(didRefresh);
        } finally {
            if (permitAcquired) {
                semaphore.release();
            }
            runFinally.run();
        }
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
     * This method needs to be overridden and be provided with what needs to be run on after refresh with permits.
     *
     * @param didRefresh true if the refresh opened a new reference
     * @return true if a retry is needed else false.
     */
    protected abstract boolean performAfterRefreshWithPermit(boolean didRefresh);

    @Override
    public final void close() throws IOException {
        try {
            if (semaphore.tryAcquire(TOTAL_PERMITS, 10, TimeUnit.MINUTES)) {
                boolean result = closed.compareAndSet(false, true);
                assert result && semaphore.availablePermits() == 0;
                getLogger().info("Closed");
            } else {
                throw new TimeoutException("timeout while closing gated refresh listener");
            }
        } catch (InterruptedException | TimeoutException e) {
            throw new RuntimeException("Failed to close the closeable retryable listener", e);
        }
    }

    protected abstract Logger getLogger();

    // Visible for testing

    /**
     * Returns if the retry is scheduled or not.
     *
     * @return boolean as mentioned above.
     */
    boolean getRetryScheduledStatus() {
        return retryScheduled.get();
    }
}
