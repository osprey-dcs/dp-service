package com.ospreydcs.dp.service.common.handler;

/**
 * Base class for the units of work executed by a {@link QueueHandlerBase} worker pool.
 *
 * <p>The two timing fields here are written and read by {@code QueueHandlerBase} alone (issue
 * #212, D2), so that every job type in every service is measured by one piece of code and a
 * job added later cannot forget to be instrumented. Subclasses do not touch them; the query
 * service reads {@link #getQueueWaitNanos()} through {@code QueryTelemetry} to attribute the
 * queue stage of a request.
 */
public abstract class HandlerJob {

    /**
     * When this job was constructed, which is immediately before it is enqueued — every handler
     * builds the job and hands it straight to {@code enqueueJob()}. Measured on
     * {@link System#nanoTime()}, so it is comparable only against other readings from the same
     * source, never against wall-clock time.
     */
    private final long createdNanos = System.nanoTime();

    /** Set by the worker before {@link #execute()}; see {@link #getQueueWaitNanos()}. */
    private long queueWaitNanos = 0;

    public abstract void execute();

    /**
     * Called by {@code QueueHandlerBase} when a job is discarded without ever running -- today
     * only when {@code enqueueJob} is interrupted during shutdown.
     *
     * <p>Exists so a dropped job is not silently absent from the metrics. The caller's response
     * stream is never answered in that case (documented at {@code enqueueJob}), and without this
     * hook the request would appear in no counter at all: the query jobs complete their telemetry
     * from {@code execute()}, which never runs. A request that vanished is exactly the one an
     * operator needs to see, so the default is a no-op only for job types that carry no telemetry.
     */
    public void discarded() {
        // no-op by default; job types carrying telemetry override to record the outcome
    }

    public long getCreatedNanos() {
        return createdNanos;
    }

    /**
     * Nanos this job waited between construction and the start of {@link #execute()}. Valid only
     * once the worker has started the job; zero before that.
     */
    public long getQueueWaitNanos() {
        return queueWaitNanos;
    }

    void setQueueWaitNanos(long queueWaitNanos) {
        this.queueWaitNanos = queueWaitNanos;
    }
}
