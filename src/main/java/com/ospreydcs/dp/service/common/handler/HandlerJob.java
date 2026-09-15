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
