package com.ospreydcs.dp.service.common.handler;

import com.ospreydcs.dp.service.common.config.ConfigurationManager;
import com.ospreydcs.dp.service.common.telemetry.DpMetrics;
import io.opentelemetry.api.common.Attributes;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;

public abstract class QueueHandlerBase {

    // static variables
    private static final Logger logger = LogManager.getLogger();

    // constants
    protected static final int TIMEOUT_SECONDS = 10;
    protected static final int MAX_QUEUE_SIZE = 1;
    protected static final int POLL_TIMEOUT_SECONDS = 1;

    // instance variables
    protected ExecutorService executorService = null;
    protected BlockingQueue<HandlerJob> requestQueue = new LinkedBlockingQueue<>(MAX_QUEUE_SIZE);
    protected final AtomicBoolean shutdownRequested = new AtomicBoolean(false);
    private AutoCloseable workersMaxGauge = null;

    // abstract method interface
    protected abstract boolean init_();
    protected abstract boolean fini_();
    protected abstract int getNumWorkers_();

    /**
     * Value of the {@code dp.service} attribute on this handler's metrics; one of the
     * {@code DpMetrics.SERVICE_*} constants. Abstract rather than derived from the class name
     * so the attribute vocabulary stays the fixed set D8 requires — a renamed handler class
     * would otherwise silently start a new time series.
     */
    protected abstract String getServiceName_();

    protected static ConfigurationManager configMgr() {
        return ConfigurationManager.getInstance();
    }

    /**
     * Enqueues a job for the worker pool, blocking while the queue is full. Shared so the
     * interrupt handling lives in one place: on interrupt the job is dropped — the caller's
     * response stream is never answered, acceptable only because the interrupt reaches this
     * thread during shutdown, when the stream is being torn down anyway — and the interrupt
     * flag is restored for the caller.
     */
    protected void enqueueJob(HandlerJob job, int jobId) {
        logger.debug("adding {} id: {} to queue", job.getClass().getSimpleName(), jobId);
        try {
            requestQueue.put(job);
        } catch (InterruptedException e) {
            logger.error("InterruptedException adding {} id: {} to requestQueue, job dropped",
                    job.getClass().getSimpleName(), jobId, e);
            // Record the drop before restoring the interrupt: execute() will never run, so this is
            // the only chance to account for a request that is about to disappear without a
            // response.
            try {
                job.discarded();
            } catch (RuntimeException ex) {
                logger.error("error recording discarded job telemetry: {}", ex.getMessage(), ex);
            }
            Thread.currentThread().interrupt();
        }
    }

    /**
     * Runs one job, recording the D2 handler measurements around it.
     *
     * <p>The {@code workers.active} decrement is in a {@code finally} rather than after the call
     * because the catch below deliberately swallows whatever {@code execute()} throws (the job
     * never dispatches and the caller's stream hangs — the failure mode documented throughout
     * CLAUDE.md). Were the decrement on the normal path only, every such escape would leak a
     * permanent +1, and the gauge that is supposed to reveal saturation would eventually read as
     * saturated on an idle service.
     */
    private void executeJob(HandlerJob job) {

        final String serviceName = getServiceName_();
        final Attributes attributes = Attributes.of(
                DpMetrics.ATTR_SERVICE, serviceName,
                DpMetrics.ATTR_JOB, job.getClass().getSimpleName());

        final long startNanos = System.nanoTime();
        final long queueWaitNanos = startNanos - job.getCreatedNanos();
        job.setQueueWaitNanos(queueWaitNanos);
        DpMetrics.handlerQueueWait().record(DpMetrics.nanosToSeconds(queueWaitNanos), attributes);

        DpMetrics.handlerWorkersActive().add(1, Attributes.of(DpMetrics.ATTR_SERVICE, serviceName));
        try {
            job.execute();
        } catch (Exception ex) {
            logger.error(
                    "{} threw out of execute(), job dropped without dispatching: {}",
                    job.getClass().getSimpleName(), ex.getMessage(), ex);
            ex.printStackTrace(System.err);
        } finally {
            DpMetrics.handlerWorkersActive()
                    .add(-1, Attributes.of(DpMetrics.ATTR_SERVICE, serviceName));
            DpMetrics.handlerJobDuration()
                    .record(DpMetrics.nanosToSeconds(System.nanoTime() - startNanos), attributes);
        }
    }

    private class QueueWorker implements Runnable {

        private final BlockingQueue queue;

        public QueueWorker(BlockingQueue q) {
            this.queue = q;
        }
        public void run() {
            try {
                while (!Thread.currentThread().isInterrupted() && !shutdownRequested.get()) {

//                    // block while waiting for a queue element
//                    HandlerQueryRequest request = (HandlerQueryRequest) queue.take();

                    // poll for next queue item with a timeout
                    HandlerJob job =
                            (HandlerJob) queue.poll(POLL_TIMEOUT_SECONDS, TimeUnit.SECONDS);

                    if (job != null) {
                        executeJob(job);
                    }
                }

                logger.trace("QueryWorker shutting down");

            } catch (InterruptedException ex) {
                logger.error("InterruptedException in QueryWorker.run");
                Thread.currentThread().interrupt();
            }
        }
    }

    public boolean getShutdownRequested() {
        return shutdownRequested.get();
    }

    public boolean init() {

        logger.trace("init");

        if (!init_()) {
            logger.error("error in init_()");
            return false;
        }

        int numWorkers = getNumWorkers_();
        logger.info("init numWorkers: {}", numWorkers);

        // init ExecutorService
        executorService = Executors.newFixedThreadPool(numWorkers);

        for (int i = 1 ; i <= numWorkers ; i++) {
            QueueWorker worker = new QueueWorker(requestQueue);
            executorService.execute(worker);
        }

        // register the observable gauge reporting the configured worker count, closed in fini()
        workersMaxGauge = DpMetrics.registerHandlerWorkersMax(getServiceName_(), this::getNumWorkers_);

        // add a JVM shutdown hook just in case
        final Thread shutdownHook = new Thread(() -> this.fini());
        Runtime.getRuntime().addShutdownHook(shutdownHook);

        return true;
    }

    public boolean fini() {

        if (shutdownRequested.get()) {
            return true;
        }
        shutdownRequested.set(true);

        logger.trace("QueueHandlerBase fini");

        // unregister the workers.max callback so it stops reporting for this handler
        if (workersMaxGauge != null) {
            try {
                workersMaxGauge.close();
            } catch (Exception ex) {
                logger.error("fini exception closing workers.max gauge: {}", ex.getMessage(), ex);
            }
            workersMaxGauge = null;
        }

        // shut down service
        if (!fini_()) {
            logger.error("error in fini_()");
        }

        // shut down executor service thread pool and workers
        logger.trace("fini shutting down executorService");
        executorService.shutdown(); // disable new tasks from being submitted
        try {
            boolean terminated = executorService.awaitTermination(TIMEOUT_SECONDS, TimeUnit.SECONDS);
            if (!terminated) {
                logger.error("fini timed out in executorService.awaitTermination()");
                executorService.shutdownNow(); // cancel currently executing tasks
                if (!executorService.awaitTermination(TIMEOUT_SECONDS, TimeUnit.SECONDS)) {
                    logger.error("fini time out in executorService.shutdownNow()");
                }
            } else {
                logger.trace("executorService shutdown completed");
            }
        } catch (InterruptedException ex) {
            logger.error("fini InterruptedException in executorService.awaitTermination: " + ex.getMessage());
            executorService.shutdownNow();
            Thread.currentThread().interrupt();
        }

        logger.info("QueueHandlerBase fini completed");

        return true;
    }

    public boolean start() {
        return true;
    }

    public boolean stop() {
        return true;
    }

}
