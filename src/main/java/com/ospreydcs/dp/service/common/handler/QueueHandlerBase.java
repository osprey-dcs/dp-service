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

    /**
     * How long an idle worker blocks in {@code poll()} before re-checking {@code shutdownRequested}.
     * This bounds how long each idle worker delays {@code fini()}: the flag is read only between
     * polls, and {@code awaitTermination} waits for every worker to notice it. At the former 1 s,
     * each handler's shutdown took a full second, and the integration tests pay that four times per
     * test method (one handler per service in {@code tearDown()}) -- about 4 s of every test, and
     * most of the suite's run time (#250). A job put on the queue is still received immediately; the
     * value affects only how soon an idle worker sees shutdown.
     *
     * <p>Not replaced by an interrupt or a sentinel job: {@code shutdownNow()} would also interrupt
     * in-flight jobs, which {@code fini()} lets finish so their telemetry is recorded, and a
     * sentinel would add a second {@code requestQueue.put} site besides {@link #enqueueJob}.
     */
    protected static final long POLL_TIMEOUT_MILLIS = 100;

    /**
     * Process exit status after a fatal error on a worker (#314). The same status HotSpot uses for
     * {@code -XX:+ExitOnOutOfMemoryError}, so a supervisor sees one value for "out of memory"
     * whichever of the two ended the process.
     */
    static final int FATAL_ERROR_EXIT_STATUS = 3;

    // A constant so that printing it allocates nothing when the heap is exhausted.
    private static final String FATAL_ERROR_MESSAGE =
            "dp-service: fatal error on a handler worker thread, halting the JVM";

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
     * <p>Whatever {@code execute()} throws propagates to {@link QueueWorker#run()}, which decides
     * between logging it and ending the process ({@link #handleWorkerThrowable}). The
     * {@code workers.active} decrement is in a {@code finally} rather than after the call because
     * a job that throws never dispatches and the worker goes on to the next one: were the
     * decrement on the normal path only, every such escape would leak a permanent +1, and the
     * gauge that is supposed to reveal saturation would eventually read as saturated on an idle
     * service.
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
        } finally {
            DpMetrics.handlerWorkersActive()
                    .add(-1, Attributes.of(DpMetrics.ATTR_SERVICE, serviceName));
            DpMetrics.handlerJobDuration()
                    .record(DpMetrics.nanosToSeconds(System.nanoTime() - startNanos), attributes);
        }
    }

    /**
     * True for an error after which the JVM cannot be assumed to be working: any
     * {@link VirtualMachineError} except {@link StackOverflowError}. A stack overflow is local to
     * the thread that hit it and the stack is unwound by the time it is caught, so the worker can
     * take the next job. An {@code OutOfMemoryError} is not local: in #314 the same exhaustion
     * also killed the Mongo driver's cluster monitor thread.
     */
    public static boolean isFatal(Throwable throwable) {
        return throwable instanceof VirtualMachineError && !(throwable instanceof StackOverflowError);
    }

    /**
     * Handles anything thrown on a worker outside a job's own error handling (#314).
     *
     * <p>A non-fatal throwable is logged and the worker continues. The job never dispatched, so
     * its caller's stream gets no response -- the failure mode documented throughout CLAUDE.md --
     * but the worker is not lost with it. A fatal error ({@link #isFatal}) ends the process.
     *
     * <p><b>This method must never throw</b> (PR #318 review): it runs inside the worker's
     * {@code catch}, so anything escaping it ends the worker -- the #314 failure one step later.
     * Reporting can fail: a throwable's {@code toString()} is arbitrary code, and logging or even
     * the first {@code getSimpleName()} for a class allocates on an exhausted heap. So every
     * reporting step is inside a {@code catch (Throwable)}: a fatal error raised while reporting
     * goes to the exit hook like any other, and a non-fatal one is dropped, since there is nothing
     * left to report it with. On the fatal path the exit hook is in a {@code finally}, so no
     * reporting failure can prevent it.
     *
     * @param job the job that was running, or null if the throw came from the queue poll
     */
    private void handleWorkerThrowable(HandlerJob job, Throwable throwable) {
        if (isFatal(throwable)) {
            reportFatalAndExit(job, throwable);
            return;
        }
        try {
            logger.error(
                    "{} threw out of execute(), job dropped without dispatching: {}",
                    jobName(job), throwable.toString(), throwable);
        } catch (Throwable reportingFailure) {
            if (isFatal(reportingFailure)) {
                reportFatalAndExit(job, reportingFailure);
            }
            // non-fatal: dropped, the worker continues
        }
    }

    /** Reports a fatal error as far as the heap allows, then always calls the exit hook. */
    private void reportFatalAndExit(HandlerJob job, Throwable fatal) {
        try {
            System.err.println(FATAL_ERROR_MESSAGE);
            logger.fatal(
                    "{} threw a fatal error, exiting with status {}: {}",
                    jobName(job), FATAL_ERROR_EXIT_STATUS, fatal.toString(), fatal);
        } catch (Throwable reportingFailure) {
            // nothing more can be reported; the exit below is what matters
        } finally {
            exitOnFatalError(fatal);
        }
    }

    private static String jobName(HandlerJob job) {
        return (job == null) ? "queue poll" : job.getClass().getSimpleName();
    }

    /**
     * Ends the process after a fatal error on a worker. {@code halt}, not {@code System.exit}:
     * {@code exit} runs the shutdown hooks, and {@link #fini()} waits for every worker to finish,
     * one of which is the caller. {@code -XX:+ExitOnOutOfMemoryError} skips the hooks as well.
     * Overridable so a test can observe the call without ending the test JVM; production code
     * must not override it.
     */
    protected void exitOnFatalError(Throwable error) {
        Runtime.getRuntime().halt(FATAL_ERROR_EXIT_STATUS);
    }

    private class QueueWorker implements Runnable {

        private final BlockingQueue queue;

        public QueueWorker(BlockingQueue q) {
            this.queue = q;
        }

        /**
         * Runs until shutdown. Nothing a job throws may end this loop (#314): the pool would start
         * a replacement <em>thread</em>, but that thread waits on the pool's own empty task queue
         * rather than on {@code requestQueue}, so it never runs a job, and a handler that loses
         * every worker this way keeps accepting requests and executes none of them. The catch is
         * therefore {@code Throwable}, and it wraps the poll as well as the job.
         */
        public void run() {
            try {
                while (!Thread.currentThread().isInterrupted() && !shutdownRequested.get()) {

                    HandlerJob job = null;
                    try {
                        // poll for next queue item with a timeout
                        job = (HandlerJob) queue.poll(POLL_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS);

                        if (job != null) {
                            executeJob(job);
                        }
                    } catch (InterruptedException ex) {
                        throw ex;
                    } catch (Throwable throwable) {
                        handleWorkerThrowable(job, throwable);
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

        // No JVM shutdown hook is registered here (#293). GrpcServerBase.stopServer() calls fini()
        // after the gRPC server has drained and before telemetry shuts down. A hook of the
        // handler's own ran beside the server's, not after it, so the handler stopped while the
        // server was still draining calls into it; and addShutdownHook() throws once shutdown has
        // begun, which is where an initialization that finishes after a SIGTERM would be.

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
        // Null until init_() has returned, which for a handler with a Mongo client is after the
        // schema migrations (#293). GrpcServerBase does not call fini() on a handler that is
        // still initializing; this keeps that from being one refactoring away from a
        // NullPointerException on the shutdown hook's thread.
        if (executorService == null) {
            logger.info("QueueHandlerBase fini completed; the handler had not finished init()");
            return true;
        }

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
