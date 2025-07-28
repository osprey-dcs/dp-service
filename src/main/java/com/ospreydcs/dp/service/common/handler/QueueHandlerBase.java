package com.ospreydcs.dp.service.common.handler;

import com.ospreydcs.dp.service.common.config.ConfigurationManager;
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

    // abstract method interface
    protected abstract boolean init_();
    protected abstract boolean fini_();
    protected abstract int getNumWorkers_();

    protected static ConfigurationManager configMgr() {
        return ConfigurationManager.getInstance();
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
                        try {
                            job.execute();
                        } catch (Exception ex) {
                            logger.error("QueryWorker.run encountered exception: {}", ex.getMessage());
                            ex.printStackTrace(System.err);
                        }
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
