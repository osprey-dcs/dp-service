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
    protected static final int TIMEOUT_SECONDS = 60;
    protected static final int MAX_QUEUE_SIZE = 1;
    protected static final int POLL_TIMEOUT_SECONDS = 1;

    // instance variables
    protected ExecutorService executorService = null;
    protected BlockingQueue<HandlerJob> requestQueue = new LinkedBlockingQueue<>(MAX_QUEUE_SIZE);
    private final AtomicBoolean shutdownRequested = new AtomicBoolean(false);

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

        logger.trace("fini");

        // shut down executor service
        try {
            logger.trace("shutting down executorService");
            executorService.shutdown();
            executorService.awaitTermination(TIMEOUT_SECONDS, TimeUnit.SECONDS);
            logger.trace("executorService shutdown completed");
        } catch (InterruptedException ex) {
            executorService.shutdownNow();
            logger.error("InterruptedException in executorService.shutdown: " + ex.getMessage());
            Thread.currentThread().interrupt();
        }

        if (!fini_()) {
            logger.error("error in mongoQueryClient.fini()");
        }

        logger.info("fini shutdown completed");

        return true;
    }

    public boolean start() {
        return true;
    }

    public boolean stop() {
        return true;
    }

}
