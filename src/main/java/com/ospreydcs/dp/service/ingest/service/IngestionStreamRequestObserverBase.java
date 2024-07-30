package com.ospreydcs.dp.service.ingest.service;

import com.ospreydcs.dp.grpc.v1.ingestion.IngestDataRequest;
import com.ospreydcs.dp.service.ingest.handler.interfaces.IngestionHandlerInterface;
import io.grpc.stub.StreamObserver;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

public abstract class IngestionStreamRequestObserverBase implements StreamObserver<IngestDataRequest> {

    // static variables
    private static final Logger logger = LogManager.getLogger();

    // instance variables
    protected final IngestionHandlerInterface handler;
    protected final IngestionServiceImpl serviceImpl;
    private final Instant t0 = Instant.now();
    private final AtomicBoolean closeRequested = new AtomicBoolean(false);
    private final AtomicInteger pendingRequestCount = new AtomicInteger(0);
    private final CountDownLatch pendingRequestLatch = new CountDownLatch(1);

    // constants
    private static final int TIMEOUT_STREAM_FINISH_MINUTES = 1;

    protected abstract void handleIngestionRequest_(IngestDataRequest request);
    protected abstract void handleClose_();

    public IngestionStreamRequestObserverBase(
            IngestionHandlerInterface handler,
            IngestionServiceImpl serviceImpl
    ) {
        this.handler = handler;
        this.serviceImpl = serviceImpl;
    }

    private void decrementPendingRequestCountAndSignalFinish() {
        logger.trace("id: {} decrementPendingRequestCountAndSignalFinish", this.hashCode());
        // decrement pending request counter, and decrement finishLatch if last request and close requested
        int pendingRequestCountValue = pendingRequestCount.decrementAndGet();
        logger.trace("id: {} pendingRequestCountValue: " + pendingRequestCountValue, this.hashCode());
        if (closeRequested.get() && pendingRequestCountValue == 0) {
            logger.trace("id: {} decrementing pendingRequestLatch", this.hashCode());
            pendingRequestLatch.countDown();
        }
    }

    @Override
    public void onNext(IngestDataRequest request) {

        final int providerId = request.getProviderId();
        final String requestId = request.getClientRequestId();

        logger.debug(
                "id: {} onNext providerId: {} requestId: {}",
                this.hashCode(), providerId, requestId);

        // add to pending request count, even if we might reject it, to avoid potential race conditions
        pendingRequestCount.incrementAndGet();

        if (closeRequested.get()) {

            // request received after close request, log and ignore
            logger.error(
                    "id: {} providerId: {} requestId: {} request received after stream close will be ignored",
                    this.hashCode(), providerId, requestId);

            // decrement pending request count and signal if we are finished
            decrementPendingRequestCountAndSignalFinish();

        } else {
            // handle ingestion request
            handleIngestionRequest_(request);

            // decrement pending request count and signal if we are finished
            decrementPendingRequestCountAndSignalFinish();
        }
    }

    @Override
    public void onError(Throwable throwable) {
        logger.error("id: {} onError: {}", this.hashCode(), throwable.getMessage());
    }

    @Override
    public void onCompleted() {

        final Instant t1 = Instant.now();

        // mark stream as closed and wait for pending requests to complete
        closeRequested.set(true);
        if (pendingRequestCount.get() > 0) {
            logger.trace("id: {} onCompleted waiting for pendingRequestLatch", this.hashCode());
            try {
                if (!pendingRequestLatch.await(TIMEOUT_STREAM_FINISH_MINUTES, TimeUnit.MINUTES)) {
                    logger.error("id: {} timeout waiting for finish latch", this.hashCode());
                }
            } catch (InterruptedException e) {
                logger.error("id: {} InterruptedException waiting for finishLatch", this.hashCode());
            }
        }

        // close the response stream
        handleClose_();

        final long dtMillis = t0.until(t1, ChronoUnit.MILLIS);
        final double dtSeconds = dtMillis / 1_000.0;
        logger.debug(
                "id: {} streamingIngestion.onCompleted seconds: {}",
                this.hashCode(), dtSeconds);
    }

}
