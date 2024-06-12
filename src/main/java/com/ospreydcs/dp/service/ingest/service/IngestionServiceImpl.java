package com.ospreydcs.dp.service.ingest.service;

import com.ospreydcs.dp.grpc.v1.common.ExceptionalResult;
import com.ospreydcs.dp.grpc.v1.ingestion.*;
import com.ospreydcs.dp.service.common.grpc.GrpcUtility;
import com.ospreydcs.dp.service.common.model.ValidationResult;
import com.ospreydcs.dp.service.ingest.handler.IngestionValidationUtility;
import com.ospreydcs.dp.service.ingest.handler.model.HandlerIngestionRequest;
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

public class IngestionServiceImpl extends DpIngestionServiceGrpc.DpIngestionServiceImplBase {

    private static final Logger logger = LogManager.getLogger();

    private static final int TIMEOUT_STREAM_FINISH_MINUTES = 1;

    private IngestionHandlerInterface handler;

    public static int getNumRequestRows(IngestDataRequest request) {
        int numRequestValues = 0;
        switch (request.getIngestionDataFrame().getDataTimestamps().getValueCase()) {
            case SAMPLINGCLOCK -> {
                numRequestValues =
                        request.getIngestionDataFrame().getDataTimestamps().getSamplingClock().getCount();
            }
            case TIMESTAMPLIST -> {
                numRequestValues =
                        request.getIngestionDataFrame().getDataTimestamps().getTimestampList().getTimestampsCount();
            }
            case VALUE_NOT_SET -> {
                numRequestValues = 0;
            }
        }
        return numRequestValues;
    }

    public static IngestDataResponse ingestionResponseReject(
            IngestDataRequest request, String msg) {

        final ExceptionalResult exceptionalResult = ExceptionalResult.newBuilder()
                .setExceptionalResultStatus(ExceptionalResult.ExceptionalResultStatus.RESULT_STATUS_REJECT)
                .setMessage(msg)
                .build();
        final IngestDataResponse response = IngestDataResponse.newBuilder()
                .setProviderId(request.getProviderId())
                .setClientRequestId(request.getClientRequestId())
                .setResponseTime(GrpcUtility.getTimestampNow())
                .setExceptionalResult(exceptionalResult)
                .build();
        return response;
    }

    public static IngestDataResponse ingestionResponseAck(IngestDataRequest request) {
        final int numRows = getNumRequestRows(request);
        final int numColumns = request.getIngestionDataFrame().getDataColumnsCount();
        final IngestDataResponse.AckResult ackResult = IngestDataResponse.AckResult.newBuilder()
                .setNumRows(numRows)
                .setNumColumns(numColumns)
                .build();
        final IngestDataResponse response = IngestDataResponse.newBuilder()
                .setProviderId(request.getProviderId())
                .setClientRequestId(request.getClientRequestId())
                .setResponseTime(GrpcUtility.getTimestampNow())
                .setAckResult(ackResult)
                .build();
        return response;
    }

    public boolean init(IngestionHandlerInterface handler) {
        this.handler = handler;
        if (!handler.init()) {
            logger.error("handler.init failed");
            return false;
        }
        if (!handler.start()) {
            logger.error("handler.start failed");
        }
        return true;
    }

    public void fini() {
        if (handler != null) {
            handler.stop();
            handler.fini();
            handler = null;
        }
    }

    @Override
    public void ingestData(IngestDataRequest request, StreamObserver<IngestDataResponse> responseObserver) {

        logger.debug(
                "ingestData providerId: {} requestId: {}",
                request.getProviderId(), request.getClientRequestId());

        // handle ingestion request
        handleIngestionRequest(request, responseObserver);

    }

    @Override
    public StreamObserver<IngestDataRequest> ingestDataStream(StreamObserver<IngestDataResponse> responseObserver) {

        return new StreamObserver<IngestDataRequest>() {

            final Instant t0 = Instant.now();
            final AtomicBoolean closeRequested = new AtomicBoolean(false);
            final AtomicInteger pendingRequestCount = new AtomicInteger(0);
            final CountDownLatch pendingRequestLatch = new CountDownLatch(1);

            private void decrementPendingRequestCountAndSignalFinish() {
                logger.trace("decrementPendingRequestCountAndSignalFinish");
                // decrement pending request counter, and decrement finishLatch if last request and close requested
                int pendingRequestCountValue = pendingRequestCount.decrementAndGet();
                logger.trace("pendingRequestCountValue: " + pendingRequestCountValue);
                if (closeRequested.get() && pendingRequestCountValue == 0) {
                    logger.trace("decrementing pendingRequestLatch");
                    pendingRequestLatch.countDown();
                }
            }

            @Override
            public void onNext(IngestDataRequest request) {

                final int providerId = request.getProviderId();
                final String requestId = request.getClientRequestId();

                logger.debug(
                        "id: {} streamingIngestion.onNext providerId: {} requestId: {}",
                        this.hashCode(), providerId, requestId);

                // add to pending request count, even if we might reject it, to avoid potential race conditions
                pendingRequestCount.incrementAndGet();

                if (closeRequested.get()) {

                    // stream close requested, send a reject for this request
                    logger.error(
                            "providerId: {} requestId: {} request received after stream close will be ignored",
                            providerId, requestId);

                    // decrement pending request count and signal if we are finished
                    decrementPendingRequestCountAndSignalFinish();

                    return;

                } else {
                    // handle ingestion request
                    handleIngestionRequest(request, responseObserver);

                    // decrement pending request count and signal if we are finished
                    decrementPendingRequestCountAndSignalFinish();
                }
            }

            @Override
            public void onError(Throwable throwable) {
                logger.error("id: {} streamingIngestion.onError: {}", this.hashCode(), throwable.getMessage());
            }

            @Override
            public void onCompleted() {

                final Instant t1 = Instant.now();

                // mark stream as closed and wait for pending requests to complete
                closeRequested.set(true);
                if (pendingRequestCount.get() > 0) {
                    logger.trace("streamingIngestion.onCompleted waiting for pendingRequestLatch");
                    try {
                        if (!pendingRequestLatch.await(TIMEOUT_STREAM_FINISH_MINUTES, TimeUnit.MINUTES)) {
                            logger.error("timeout waiting for finish latch");
                        }
                    } catch (InterruptedException e) {
                        logger.error("InterruptedException waiting for finishLatch");
                    }
                }

                // close the response stream
                responseObserver.onCompleted();

                final long dtMillis = t0.until(t1, ChronoUnit.MILLIS);
                final double dtSeconds = dtMillis / 1_000.0;
                logger.debug(
                        "id: {} streamingIngestion.onCompleted seconds: {}",
                        this.hashCode(), dtSeconds);
            }
        };
    }

    private void handleIngestionRequest(
            IngestDataRequest request,
            StreamObserver<IngestDataResponse> responseObserver
    ) {
        // validate request, send error response for invalid request
        final ValidationResult validationResult = IngestionValidationUtility.validateIngestionRequest(request);
        boolean validationError = false;
        String validationMsg = "";

        if (validationResult.isError) {
            // send error reject
            validationError = true;
            validationMsg = validationResult.msg;
            final IngestDataResponse rejectResponse = ingestionResponseReject(request, validationMsg);
            responseObserver.onNext(rejectResponse);

        } else {
            // send ack response
            final IngestDataResponse ackResponse = ingestionResponseAck(request);
            responseObserver.onNext(ackResponse);
        }

        // handle the request, even if rejected
        final HandlerIngestionRequest handlerIngestionRequest =
                new HandlerIngestionRequest(request, responseObserver, validationError, validationMsg);
        handler.handleIngestDataStream(handlerIngestionRequest);
    }

}
