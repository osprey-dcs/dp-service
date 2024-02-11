package com.ospreydcs.dp.service.ingest.service;

import com.ospreydcs.dp.grpc.v1.common.RejectDetails;
import com.ospreydcs.dp.grpc.v1.common.ResponseType;
import com.ospreydcs.dp.grpc.v1.ingestion.*;
import com.ospreydcs.dp.service.common.grpc.GrpcUtility;
import com.ospreydcs.dp.service.common.model.ValidationResult;
import com.ospreydcs.dp.service.ingest.handler.model.HandlerIngestionRequest;
import com.ospreydcs.dp.service.ingest.handler.IngestionHandlerInterface;
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

    public static int getNumRequestRows(IngestionRequest request) {
        int numRequestValues = 0;
        switch (request.getDataTable().getDataTimeSpec().getValueOneofCase()) {
            case FIXEDINTERVALTIMESTAMPSPEC -> {
                numRequestValues = request.getDataTable().getDataTimeSpec().getFixedIntervalTimestampSpec().getNumSamples();
            }
            case TIMESTAMPLIST -> {
                numRequestValues = request.getDataTable().getDataTimeSpec().getTimestampList().getTimestampsCount();
            }
            case VALUEONEOF_NOT_SET -> {
                numRequestValues = 0;
            }
        }
        return numRequestValues;
    }

    public static IngestionResponse ingestionResponseReject(
            IngestionRequest request, String msg, RejectDetails.RejectReason reason) {

        RejectDetails rejectDetails = RejectDetails.newBuilder()
                .setRejectReason(reason)
                .setMessage(msg)
                .build();
        IngestionResponse response = IngestionResponse.newBuilder()
                .setProviderId(request.getProviderId())
                .setClientRequestId(request.getClientRequestId())
                .setResponseType(ResponseType.REJECT_RESPONSE)
                .setResponseTime(GrpcUtility.getTimestampNow())
                .setRejectDetails(rejectDetails)
                .build();
        return response;
    }

    public static IngestionResponse ingestionResponseAck(IngestionRequest request) {
        int numRows = getNumRequestRows(request);
        int numColumns = request.getDataTable().getDataColumnsCount();
        AckDetails details = AckDetails.newBuilder()
                .setNumRows(numRows)
                .setNumColumns(numColumns)
                .build();
        IngestionResponse response = IngestionResponse.newBuilder()
                .setProviderId(request.getProviderId())
                .setClientRequestId(request.getClientRequestId())
                .setResponseType(ResponseType.ACK_RESPONSE)
                .setResponseTime(GrpcUtility.getTimestampNow())
                .setAckDetails(details)
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
    public StreamObserver<IngestionRequest> streamingIngestion(StreamObserver<IngestionResponse> responseObserver) {

        return new StreamObserver<IngestionRequest>() {

            Instant t0 = Instant.now();
            AtomicBoolean closeRequested = new AtomicBoolean(false);
            AtomicInteger pendingRequestCount = new AtomicInteger(0);
            CountDownLatch pendingRequestLatch = new CountDownLatch(1);

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
            public void onNext(IngestionRequest request) {

                int providerId = request.getProviderId();
                String requestId = request.getClientRequestId();

                logger.debug(
                        "id: {} streamingIngestion.onNext providerId: {} requestId: {}",
                        this.hashCode(), providerId, requestId);

                // add to pending request count, even if we might reject it, to avoid potential race conditions
                int pendingRequestCountValue = pendingRequestCount.incrementAndGet();

                if (closeRequested.get()) {

                    // stream close requested, send a reject for this request
                    logger.error(
                            "providerId: {} requestId: {} request received after stream close will be ignored",
                            providerId, requestId);

                    // decrement pending request count and signal if we are finished
                    decrementPendingRequestCountAndSignalFinish();

                    return;

                } else {

                    // validate request, send error response for invalid request
                    ValidationResult validationResult = handler.validateIngestionRequest(request);
                    boolean validationError = false;
                    String validationMsg = "";

                    if (validationResult.isError) {
                        // send error reject
                        validationError = true;
                        validationMsg = validationResult.msg;
                        IngestionResponse rejectResponse = ingestionResponseReject(
                                request, validationMsg, RejectDetails.RejectReason.INVALID_REQUEST_REASON);
                        responseObserver.onNext(rejectResponse);

                    } else {
                        // send ack response
                        IngestionResponse ackResponse = ingestionResponseAck(request);
                        responseObserver.onNext(ackResponse);
                    }

                    // handle the request, even if rejected
                    HandlerIngestionRequest handlerIngestionRequest =
                            new HandlerIngestionRequest(request, responseObserver, validationError, validationMsg);
                    handler.onNext(handlerIngestionRequest);

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

                Instant t1 = Instant.now();

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

                long dtMillis = t0.until(t1, ChronoUnit.MILLIS);
                double dtSeconds = dtMillis / 1_000.0;
                logger.debug(
                        "id: {} streamingIngestion.onCompleted seconds: {}",
                        this.hashCode(), dtSeconds);
            }
        };
    }

}
