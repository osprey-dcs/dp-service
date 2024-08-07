package com.ospreydcs.dp.service.ingest.service;

import com.ospreydcs.dp.grpc.v1.common.ExceptionalResult;
import com.ospreydcs.dp.grpc.v1.ingestion.*;
import com.ospreydcs.dp.service.common.grpc.TimestampUtility;
import com.ospreydcs.dp.service.common.model.ValidationResult;
import com.ospreydcs.dp.service.ingest.handler.IngestionValidationUtility;
import com.ospreydcs.dp.service.ingest.handler.model.HandlerIngestionRequest;
import com.ospreydcs.dp.service.ingest.handler.interfaces.IngestionHandlerInterface;
import io.grpc.stub.StreamObserver;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;

public class IngestionServiceImpl extends DpIngestionServiceGrpc.DpIngestionServiceImplBase {

    private static final Logger logger = LogManager.getLogger();

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
                .setResponseTime(TimestampUtility.getTimestampNow())
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
                .setResponseTime(TimestampUtility.getTimestampNow())
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

        // close response stream, this is a unary single-response rpc
        responseObserver.onCompleted();
    }

    @Override
    public StreamObserver<IngestDataRequest> ingestDataStream(StreamObserver<IngestDataStreamResponse> responseObserver) {
        logger.debug("ingestDataStream");
        return new IngestDataStreamRequestObserver(responseObserver, handler, this);
    }

    @Override
    public StreamObserver<IngestDataRequest> ingestDataBidiStream(StreamObserver<IngestDataResponse> responseObserver) {
        logger.debug("ingestDataBidiStream");
        return new IngestDataBidiStreamRequestObserver(responseObserver, handler, this);
    }
    
    protected void handleIngestionRequest(
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
                new HandlerIngestionRequest(request, validationError, validationMsg);
        handler.handleIngestionRequest(handlerIngestionRequest);
    }

    public void sendIngestDataStreamResponse(
            StreamObserver<IngestDataStreamResponse> responseObserver,
            List<String> requestIdList,
            List<String> rejectedIdList
    ) {
        // build response object
        IngestDataStreamResponse.Builder responseBuilder = IngestDataStreamResponse.newBuilder();
        responseBuilder.setResponseTime(TimestampUtility.getTimestampNow());
        responseBuilder.addAllClientRequestIds(requestIdList);
        responseBuilder.addAllRejectedRequestIds(rejectedIdList);

        if (rejectedIdList.size() > 0) {
            // send ExceptionalResult payload indicating failure
            ExceptionalResult exceptionalResult = ExceptionalResult.newBuilder()
                    .setExceptionalResultStatus(ExceptionalResult.ExceptionalResultStatus.RESULT_STATUS_REJECT)
                    .setMessage("one or more requests were rejected")
                    .build();
            responseBuilder.setExceptionalResult(exceptionalResult);

        } else {
            // send IngestDataStreamResult payload indicating success
            IngestDataStreamResponse.IngestDataStreamResult successfulResult =
                    IngestDataStreamResponse.IngestDataStreamResult.newBuilder()
                            .setNumRequests(requestIdList.size())
                            .build();
            responseBuilder.setIngestDataStreamResult(successfulResult);
        }

        IngestDataStreamResponse response = responseBuilder.build();
        responseObserver.onNext(response);
    }

    private static QueryRequestStatusResponse queryRequestStatusResponseExceptionalResult(
            String msg, ExceptionalResult.ExceptionalResultStatus status
    ) {
        final ExceptionalResult exceptionalResult = ExceptionalResult.newBuilder()
                .setExceptionalResultStatus(status)
                .setMessage(msg)
                .build();

        final QueryRequestStatusResponse response = QueryRequestStatusResponse.newBuilder()
                .setResponseTime(TimestampUtility.getTimestampNow())
                .setExceptionalResult(exceptionalResult)
                .build();

        return response;
    }

    private static QueryRequestStatusResponse queryRequestStatusResponseReject(String msg
    ) {
        return queryRequestStatusResponseExceptionalResult(
                msg, ExceptionalResult.ExceptionalResultStatus.RESULT_STATUS_REJECT);
    }

    private static QueryRequestStatusResponse queryRequestStatusResponseError(String msg
    ) {
        return queryRequestStatusResponseExceptionalResult(
                msg, ExceptionalResult.ExceptionalResultStatus.RESULT_STATUS_ERROR);
    }

    public static QueryRequestStatusResponse queryRequestStatusResponseEmpty(
    ) {
        return queryRequestStatusResponseExceptionalResult(
                "query returned no data", ExceptionalResult.ExceptionalResultStatus.RESULT_STATUS_EMPTY);
    }

    public static QueryRequestStatusResponse queryRequestStatusResponse(
            QueryRequestStatusResponse.RequestStatusResult result
    ) {
        return QueryRequestStatusResponse.newBuilder()
                .setResponseTime(TimestampUtility.getTimestampNow())
                .setRequestStatusResult(result)
                .build();
    }

    public static void sendQueryRequestStatusResponseReject(
            String msg, StreamObserver<QueryRequestStatusResponse> responseObserver
    ) {
        final QueryRequestStatusResponse response = queryRequestStatusResponseReject(msg);
        responseObserver.onNext(response);
        responseObserver.onCompleted();
    }

    public static void sendQueryRequestStatusResponseError(
            String msg, StreamObserver<QueryRequestStatusResponse> responseObserver
    ) {
        final QueryRequestStatusResponse response = queryRequestStatusResponseError(msg);
        responseObserver.onNext(response);
        responseObserver.onCompleted();
    }

    public static void sendQueryRequestStatusResponseEmpty(
            StreamObserver<QueryRequestStatusResponse> responseObserver
    ) {
        final QueryRequestStatusResponse response = queryRequestStatusResponseEmpty();
        responseObserver.onNext(response);
        responseObserver.onCompleted();
    }

    public static void sendQueryRequestStatusResponse(
            QueryRequestStatusResponse.RequestStatusResult requestStatusResult,
            StreamObserver<QueryRequestStatusResponse> responseObserver
    ) {
        final QueryRequestStatusResponse response  = queryRequestStatusResponse(requestStatusResult);
        responseObserver.onNext(response);
        responseObserver.onCompleted();
    }

    @Override
    public void queryRequestStatus(
            QueryRequestStatusRequest request,
            StreamObserver<QueryRequestStatusResponse> responseObserver
    ) {
        logger.info("id: {} queryRequestStatus request received", responseObserver.hashCode());

        // check that request contains non-empty list of criteria
        final List<QueryRequestStatusRequest.QueryRequestStatusCriterion> criterionList = request.getCriteriaList();
        if (criterionList.size() == 0) {
            final String errorMsg = "QueryRequestStatusRequest.criteria list must not be empty";
            sendQueryRequestStatusResponseReject(errorMsg, responseObserver);
        }

        // validate query criteria
        for (QueryRequestStatusRequest.QueryRequestStatusCriterion criterion : criterionList) {

            switch (criterion.getCriterionCase()) {

                case PROVIDERIDCRITERION -> {
                    final QueryRequestStatusRequest.QueryRequestStatusCriterion.ProviderIdCriterion providerIdCriterion = 
                            criterion.getProviderIdCriterion();
                    if (providerIdCriterion.getProviderId() < 1) {
                        final String errorMsg = "QueryRequestStatusRequest.ProviderIdCriterion.providerId must be greater than 0";
                        sendQueryRequestStatusResponseReject(errorMsg, responseObserver);
                        return;
                    }
                }
                
                case PROVIDERNAMECRITERION -> {
                    final QueryRequestStatusRequest.QueryRequestStatusCriterion.ProviderNameCriterion providerNameCriterion =
                            criterion.getProviderNameCriterion();
                    if (providerNameCriterion.getProviderName().isBlank()) {
                        final String errorMsg = "QueryRequestStatusRequest.ProviderNameCriterion.providerName must not be blank";
                        sendQueryRequestStatusResponseReject(errorMsg, responseObserver);
                        return;
                    }
                }
                
                case REQUESTIDCRITERION -> {
                    final QueryRequestStatusRequest.QueryRequestStatusCriterion.RequestIdCriterion requestIdCriterion =
                            criterion.getRequestIdCriterion();
                    if (requestIdCriterion.getRequestId().isBlank()) {
                        final String errorMsg = "QueryRequestStatusRequest.RequestIdCriterion.requestId must not be blank";
                        sendQueryRequestStatusResponseReject(errorMsg, responseObserver);
                        return;
                    }
                }
                
                case STATUSCRITERION -> {
//                    final QueryRequestStatusRequest.QueryRequestStatusCriterion.StatusCriterion statusCriterion =
//                            criterion.getStatusCriterion();
//                    if (statusCriterion.getStatus().) {
//                        final String errorMsg = "QueryRequestStatusRequest.StatusCriterion.status must";
//                        sendQueryRequestStatusResponseReject(errorMsg, responseObserver);
//                        return;
//                    }
                }
                
                case TIMERANGECRITERION -> {
                    final QueryRequestStatusRequest.QueryRequestStatusCriterion.TimeRangeCriterion timeRangeCriterion =
                            criterion.getTimeRangeCriterion();
                    if (timeRangeCriterion.getBeginTime().getEpochSeconds() < 1) {
                        final String errorMsg = "QueryRequestStatusRequest.TimeRangeCriterion.beginTime seconds must be greater than 0";
                        sendQueryRequestStatusResponseReject(errorMsg, responseObserver);
                        return;
                    }
                }
                
                case CRITERION_NOT_SET -> {
                    final String errorMsg = "QueryRequestStatusRequest.criterion is not set";
                    sendQueryRequestStatusResponseReject(errorMsg, responseObserver);
                    return;
                }
            }
        }

        handler.handleQueryRequestStatus(request, responseObserver);
    }
}
