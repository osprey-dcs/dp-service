package com.ospreydcs.dp.service.query.service;

import com.ospreydcs.dp.grpc.v1.common.RejectDetails;
import com.ospreydcs.dp.grpc.v1.common.ResponseType;
import com.ospreydcs.dp.grpc.v1.query.DpQueryServiceGrpc;
import com.ospreydcs.dp.grpc.v1.query.QueryRequest;
import com.ospreydcs.dp.grpc.v1.query.QueryResponse;
import com.ospreydcs.dp.service.common.grpc.GrpcUtility;
import com.ospreydcs.dp.service.common.model.ValidationResult;
import com.ospreydcs.dp.service.query.handler.QueryHandlerInterface;
import com.ospreydcs.dp.service.query.handler.model.HandlerQueryRequest;
import io.grpc.stub.StreamObserver;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class QueryServiceImpl extends DpQueryServiceGrpc.DpQueryServiceImplBase {

    private static final Logger LOGGER = LogManager.getLogger();

    private static final int TIMEOUT_STREAM_FINISH_MINUTES = 1;

    private QueryHandlerInterface handler;

    public boolean init(QueryHandlerInterface handler) {
        this.handler = handler;
        if (!handler.init()) {
            LOGGER.error("handler.init failed");
            return false;
        }
        if (!handler.start()) {
            LOGGER.error("handler.start failed");
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

    public static QueryResponse queryResponseReject(String msg, RejectDetails.RejectReason reason) {

        final RejectDetails rejectDetails = RejectDetails.newBuilder()
                .setRejectReason(reason)
                .setMessage(msg)
                .build();

        final QueryResponse response = QueryResponse.newBuilder()
                .setResponseType(ResponseType.REJECT_RESPONSE)
                .setResponseTime(GrpcUtility.getTimestampNow())
                .setQueryReject(rejectDetails)
                .build();

        return response;
    }

    private static QueryResponse responseWithReport(QueryResponse.QueryReport report, ResponseType responseType) {
        return QueryResponse.newBuilder()
                .setResponseType(responseType)
                .setResponseTime(GrpcUtility.getTimestampNow())
                .setQueryReport(report)
                .build();
    }

    public static QueryResponse queryResponseError(String msg) {

        final QueryResponse.QueryReport.QueryStatus errorStatus = QueryResponse.QueryReport.QueryStatus.newBuilder()
                .setQueryStatusType(QueryResponse.QueryReport.QueryStatus.QueryStatusType.QUERY_STATUS_ERROR)
                .setStatusMessage(msg)
                .build();

        final QueryResponse.QueryReport queryReport = QueryResponse.QueryReport.newBuilder()
                .setQueryStatus(errorStatus)
                .build();

        return responseWithReport(queryReport, ResponseType.ERROR_RESPONSE);
    }

    public static QueryResponse queryResponseEmpty() {

        final QueryResponse.QueryReport.QueryStatus emptyStatus = QueryResponse.QueryReport.QueryStatus.newBuilder()
                .setQueryStatusType(QueryResponse.QueryReport.QueryStatus.QueryStatusType.QUERY_STATUS_EMPTY)
                .build();

        final QueryResponse.QueryReport queryReport = QueryResponse.QueryReport.newBuilder()
                .setQueryStatus(emptyStatus)
                .build();

        return responseWithReport(queryReport, ResponseType.SUMMARY_RESPONSE);
    }

    public static QueryResponse queryResponseData(QueryResponse.QueryReport.QueryData.Builder resultDataBuilder) {

        resultDataBuilder.build();
        final QueryResponse.QueryReport dataReport = QueryResponse.QueryReport.newBuilder()
                .setQueryData(resultDataBuilder)
                .build();
        return responseWithReport(dataReport, ResponseType.DETAIL_RESPONSE);
    }

    public static void sendQueryResponseReject(
            String msg, RejectDetails.RejectReason reason, StreamObserver<QueryResponse> responseObserver) {

        final QueryResponse rejectResponse = queryResponseReject(msg, reason);
        responseObserver.onNext(rejectResponse);
        responseObserver.onCompleted();
    }

    public static void sendQueryResponseError(String msg, StreamObserver<QueryResponse> responseObserver) {
        final QueryResponse errorResponse = queryResponseError(msg);
        responseObserver.onNext(errorResponse);
        responseObserver.onCompleted();
    }

    public static void sendQueryResponseEmpty(StreamObserver<QueryResponse> responseObserver) {
        final QueryResponse summaryResponse = queryResponseEmpty();
        responseObserver.onNext(summaryResponse);
        responseObserver.onCompleted();
    }

    /*
     * Wraps the supplied ResultData.Builder in a QueryResponse and sends it in the specified response stream.
     */
    public static void sendQueryResponseData(
            QueryResponse.QueryReport.QueryData.Builder resultDataBuilder,
            StreamObserver<QueryResponse> responseObserver) {

        final QueryResponse dataResponse = queryResponseData(resultDataBuilder);
        responseObserver.onNext(dataResponse);
    }

    public static void closeResponseStream(StreamObserver<QueryResponse> responseObserver) {
        responseObserver.onCompleted();
    }

    @Override
    public void queryResponseStream(QueryRequest request, StreamObserver<QueryResponse> responseObserver) {

        // check that query request contains a QuerySpec
        if (!request.hasQuerySpec()) {
            String errorMsg = "QueryRequest does not contain a QuerySpec";
            sendQueryResponseReject(errorMsg, RejectDetails.RejectReason.INVALID_REQUEST_REASON, responseObserver);
            return;
        }

        QueryRequest.QuerySpec querySpec = request.getQuerySpec();

        LOGGER.info("query columnNames: {} startSeconds: {} endSeconds: {}",
                querySpec.getColumnNamesList(),
                querySpec.getStartTime().getEpochSeconds(),
                querySpec.getEndTime().getEpochSeconds());

        // validate request
        ValidationResult validationResult = handler.validateQuerySpec(querySpec);

        // send reject if request is invalid
        if (validationResult.isError) {
            String validationMsg = validationResult.msg;
            sendQueryResponseReject(validationMsg, RejectDetails.RejectReason.INVALID_REQUEST_REASON, responseObserver);
            return;
        }

        // otherwise handle request
        HandlerQueryRequest handlerQueryRequest = new HandlerQueryRequest(querySpec, responseObserver);
        handler.handleQueryRequest(handlerQueryRequest);
    }

}
