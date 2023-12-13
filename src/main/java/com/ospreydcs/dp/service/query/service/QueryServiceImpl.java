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
                .setRejectDetails(rejectDetails)
                .build();

        return response;
    }

    public static QueryResponse responseWithResult(QueryResponse.QueryResult result, ResponseType responseType) {
        return QueryResponse.newBuilder()
                .setResponseType(responseType)
                .setResponseTime(GrpcUtility.getTimestampNow())
                .setQueryResult(result)
                .build();
    }

    public static QueryResponse.QueryResult queryResultWithSummary(QueryResponse.QueryResult.ResultSummary summary) {
        return QueryResponse.QueryResult.newBuilder()
                .setResultSummary(summary)
                .build();
    }

    public static QueryResponse queryResponseError(String msg) {

        final QueryResponse.QueryResult.ResultSummary errorSummary = QueryResponse.QueryResult.ResultSummary.newBuilder()
                .setIsError(true)
                .setMessage(msg)
                .setNumBuckets(0)
                .build();

        final QueryResponse.QueryResult errorResult = queryResultWithSummary(errorSummary);

        return responseWithResult(errorResult, ResponseType.SUMMARY_RESPONSE);
    }

    public static QueryResponse queryResponseSummary(int numResults) {

        final QueryResponse.QueryResult.ResultSummary querySummary =
                QueryResponse.QueryResult.ResultSummary.newBuilder()
                .setIsError(false)
                .setMessage("")
                .setNumBuckets(numResults)
                .build();

        final QueryResponse.QueryResult summaryResult = queryResultWithSummary(querySummary);

        return responseWithResult(summaryResult, ResponseType.SUMMARY_RESPONSE);
    }

    public static QueryResponse queryResponseData(QueryResponse.QueryResult.ResultData.Builder resultDataBuilder) {

        resultDataBuilder.build();
        final QueryResponse.QueryResult dataResult = QueryResponse.QueryResult.newBuilder()
                .setResultData(resultDataBuilder)
                .build();
        return responseWithResult(dataResult, ResponseType.DETAIL_RESPONSE);
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

    public static void sendQueryResponseSummary(int numResults, StreamObserver<QueryResponse> responseObserver) {
        final QueryResponse summaryResponse = queryResponseSummary(numResults);
        responseObserver.onNext(summaryResponse);
    }

    /*
     * Wraps the supplied ResultData.Builder in a QueryResponse and sends it in the specified response stream.
     */
    public static void sendQueryResponseData(
            QueryResponse.QueryResult.ResultData.Builder resultDataBuilder,
            StreamObserver<QueryResponse> responseObserver) {

        final QueryResponse dataResponse = queryResponseData(resultDataBuilder);
        responseObserver.onNext(dataResponse);
    }

    public static void closeResponseStream(StreamObserver<QueryResponse> responseObserver) {
        responseObserver.onCompleted();
    }

    public void query(QueryRequest request, StreamObserver<QueryResponse> responseObserver) {

        LOGGER.info("query columnNames: {} startSeconds: {} endSeconds: {}",
                request.getColumnNamesList(),
                request.getStartTime().getEpochSeconds(),
                request.getEndTime().getEpochSeconds());

        // validate request
        ValidationResult validationResult = handler.validateQueryRequest(request);
        boolean validationError = false;
        String validationMsg = "";

        // send reject if request is invalid
        if (validationResult.isError) {
            validationError = true;
            validationMsg = validationResult.msg;
            sendQueryResponseReject(validationMsg, RejectDetails.RejectReason.INVALID_REQUEST_REASON, responseObserver);
        }

        // otherwise handle request
        HandlerQueryRequest handlerQueryRequest = new HandlerQueryRequest(request, responseObserver);
        handler.handleQueryRequest(handlerQueryRequest);
    }

}
