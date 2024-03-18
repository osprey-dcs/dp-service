package com.ospreydcs.dp.service.query.service;

import com.ospreydcs.dp.grpc.v1.common.ExceptionalResult;
import com.ospreydcs.dp.grpc.v1.query.*;
import com.ospreydcs.dp.service.common.grpc.GrpcUtility;
import com.ospreydcs.dp.service.common.model.ValidationResult;
import com.ospreydcs.dp.service.query.handler.interfaces.QueryHandlerInterface;
import io.grpc.stub.StreamObserver;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class QueryServiceImpl extends DpQueryServiceGrpc.DpQueryServiceImplBase {

    // static variables
    private static final Logger logger = LogManager.getLogger();

    // instance variables
    private QueryHandlerInterface handler;

    // constants
    protected static final String REQUEST_STREAM = "queryResponseStream";
    protected static final String REQUEST_CURSOR = "queryResponseCursor";
    protected static final String REQUEST_SINGLE = "queryResponseSingle";

    public boolean init(QueryHandlerInterface handler) {
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

    private static QueryDataResponse queryDataResponseExceptionalResult(
            String msg, ExceptionalResult.ExceptionalResultStatus status
    ) {
        final ExceptionalResult exceptionalResult = ExceptionalResult.newBuilder()
                .setExceptionalResultStatus(status)
                .setMessage(msg)
                .build();

        final QueryDataResponse response = QueryDataResponse.newBuilder()
                .setResponseTime(GrpcUtility.getTimestampNow())
                .setExceptionalResult(exceptionalResult)
                .build();

        return response;
    }

    public static QueryDataResponse queryDataResponseReject(String msg) {
        return queryDataResponseExceptionalResult(msg, ExceptionalResult.ExceptionalResultStatus.RESULT_STATUS_REJECT);
    }

    public static QueryDataResponse queryDataResponseError(String msg) {
        return queryDataResponseExceptionalResult(msg, ExceptionalResult.ExceptionalResultStatus.RESULT_STATUS_ERROR);
    }

    public static QueryDataResponse queryDataResponseEmpty() {
        return queryDataResponseExceptionalResult(
                "query returned no data", ExceptionalResult.ExceptionalResultStatus.RESULT_STATUS_EMPTY);
    }

    public static QueryDataResponse queryDataResponseNotReady() {
        return queryDataResponseExceptionalResult(
                "cursor not ready for operation", ExceptionalResult.ExceptionalResultStatus.RESULT_STATUS_NOT_READY);
    }

    public static QueryDataResponse queryDataResponse(QueryDataResponse.QueryData.Builder queryDataBuilder) {
        queryDataBuilder.build();
        return QueryDataResponse.newBuilder()
                .setResponseTime(GrpcUtility.getTimestampNow())
                .setQueryData(queryDataBuilder)
                .build();
    }

    public static void sendQueryDataResponseReject(
            String msg, StreamObserver<QueryDataResponse> responseObserver) {

        final QueryDataResponse response = queryDataResponseReject(msg);
        responseObserver.onNext(response);
        responseObserver.onCompleted();
    }

    public static void sendQueryDataResponseError(String msg, StreamObserver<QueryDataResponse> responseObserver) {
        final QueryDataResponse errorResponse = queryDataResponseError(msg);
        responseObserver.onNext(errorResponse);
        responseObserver.onCompleted();
    }

    public static void sendQueryDataResponseEmpty(StreamObserver<QueryDataResponse> responseObserver) {
        final QueryDataResponse summaryResponse = queryDataResponseEmpty();
        responseObserver.onNext(summaryResponse);
        responseObserver.onCompleted();
    }

    private static QueryTableResponse queryTableResponseExceptionalResult(
            String msg, ExceptionalResult.ExceptionalResultStatus status
    ) {
        final ExceptionalResult exceptionalResult = ExceptionalResult.newBuilder()
                .setExceptionalResultStatus(status)
                .setMessage(msg)
                .build();

        final QueryTableResponse response = QueryTableResponse.newBuilder()
                .setResponseTime(GrpcUtility.getTimestampNow())
                .setExceptionalResult(exceptionalResult)
                .build();

        return response;
    }

    public static QueryTableResponse queryTableResponseReject(String msg) {
        return queryTableResponseExceptionalResult(msg, ExceptionalResult.ExceptionalResultStatus.RESULT_STATUS_REJECT);
    }

    public static QueryTableResponse queryTableResponseError(String msg) {
        return queryTableResponseExceptionalResult(msg, ExceptionalResult.ExceptionalResultStatus.RESULT_STATUS_ERROR);
    }

    public static QueryTableResponse queryTableResponseEmpty() {
        return queryTableResponseExceptionalResult(
                "query returned no data", ExceptionalResult.ExceptionalResultStatus.RESULT_STATUS_EMPTY);
    }

    public static QueryTableResponse queryTableResponse(QueryTableResponse.TableResult tableResult) {
        return QueryTableResponse.newBuilder()
                .setResponseTime(GrpcUtility.getTimestampNow())
                .setTableResult(tableResult)
                .build();
    }

    public static void sendQueryTableResponseReject(
            String msg, StreamObserver<QueryTableResponse> responseObserver) {

        final QueryTableResponse response = queryTableResponseReject(msg);
        responseObserver.onNext(response);
        responseObserver.onCompleted();
    }

    public static void sendQueryTableResponseError(String msg, StreamObserver<QueryTableResponse> responseObserver) {
        final QueryTableResponse errorResponse = queryTableResponseError(msg);
        responseObserver.onNext(errorResponse);
        responseObserver.onCompleted();
    }

    public static void sendQueryTableResponseEmpty(StreamObserver<QueryTableResponse> responseObserver) {
        final QueryTableResponse summaryResponse = queryTableResponseEmpty();
        responseObserver.onNext(summaryResponse);
        responseObserver.onCompleted();
    }

    private static QueryMetadataResponse queryMetadataResponseExceptionalResult(
            String msg,
            ExceptionalResult.ExceptionalResultStatus status
    ) {
        final ExceptionalResult exceptionalResult = ExceptionalResult.newBuilder()
                .setExceptionalResultStatus(status)
                .setMessage(msg)
                .build();

        final QueryMetadataResponse response = QueryMetadataResponse.newBuilder()
                .setResponseTime(GrpcUtility.getTimestampNow())
                .setExceptionalResult(exceptionalResult)
                .build();

        return response;
    }

    public static QueryMetadataResponse queryMetadataResponseReject(String msg) {
        return queryMetadataResponseExceptionalResult(msg, ExceptionalResult.ExceptionalResultStatus.RESULT_STATUS_REJECT);
    }

    public static QueryMetadataResponse queryMetadataResponseError(String msg) {
        return queryMetadataResponseExceptionalResult(msg, ExceptionalResult.ExceptionalResultStatus.RESULT_STATUS_ERROR);
    }

    public static QueryMetadataResponse queryMetadataResponseEmpty() {
        return queryMetadataResponseExceptionalResult(
                "query returned no data", ExceptionalResult.ExceptionalResultStatus.RESULT_STATUS_EMPTY);
    }

    public static QueryMetadataResponse queryMetadataResponse(
            QueryMetadataResponse.MetadataResult metadataResult
    ) {
        return QueryMetadataResponse.newBuilder()
                .setResponseTime(GrpcUtility.getTimestampNow())
                .setMetadataResult(metadataResult)
                .build();
    }

    public static void sendQueryMetadataResponseReject(
            String msg, StreamObserver<QueryMetadataResponse> responseObserver) {

        final QueryMetadataResponse response = queryMetadataResponseReject(msg);
        responseObserver.onNext(response);
        responseObserver.onCompleted();
    }

    public static void sendQueryMetadataResponseError(
            String msg, StreamObserver<QueryMetadataResponse> responseObserver
    ) {
        final QueryMetadataResponse response = queryMetadataResponseError(msg);
        responseObserver.onNext(response);
        responseObserver.onCompleted();
    }

    public static void sendQueryMetadataResponseEmpty(StreamObserver<QueryMetadataResponse> responseObserver) {
        final QueryMetadataResponse response = queryMetadataResponseEmpty();
        responseObserver.onNext(response);
        responseObserver.onCompleted();
    }

    public static void sendQueryMetadataResponse(
            QueryMetadataResponse.MetadataResult metadataResult,
            StreamObserver<QueryMetadataResponse> responseObserver
    ) {
        final QueryMetadataResponse response  = queryMetadataResponse(metadataResult);
        responseObserver.onNext(response);
        responseObserver.onCompleted();
    }

    protected QueryDataRequest.QuerySpec validateQueryDataRequest(
            String requestType, QueryDataRequest request,
            StreamObserver<QueryDataResponse> responseObserver
    ) {
        final QueryDataRequest.QuerySpec querySpec = request.getQuerySpec();
        logger.debug("id: {} query request: {} received columnNames: {} startSeconds: {} endSeconds: {}",
                responseObserver.hashCode(),
                requestType,
                querySpec.getPvNamesList(),
                querySpec.getBeginTime().getEpochSeconds(),
                querySpec.getEndTime().getEpochSeconds());

        ValidationResult validationResult = validateQueryDataRequest(request);
        if (validationResult.isError) {
            sendQueryDataResponseReject(validationResult.msg, responseObserver);
            return null;
        } else {
            return querySpec;
        }
    }

    protected ValidationResult validateQueryDataRequest(QueryDataRequest request) {

        // check that query request contains a QuerySpec
        if (!request.hasQuerySpec()) {
            String errorMsg = "QueryRequest does not contain a QuerySpec";
            return new ValidationResult(true, errorMsg);
        }

        QueryDataRequest.QuerySpec querySpec = request.getQuerySpec();

        // validate request
        ValidationResult validationResult = handler.validateQuerySpecData(querySpec);

        // send reject if request is invalid
        if (validationResult.isError) {
            String validationMsg = validationResult.msg;
            return new ValidationResult(true, validationMsg);
        }

        return new ValidationResult(false, "");
    }

    @Override
    public void queryDataStream(QueryDataRequest request, StreamObserver<QueryDataResponse> responseObserver) {

        // log and validate request
        QueryDataRequest.QuerySpec querySpec = validateQueryDataRequest(REQUEST_STREAM, request, responseObserver);

        // handle request
        if (querySpec != null) {
            handler.handleQueryDataStream(querySpec, responseObserver);
        }
    }

    @Override
    public StreamObserver<QueryDataRequest> queryDataBidiStream(StreamObserver<QueryDataResponse> responseObserver) {
        logger.trace("queryDataBidiStream");
        return new QueryResponseBidiStreamRequestStreamObserver(responseObserver, handler, this);
    }

    @Override
    public void queryData(QueryDataRequest request, StreamObserver<QueryDataResponse> responseObserver) {

        // log and validate request
        QueryDataRequest.QuerySpec querySpec = validateQueryDataRequest(REQUEST_SINGLE, request, responseObserver);

        // handle request
        if (querySpec != null) {
            handler.handleQueryData(querySpec, responseObserver);
        }
    }

    @Override
    public void queryDataTable(QueryDataRequest request, StreamObserver<QueryTableResponse> responseObserver) {

        final QueryDataRequest.QuerySpec querySpec = request.getQuerySpec();
        logger.debug("queryDataTable request received id: {} columnNames: {} startSeconds: {} endSeconds: {}",
                responseObserver.hashCode(),
                querySpec.getPvNamesList(),
                querySpec.getBeginTime().getEpochSeconds(),
                querySpec.getEndTime().getEpochSeconds());

        // validate request
        ValidationResult validationResult = validateQueryDataRequest(request);
        if (validationResult.isError) {
            sendQueryTableResponseReject(validationResult.msg, responseObserver);
            return;
        }

        // handle request
        if (querySpec != null) {
            handler.handleQueryDataTable(querySpec, responseObserver);
        }
    }

    public void queryMetadata(QueryMetadataRequest request, StreamObserver<QueryMetadataResponse> responseObserver) {

        logger.debug("id: {} queryMetadata request received", responseObserver.hashCode());

        // validate query spec
        boolean isError = false;
        if (request.hasPvNameList()) {
            if (request.getPvNameList().getPvNamesCount() == 0) {
                String errorMsg = "QueryMetadataRequest.pvNameList.pvNames must not be empty";
                sendQueryMetadataResponseReject(errorMsg, responseObserver);
                return;
            }

        } else if (request.hasPvNamePattern()) {
            if (request.getPvNamePattern().getPattern().isBlank()) {
                String errorMsg = "QueryMetadataRequest.pvNamePattern.pattern must not be empty";
                sendQueryMetadataResponseReject(errorMsg, responseObserver);
                return;
            }
        } else {
            String errorMsg = "QueryMetadataRequest must specify either pvNameList or pvNamePattern";
            sendQueryMetadataResponseReject(errorMsg, responseObserver);
            return;
        }

        handler.handleQueryMetadata(request, responseObserver);
    }

}
