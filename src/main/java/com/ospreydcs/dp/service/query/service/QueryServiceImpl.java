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
    protected static final String REQUEST_TABLE = "queryResponseTable";

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

    public static QueryDataResponse queryResponseDataReject(String msg) {
        return queryDataResponseExceptionalResult(msg, ExceptionalResult.ExceptionalResultStatus.RESULT_STATUS_REJECT);
    }

    public static QueryDataResponse queryResponseDataError(String msg) {
        return queryDataResponseExceptionalResult(msg, ExceptionalResult.ExceptionalResultStatus.RESULT_STATUS_ERROR);
    }

    public static QueryDataResponse queryResponseDataEmpty() {
        return queryDataResponseExceptionalResult(
                "query returned no data", ExceptionalResult.ExceptionalResultStatus.RESULT_STATUS_EMPTY);
    }

    public static QueryDataResponse queryResponseDataNotReady() {
        return queryDataResponseExceptionalResult(
                "cursor not ready for operation", ExceptionalResult.ExceptionalResultStatus.RESULT_STATUS_NOT_READY);
    }

    public static QueryDataResponse queryResponseData(QueryDataResponse.QueryData.Builder queryDataBuilder) {
        queryDataBuilder.build();
        return QueryDataResponse.newBuilder()
                .setResponseTime(GrpcUtility.getTimestampNow())
                .setQueryData(queryDataBuilder)
                .build();
    }

    public static QueryTableResponse queryResponseTable(QueryTableResponse.TableResult tableResult) {
        return QueryTableResponse.newBuilder()
                .setResponseTime(GrpcUtility.getTimestampNow())
                .setTableResult(tableResult)
                .build();
    }

    public static void sendQueryResponseDataReject(
            String msg, StreamObserver<QueryDataResponse> responseObserver) {

        final QueryDataResponse response = queryResponseDataReject(msg);
        responseObserver.onNext(response);
        responseObserver.onCompleted();
    }

    public static void sendQueryResponseDataError(String msg, StreamObserver<QueryDataResponse> responseObserver) {
        final QueryDataResponse errorResponse = queryResponseDataError(msg);
        responseObserver.onNext(errorResponse);
        responseObserver.onCompleted();
    }

    public static void sendQueryResponseDataEmpty(StreamObserver<QueryDataResponse> responseObserver) {
        final QueryDataResponse summaryResponse = queryResponseDataEmpty();
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

    public static QueryMetadataResponse queryResponseMetadataReject(String msg) {
        return queryMetadataResponseExceptionalResult(msg, ExceptionalResult.ExceptionalResultStatus.RESULT_STATUS_REJECT);
    }

    public static QueryMetadataResponse queryResponseMetadataError(String msg) {
        return queryMetadataResponseExceptionalResult(msg, ExceptionalResult.ExceptionalResultStatus.RESULT_STATUS_ERROR);
    }

    public static QueryMetadataResponse queryResponseMetadataEmpty() {
        return queryMetadataResponseExceptionalResult(
                "query returned no data", ExceptionalResult.ExceptionalResultStatus.RESULT_STATUS_EMPTY);
    }

    public static QueryMetadataResponse queryResponseMetadata(
            QueryMetadataResponse.MetadataResult metadataResult
    ) {
        return QueryMetadataResponse.newBuilder()
                .setResponseTime(GrpcUtility.getTimestampNow())
                .setMetadataResult(metadataResult)
                .build();
    }

    public static void sendQueryResponseMetadataReject(
            String msg, StreamObserver<QueryMetadataResponse> responseObserver) {

        final QueryMetadataResponse response = queryResponseMetadataReject(msg);
        responseObserver.onNext(response);
        responseObserver.onCompleted();
    }

    public static void sendQueryResponseMetadataError(
            String msg, StreamObserver<QueryMetadataResponse> responseObserver
    ) {
        final QueryMetadataResponse response = queryResponseMetadataError(msg);
        responseObserver.onNext(response);
        responseObserver.onCompleted();
    }

    public static void sendQueryResponseMetadataEmpty(StreamObserver<QueryMetadataResponse> responseObserver) {
        final QueryMetadataResponse response = queryResponseMetadataEmpty();
        responseObserver.onNext(response);
        responseObserver.onCompleted();
    }

    public static void sendQueryResponseMetadata(
            QueryMetadataResponse.MetadataResult metadataResult,
            StreamObserver<QueryMetadataResponse> responseObserver
    ) {
        final QueryMetadataResponse response  = queryResponseMetadata(metadataResult);
        responseObserver.onNext(response);
        responseObserver.onCompleted();
    }

    protected QueryDataRequest.QuerySpec validateQueryDataRequest(
            String requestType, QueryDataRequest request, StreamObserver responseObserver) {

        // check that query request contains a QuerySpec
        if (!request.hasQuerySpec()) {
            String errorMsg = "QueryRequest does not contain a QuerySpec";
            sendQueryResponseDataReject(errorMsg, responseObserver);
            return null;
        }

        QueryDataRequest.QuerySpec querySpec = request.getQuerySpec();

        logger.debug("id: {} query request: {} received columnNames: {} startSeconds: {} endSeconds: {}",
                responseObserver.hashCode(),
                requestType,
                querySpec.getPvNamesList(),
                querySpec.getBeginTime().getEpochSeconds(),
                querySpec.getEndTime().getEpochSeconds());

        // validate request
        ValidationResult validationResult = handler.validateQuerySpecData(querySpec);

        // send reject if request is invalid
        if (validationResult.isError) {
            String validationMsg = validationResult.msg;
            sendQueryResponseDataReject(validationMsg, responseObserver);
            return null;
        }

        return querySpec;
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

        // log and validate request
        QueryDataRequest.QuerySpec querySpec = validateQueryDataRequest(REQUEST_TABLE, request, responseObserver);

        // handle request
        if (querySpec != null) {
            handler.handleQueryDataTable(querySpec, responseObserver);
        }
    }

    public void queryMetadata(QueryMetadataRequest request, StreamObserver<QueryMetadataResponse> responseObserver) {

        // check that query request contains a ColumnInfoQuerySpec
        if (!request.hasQuerySpec()) {
            String errorMsg = "QueryDataRequest does not contain a QuerySpec";
            sendQueryResponseMetadataReject(errorMsg, responseObserver);
            return;
        }

        logger.debug("id: {} column info request received", responseObserver.hashCode());

        QueryMetadataRequest.QuerySpec querySpec = request.getQuerySpec();

        // validate query spec
        boolean isError = false;
        if (querySpec.hasPvNameList()) {
            if (querySpec.getPvNameList().getPvNamesCount() == 0) {
                String errorMsg = "QuerySpec.pvNameList.pvNames must not be empty";
                sendQueryResponseMetadataReject(errorMsg, responseObserver);
                return;
            }

        } else if (querySpec.hasPvNamePattern()) {
            if (querySpec.getPvNamePattern().getPattern().isBlank()) {
                String errorMsg = "QuerySpec.pvNamePattern.pattern must not be empty";
                sendQueryResponseMetadataReject(errorMsg, responseObserver);
                return;
            }
        } else {
            String errorMsg = "column info query must specify either list of column names or column name pattern";
            sendQueryResponseMetadataReject(errorMsg, responseObserver);
            return;
        }

        handler.handleQueryMetadata(querySpec, responseObserver);
    }

}
