package com.ospreydcs.dp.service.query.service;

import com.ospreydcs.dp.grpc.v1.common.RejectionDetails;
import com.ospreydcs.dp.grpc.v1.common.ResponseType;
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

    public static QueryDataResponse queryResponseDataReject(String msg, RejectionDetails.Reason reason) {

        final RejectionDetails rejectionDetails = RejectionDetails.newBuilder()
                .setReason(reason)
                .setMessage(msg)
                .build();

        final QueryDataResponse response = QueryDataResponse.newBuilder()
                .setResponseType(ResponseType.REJECT_RESPONSE)
                .setResponseTime(GrpcUtility.getTimestampNow())
                .setRejectionDetails(rejectionDetails)
                .build();

        return response;
    }

    private static QueryDataResponse dataResponseWithResult(
            QueryDataResponse.QueryResult queryResult, ResponseType responseType
    ) {
        return QueryDataResponse.newBuilder()
                .setResponseType(responseType)
                .setResponseTime(GrpcUtility.getTimestampNow())
                .setQueryResult(queryResult)
                .build();
    }

    private static QueryMetadataResponse metadataResponseWithResult(
            QueryMetadataResponse.QueryResult queryResult, ResponseType responseType
    ) {
        return QueryMetadataResponse.newBuilder()
                .setResponseType(responseType)
                .setResponseTime(GrpcUtility.getTimestampNow())
                .setQueryResult(queryResult)
                .build();
    }

    private static QueryStatus queryStatus(
            String msg, QueryStatus.QueryStatusType statusType
    ) {
        QueryStatus.Builder statusBuilder = QueryStatus.newBuilder();
        statusBuilder.setQueryStatusType(statusType);
        if (msg != null) {
            statusBuilder.setStatusMessage(msg);
        }
        return statusBuilder.build();
    }

    public static QueryDataResponse queryResponseDataError(String msg) {

        final QueryStatus errorStatus =
                queryStatus(msg, QueryStatus.QueryStatusType.QUERY_STATUS_ERROR);

        final QueryDataResponse.QueryResult queryResult = QueryDataResponse.QueryResult.newBuilder()
                .setQueryStatus(errorStatus)
                .build();

        return dataResponseWithResult(queryResult, ResponseType.ERROR_RESPONSE);
    }

    public static QueryDataResponse queryResponseDataEmpty() {

        final QueryStatus emptyStatus =
                queryStatus(null, QueryStatus.QueryStatusType.QUERY_STATUS_EMPTY);

        final QueryDataResponse.QueryResult queryResult = QueryDataResponse.QueryResult.newBuilder()
                .setQueryStatus(emptyStatus)
                .build();

        return dataResponseWithResult(queryResult, ResponseType.STATUS_RESPONSE);
    }

    public static QueryDataResponse queryResponseDataNotReady() {

        final QueryStatus emptyStatus =
                queryStatus(null, QueryStatus.QueryStatusType.QUERY_STATUS_NOT_READY);

        final QueryDataResponse.QueryResult queryResult = QueryDataResponse.QueryResult.newBuilder()
                .setQueryStatus(emptyStatus)
                .build();

        return dataResponseWithResult(queryResult, ResponseType.STATUS_RESPONSE);
    }

    public static QueryDataResponse queryResponseData(QueryDataResponse.QueryResult.QueryData.Builder queryDataBuilder) {

        queryDataBuilder.build();
        final QueryDataResponse.QueryResult queryResult = QueryDataResponse.QueryResult.newBuilder()
                .setQueryData(queryDataBuilder)
                .build();
        return dataResponseWithResult(queryResult, ResponseType.DETAIL_RESPONSE);
    }

    public static QueryTableResponse queryResponseTable(QueryTableResponse.QueryResult.TableResult tableResult) {

        final QueryTableResponse.QueryResult queryResult = QueryTableResponse.QueryResult.newBuilder()
                .setTableResult(tableResult)
                .build();

        return QueryTableResponse.newBuilder()
                .setResponseType(ResponseType.DETAIL_RESPONSE)
                .setResponseTime(GrpcUtility.getTimestampNow())
                .setQueryResult(queryResult)
                .build();
    }

    public static QueryMetadataResponse queryResponseMetadataReject(String msg, RejectionDetails.Reason reason) {

        final RejectionDetails rejectionDetails = RejectionDetails.newBuilder()
                .setReason(reason)
                .setMessage(msg)
                .build();

        final QueryMetadataResponse response = QueryMetadataResponse.newBuilder()
                .setResponseType(ResponseType.REJECT_RESPONSE)
                .setResponseTime(GrpcUtility.getTimestampNow())
                .setRejectionDetails(rejectionDetails)
                .build();

        return response;
    }

    public static QueryMetadataResponse queryResponseMetadataError(String msg) {

        final QueryStatus errorStatus =
                queryStatus(msg, QueryStatus.QueryStatusType.QUERY_STATUS_ERROR);

        final QueryMetadataResponse.QueryResult queryResult = QueryMetadataResponse.QueryResult.newBuilder()
                .setQueryStatus(errorStatus)
                .build();

        return metadataResponseWithResult(queryResult, ResponseType.ERROR_RESPONSE);
    }

    public static QueryMetadataResponse queryResponseMetadataEmpty() {

        final QueryStatus emptyStatus =
                queryStatus(null, QueryStatus.QueryStatusType.QUERY_STATUS_EMPTY);

        final QueryMetadataResponse.QueryResult queryResult = QueryMetadataResponse.QueryResult.newBuilder()
                .setQueryStatus(emptyStatus)
                .build();

        return metadataResponseWithResult(queryResult, ResponseType.STATUS_RESPONSE);
    }

    public static QueryMetadataResponse queryResponseMetadata(
            QueryMetadataResponse.QueryResult.MetadataResult metadataResult
    ) {
        final QueryMetadataResponse.QueryResult queryResult = QueryMetadataResponse.QueryResult.newBuilder()
                .setMetadataResult(metadataResult)
                .build();

        return QueryMetadataResponse.newBuilder()
                .setResponseType(ResponseType.DETAIL_RESPONSE)
                .setResponseTime(GrpcUtility.getTimestampNow())
                .setQueryResult(queryResult)
                .build();
    }

    public static void sendQueryResponseDataReject(
            String msg, RejectionDetails.Reason reason, StreamObserver<QueryDataResponse> responseObserver) {

        final QueryDataResponse response = queryResponseDataReject(msg, reason);
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

    public static void sendQueryResponseMetadataReject(
            String msg, RejectionDetails.Reason reason, StreamObserver<QueryMetadataResponse> responseObserver) {

        final QueryMetadataResponse response = queryResponseMetadataReject(msg, reason);
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
            QueryMetadataResponse.QueryResult.MetadataResult metadataResult,
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
            sendQueryResponseDataReject(errorMsg, RejectionDetails.Reason.INVALID_REQUEST_REASON, responseObserver);
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
            sendQueryResponseDataReject(validationMsg, RejectionDetails.Reason.INVALID_REQUEST_REASON, responseObserver);
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
            sendQueryResponseMetadataReject(errorMsg, RejectionDetails.Reason.INVALID_REQUEST_REASON, responseObserver);
            return;
        }

        logger.debug("id: {} column info request received", responseObserver.hashCode());

        QueryMetadataRequest.QuerySpec querySpec = request.getQuerySpec();

        // validate query spec
        boolean isError = false;
        if (querySpec.hasPvNameList()) {
            if (querySpec.getPvNameList().getPvNamesCount() == 0) {
                String errorMsg = "QuerySpec.pvNameList.pvNames must not be empty";
                sendQueryResponseMetadataReject(errorMsg, RejectionDetails.Reason.INVALID_REQUEST_REASON, responseObserver);
                return;
            }

        } else if (querySpec.hasPvNamePattern()) {
            if (querySpec.getPvNamePattern().getPattern().isBlank()) {
                String errorMsg = "QuerySpec.pvNamePattern.pattern must not be empty";
                sendQueryResponseMetadataReject(errorMsg, RejectionDetails.Reason.INVALID_REQUEST_REASON, responseObserver);
                return;
            }
        } else {
            String errorMsg = "column info query must specify either list of column names or column name pattern";
            sendQueryResponseMetadataReject(errorMsg, RejectionDetails.Reason.INVALID_REQUEST_REASON, responseObserver);
            return;
        }

        handler.handleQueryMetadata(querySpec, responseObserver);
    }

}
