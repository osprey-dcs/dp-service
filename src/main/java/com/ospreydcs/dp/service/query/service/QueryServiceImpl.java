package com.ospreydcs.dp.service.query.service;

import com.ospreydcs.dp.grpc.v1.common.DataTable;
import com.ospreydcs.dp.grpc.v1.common.RejectDetails;
import com.ospreydcs.dp.grpc.v1.common.ResponseType;
import com.ospreydcs.dp.grpc.v1.query.DpQueryServiceGrpc;
import com.ospreydcs.dp.grpc.v1.query.QueryRequest;
import com.ospreydcs.dp.grpc.v1.query.QueryResponse;
import com.ospreydcs.dp.service.common.grpc.GrpcUtility;
import com.ospreydcs.dp.service.common.model.ValidationResult;
import com.ospreydcs.dp.service.query.handler.interfaces.QueryHandlerInterface;
import io.grpc.stub.StreamObserver;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import javax.xml.crypto.Data;

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

    private static QueryResponse.QueryReport.QueryStatus queryStatus(
            String msg, QueryResponse.QueryReport.QueryStatus.QueryStatusType statusType
    ) {
        QueryResponse.QueryReport.QueryStatus.Builder statusBuilder = QueryResponse.QueryReport.QueryStatus.newBuilder();
        statusBuilder.setQueryStatusType(statusType);
        if (msg != null) {
            statusBuilder.setStatusMessage(msg);
        }
        return statusBuilder.build();
    }

    public static QueryResponse queryResponseError(String msg) {

        final QueryResponse.QueryReport.QueryStatus errorStatus =
                queryStatus(msg, QueryResponse.QueryReport.QueryStatus.QueryStatusType.QUERY_STATUS_ERROR);

        final QueryResponse.QueryReport queryReport = QueryResponse.QueryReport.newBuilder()
                .setQueryStatus(errorStatus)
                .build();

        return responseWithReport(queryReport, ResponseType.ERROR_RESPONSE);
    }

    public static QueryResponse queryResponseEmpty() {

        final QueryResponse.QueryReport.QueryStatus emptyStatus =
                queryStatus(null, QueryResponse.QueryReport.QueryStatus.QueryStatusType.QUERY_STATUS_EMPTY);

        final QueryResponse.QueryReport queryReport = QueryResponse.QueryReport.newBuilder()
                .setQueryStatus(emptyStatus)
                .build();

        return responseWithReport(queryReport, ResponseType.STATUS_RESPONSE);
    }

    public static QueryResponse queryResponseNotReady() {

        final QueryResponse.QueryReport.QueryStatus emptyStatus =
                queryStatus(null, QueryResponse.QueryReport.QueryStatus.QueryStatusType.QUERY_STATUS_NOT_READY);

        final QueryResponse.QueryReport queryReport = QueryResponse.QueryReport.newBuilder()
                .setQueryStatus(emptyStatus)
                .build();

        return responseWithReport(queryReport, ResponseType.STATUS_RESPONSE);
    }

    public static QueryResponse queryResponseData(QueryResponse.QueryReport.BucketData.Builder resultDataBuilder) {

        resultDataBuilder.build();
        final QueryResponse.QueryReport dataReport = QueryResponse.QueryReport.newBuilder()
                .setBucketData(resultDataBuilder)
                .build();
        return responseWithReport(dataReport, ResponseType.DETAIL_RESPONSE);
    }

    public static QueryResponse queryResponseWithTable(DataTable table) {
        final QueryResponse.QueryReport tableReport = QueryResponse.QueryReport.newBuilder()
                .setDataTable(table)
                .build();
        return responseWithReport(tableReport, ResponseType.DETAIL_RESPONSE);
    }

    public static QueryResponse queryResponseColumnInfo(QueryResponse.QueryReport.ColumnInfoList columnInfoList) {
        final QueryResponse.QueryReport columnInfoReport = QueryResponse.QueryReport.newBuilder()
                .setColumnInfoList(columnInfoList)
                .build();
        return responseWithReport(columnInfoReport, ResponseType.DETAIL_RESPONSE);
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

    public static void sendQueryResponseColumnInfo(
            QueryResponse.QueryReport.ColumnInfoList columnInfoList,
            StreamObserver<QueryResponse> responseObserver
    ) {
        final QueryResponse columnInfoResponse  = queryResponseColumnInfo(columnInfoList);
        responseObserver.onNext(columnInfoResponse);
        responseObserver.onCompleted();
    }

    protected QueryRequest.QuerySpec validateRequest(
            String requestType, QueryRequest request, StreamObserver responseObserver) {

        // check that query request contains a QuerySpec
        if (!request.hasQuerySpec()) {
            String errorMsg = "QueryRequest does not contain a QuerySpec";
            sendQueryResponseReject(errorMsg, RejectDetails.RejectReason.INVALID_REQUEST_REASON, responseObserver);
            return null;
        }

        QueryRequest.QuerySpec querySpec = request.getQuerySpec();

        logger.debug("id: {} query request: {} received columnNames: {} startSeconds: {} endSeconds: {}",
                responseObserver.hashCode(),
                requestType,
                querySpec.getColumnNamesList(),
                querySpec.getStartTime().getEpochSeconds(),
                querySpec.getEndTime().getEpochSeconds());

        // validate request
        ValidationResult validationResult = handler.validateQuerySpec(querySpec);

        // send reject if request is invalid
        if (validationResult.isError) {
            String validationMsg = validationResult.msg;
            sendQueryResponseReject(validationMsg, RejectDetails.RejectReason.INVALID_REQUEST_REASON, responseObserver);
            return null;
        }

        return querySpec;
    }

    @Override
    public void queryResponseStream(QueryRequest request, StreamObserver<QueryResponse> responseObserver) {

        // log and validate request
        QueryRequest.QuerySpec querySpec = validateRequest(REQUEST_STREAM, request, responseObserver);

        // handle request
        if (querySpec != null) {
            handler.handleQueryResponseStream(querySpec, responseObserver);
        }
    }

    @Override
    public StreamObserver<QueryRequest> queryResponseCursor(StreamObserver<QueryResponse> responseObserver) {
        logger.trace("queryResponseCursor");
        return new QueryResponseCursorRequestStreamObserver(responseObserver, handler, this);
    }

    @Override
    public void queryResponseSingle(QueryRequest request, StreamObserver<QueryResponse> responseObserver) {

        // log and validate request
        QueryRequest.QuerySpec querySpec = validateRequest(REQUEST_SINGLE, request, responseObserver);

        // handle request
        if (querySpec != null) {
            handler.handleQueryResponseSingle(querySpec, responseObserver);
        }
    }

    @Override
    public void queryResponseTable(QueryRequest request, StreamObserver<QueryResponse> responseObserver) {

        // log and validate request
        QueryRequest.QuerySpec querySpec = validateRequest(REQUEST_TABLE, request, responseObserver);

        // handle request
        if (querySpec != null) {
            handler.handleQueryResponseTable(querySpec, responseObserver);
        }
    }

    @Override
    public void getColumnInfo(QueryRequest request, StreamObserver<QueryResponse> responseObserver) {

        // check that query request contains a ColumnInfoQuerySpec
        if (!request.hasColumnInfoQuerySpec()) {
            String errorMsg = "QueryRequest does not contain a ColumnInfoQuerySpec";
            sendQueryResponseReject(errorMsg, RejectDetails.RejectReason.INVALID_REQUEST_REASON, responseObserver);
            return;
        }

        logger.debug("id: {} column info request received", responseObserver.hashCode());

        QueryRequest.ColumnInfoQuerySpec columnInfoQuerySpec = request.getColumnInfoQuerySpec();

        // validate query spec
        boolean isError = false;
        if (columnInfoQuerySpec.hasColumnNameList()) {
            if (columnInfoQuerySpec.getColumnNameList().getColumnNamesCount() == 0) {
                String errorMsg = "column name list must not be empty";
                sendQueryResponseReject(errorMsg, RejectDetails.RejectReason.INVALID_REQUEST_REASON, responseObserver);
                return;
            }
        } else if (columnInfoQuerySpec.hasColumnNamePattern()) {
            if (columnInfoQuerySpec.getColumnNamePattern().getPattern().isBlank()) {
                String errorMsg = "column name pattern must not be empty";
                sendQueryResponseReject(errorMsg, RejectDetails.RejectReason.INVALID_REQUEST_REASON, responseObserver);
                return;
            }
        } else {
            String errorMsg = "column info query must specify either list of column names or column name pattern";
            sendQueryResponseReject(errorMsg, RejectDetails.RejectReason.INVALID_REQUEST_REASON, responseObserver);
            return;
        }

        handler.handleGetColumnInfo(columnInfoQuerySpec, responseObserver);
    }

}
