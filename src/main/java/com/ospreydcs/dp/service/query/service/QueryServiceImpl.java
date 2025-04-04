package com.ospreydcs.dp.service.query.service;

import com.ospreydcs.dp.grpc.v1.common.ExceptionalResult;
import com.ospreydcs.dp.grpc.v1.query.*;
import com.ospreydcs.dp.service.common.protobuf.TimestampUtility;
import com.ospreydcs.dp.service.common.model.ValidationResult;
import com.ospreydcs.dp.service.query.handler.interfaces.QueryHandlerInterface;
import io.grpc.stub.StreamObserver;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;

public class QueryServiceImpl extends DpQueryServiceGrpc.DpQueryServiceImplBase {

    // static variables
    private static final Logger logger = LogManager.getLogger();

    // instance variables
    private QueryHandlerInterface handler;

    // constants
    protected static final String REQUEST_STREAM = "queryDataStream";
    protected static final String REQUEST_BIDI_STREAM = "queryDataBidiStream";
    protected static final String REQUEST_UNARY = "queryData";

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
                .setResponseTime(TimestampUtility.getTimestampNow())
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

    public static QueryDataResponse queryDataResponseNotReady() {
        return queryDataResponseExceptionalResult(
                "cursor not ready for operation", ExceptionalResult.ExceptionalResultStatus.RESULT_STATUS_NOT_READY);
    }

    public static QueryDataResponse queryDataResponse(QueryDataResponse.QueryData.Builder queryDataBuilder) {
        queryDataBuilder.build();
        return QueryDataResponse.newBuilder()
                .setResponseTime(TimestampUtility.getTimestampNow())
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

    public static void sendQueryDataResponse(
            QueryDataResponse.QueryData.Builder queryDataBuilder,
            StreamObserver<QueryDataResponse> responseObserver
    ) {
        final QueryDataResponse response = queryDataResponse(queryDataBuilder);
        responseObserver.onNext(response);
    }

    private static QueryTableResponse queryTableResponseExceptionalResult(
            String msg, ExceptionalResult.ExceptionalResultStatus status
    ) {
        final ExceptionalResult exceptionalResult = ExceptionalResult.newBuilder()
                .setExceptionalResultStatus(status)
                .setMessage(msg)
                .build();

        final QueryTableResponse response = QueryTableResponse.newBuilder()
                .setResponseTime(TimestampUtility.getTimestampNow())
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
                .setResponseTime(TimestampUtility.getTimestampNow())
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

    private static QueryPvMetadataResponse QueryPvMetadataResponseExceptionalResult(
            String msg,
            ExceptionalResult.ExceptionalResultStatus status
    ) {
        final ExceptionalResult exceptionalResult = ExceptionalResult.newBuilder()
                .setExceptionalResultStatus(status)
                .setMessage(msg)
                .build();

        final QueryPvMetadataResponse response = QueryPvMetadataResponse.newBuilder()
                .setResponseTime(TimestampUtility.getTimestampNow())
                .setExceptionalResult(exceptionalResult)
                .build();

        return response;
    }

    public static QueryPvMetadataResponse QueryPvMetadataResponseReject(String msg) {
        return QueryPvMetadataResponseExceptionalResult(
                msg, ExceptionalResult.ExceptionalResultStatus.RESULT_STATUS_REJECT);
    }

    public static QueryPvMetadataResponse QueryPvMetadataResponseError(String msg) {
        return QueryPvMetadataResponseExceptionalResult(
                msg, ExceptionalResult.ExceptionalResultStatus.RESULT_STATUS_ERROR);
    }

    public static QueryPvMetadataResponse QueryPvMetadataResponseEmpty() {
        return QueryPvMetadataResponseExceptionalResult(
                "query returned no data", ExceptionalResult.ExceptionalResultStatus.RESULT_STATUS_EMPTY);
    }

    public static QueryPvMetadataResponse QueryPvMetadataResponse(
            QueryPvMetadataResponse.MetadataResult metadataResult
    ) {
        return QueryPvMetadataResponse.newBuilder()
                .setResponseTime(TimestampUtility.getTimestampNow())
                .setMetadataResult(metadataResult)
                .build();
    }

    public static void sendQueryPvMetadataResponseReject(
            String msg, StreamObserver<QueryPvMetadataResponse> responseObserver) {

        final QueryPvMetadataResponse response = QueryPvMetadataResponseReject(msg);
        responseObserver.onNext(response);
        responseObserver.onCompleted();
    }

    public static void sendQueryPvMetadataResponseError(
            String msg, StreamObserver<QueryPvMetadataResponse> responseObserver
    ) {
        final QueryPvMetadataResponse response = QueryPvMetadataResponseError(msg);
        responseObserver.onNext(response);
        responseObserver.onCompleted();
    }

    public static void sendQueryPvMetadataResponseEmpty(StreamObserver<QueryPvMetadataResponse> responseObserver) {
        final QueryPvMetadataResponse response = QueryPvMetadataResponseEmpty();
        responseObserver.onNext(response);
        responseObserver.onCompleted();
    }

    public static void sendQueryPvMetadataResponse(
            QueryPvMetadataResponse.MetadataResult metadataResult,
            StreamObserver<QueryPvMetadataResponse> responseObserver
    ) {
        final QueryPvMetadataResponse response  = QueryPvMetadataResponse(metadataResult);
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
        return new QueryDataBidiStreamRequestObserver(responseObserver, handler, this);
    }

    @Override
    public void queryData(QueryDataRequest request, StreamObserver<QueryDataResponse> responseObserver) {

        // log and validate request
        QueryDataRequest.QuerySpec querySpec = validateQueryDataRequest(REQUEST_UNARY, request, responseObserver);

        // handle request
        if (querySpec != null) {
            handler.handleQueryData(querySpec, responseObserver);
        }
    }

    @Override
    public void queryTable(QueryTableRequest request, StreamObserver<QueryTableResponse> responseObserver) {

        logger.debug("queryTable request received id: {}",
                responseObserver.hashCode());

        // validate request
        ValidationResult validationResult = handler.validateQueryTableRequest(request);

        // send reject if request is invalid
        if (validationResult.isError) {
            String validationMsg = validationResult.msg;
            sendQueryTableResponseReject(validationMsg, responseObserver);
        }

        handler.handleQueryTable(request, responseObserver);
    }

    @Override
    public void queryPvMetadata(
            QueryPvMetadataRequest request,
            StreamObserver<QueryPvMetadataResponse> responseObserver
    ) {
        logger.debug("id: {} queryPvMetadata request received", responseObserver.hashCode());

        // validate query
        if (request.hasPvNameList()) {
            if (request.getPvNameList().getPvNamesCount() == 0) {
                final String errorMsg = "QueryPvMetadataRequest.pvNameList.pvNames must not be empty";
                sendQueryPvMetadataResponseReject(errorMsg, responseObserver);
                return;
            }

        } else if (request.hasPvNamePattern()) {
            if (request.getPvNamePattern().getPattern().isBlank()) {
                final String errorMsg = "QueryPvMetadataRequest.pvNamePattern.pattern must not be empty";
                sendQueryPvMetadataResponseReject(errorMsg, responseObserver);
                return;
            }
        } else {
            final String errorMsg = "QueryPvMetadataRequest must specify either pvNameList or pvNamePattern";
            sendQueryPvMetadataResponseReject(errorMsg, responseObserver);
            return;
        }

        handler.handleQueryPvMetadata(request, responseObserver);
    }

    private static QueryProvidersResponse queryProvidersResponseExceptionalResult(
            String msg,
            ExceptionalResult.ExceptionalResultStatus status
    ) {
        final ExceptionalResult exceptionalResult = ExceptionalResult.newBuilder()
                .setExceptionalResultStatus(status)
                .setMessage(msg)
                .build();

        final QueryProvidersResponse response = QueryProvidersResponse.newBuilder()
                .setResponseTime(TimestampUtility.getTimestampNow())
                .setExceptionalResult(exceptionalResult)
                .build();

        return response;
    }

    public static QueryProvidersResponse queryProvidersResponseReject(String msg) {
        return queryProvidersResponseExceptionalResult(
                msg, ExceptionalResult.ExceptionalResultStatus.RESULT_STATUS_REJECT);
    }

    public static QueryProvidersResponse queryProvidersResponseError(String msg) {
        return queryProvidersResponseExceptionalResult(
                msg, ExceptionalResult.ExceptionalResultStatus.RESULT_STATUS_ERROR);
    }

    public static QueryProvidersResponse queryProvidersResponseEmpty() {
        return queryProvidersResponseExceptionalResult(
                "query returned no data", ExceptionalResult.ExceptionalResultStatus.RESULT_STATUS_EMPTY);
    }

    public static QueryProvidersResponse queryProvidersResponse(
            QueryProvidersResponse.ProvidersResult providersResult
    ) {
        return QueryProvidersResponse.newBuilder()
                .setResponseTime(TimestampUtility.getTimestampNow())
                .setProvidersResult(providersResult)
                .build();
    }

    public static void sendQueryProvidersResponseReject(
            String msg, StreamObserver<QueryProvidersResponse> responseObserver) {

        final QueryProvidersResponse response = queryProvidersResponseReject(msg);
        responseObserver.onNext(response);
        responseObserver.onCompleted();
    }

    public static void sendQueryProvidersResponseError(
            String msg, StreamObserver<QueryProvidersResponse> responseObserver
    ) {
        final QueryProvidersResponse response = queryProvidersResponseError(msg);
        responseObserver.onNext(response);
        responseObserver.onCompleted();
    }

    public static void sendQueryProvidersResponseEmpty(StreamObserver<QueryProvidersResponse> responseObserver) {
        final QueryProvidersResponse response = queryProvidersResponseEmpty();
        responseObserver.onNext(response);
        responseObserver.onCompleted();
    }

    public static void sendQueryProvidersResponse(
            QueryProvidersResponse.ProvidersResult providersResult,
            StreamObserver<QueryProvidersResponse> responseObserver
    ) {
        final QueryProvidersResponse response  = queryProvidersResponse(providersResult);
        responseObserver.onNext(response);
        responseObserver.onCompleted();
    }

    @Override
    public void queryProviders(QueryProvidersRequest request, StreamObserver<QueryProvidersResponse> responseObserver) {
        logger.info("id: {} queryProviders request received", responseObserver.hashCode());

        // check that request contains non-empty list of criteria
        final List<QueryProvidersRequest.Criterion> criterionList = request.getCriteriaList();
        if (criterionList.size() == 0) {
            final String errorMsg = "QueryProvidersRequest.criteria list must not be empty";
            sendQueryProvidersResponseReject(errorMsg, responseObserver);
        }

        // validate query criteria
        for (QueryProvidersRequest.Criterion criterion: criterionList) {

            switch (criterion.getCriterionCase()) {

                case IDCRITERION -> {
                    final QueryProvidersRequest.Criterion.IdCriterion idCriterion
                            = criterion.getIdCriterion();
                    if (idCriterion.getId().isBlank()) {
                        final String errorMsg =
                                "QueryProvidersRequest.criteria.IdCriterion id must be specified";
                        sendQueryProvidersResponseReject(errorMsg, responseObserver);
                        return;
                    }
                }

                case TEXTCRITERION -> {
                    final QueryProvidersRequest.Criterion.TextCriterion commentCriterion
                            = criterion.getTextCriterion();
                    if (commentCriterion.getText().isBlank()) {
                        final String errorMsg =
                                "QueryProvidersRequest.criteria.TextCriterion text must be specified";
                        sendQueryProvidersResponseReject(errorMsg, responseObserver);
                        return;
                    }
                }

                case TAGSCRITERION -> {
                    final QueryProvidersRequest.Criterion.TagsCriterion tagsCriterion
                            = criterion.getTagsCriterion();
                    if (tagsCriterion.getTagValue().isBlank()) {
                        final String errorMsg =
                                "QueryProvidersRequest.criteria.TagsCriterion tagValue must be specified";
                        sendQueryProvidersResponseReject(errorMsg, responseObserver);
                        return;
                    }
                }

                case ATTRIBUTESCRITERION -> {
                    final QueryProvidersRequest.Criterion.AttributesCriterion attributesCriterion
                            = criterion.getAttributesCriterion();
                    if (attributesCriterion.getKey().isBlank()) {
                        final String errorMsg =
                                "QueryProvidersRequest.criteria.AttributesCriterion key must be specified";
                        sendQueryProvidersResponseReject(errorMsg, responseObserver);
                        return;
                    }
                    if (attributesCriterion.getValue().isBlank()) {
                        final String errorMsg =
                                "QueryProvidersRequest.criteria.AttributesCriterion value must be specified";
                        sendQueryProvidersResponseReject(errorMsg, responseObserver);
                        return;
                    }
                }

                case CRITERION_NOT_SET -> {
                    final String errorMsg =
                            "QueryProvidersRequest.criteria criterion case not set";
                    sendQueryProvidersResponseReject(errorMsg, responseObserver);
                    return;
                }
            }
        }

        handler.handleQueryProviders(request, responseObserver);
    }

    private static QueryProviderMetadataResponse queryProviderMetadataResponseExceptionalResult(
            String msg,
            ExceptionalResult.ExceptionalResultStatus status
    ) {
        final ExceptionalResult exceptionalResult = ExceptionalResult.newBuilder()
                .setExceptionalResultStatus(status)
                .setMessage(msg)
                .build();

        final QueryProviderMetadataResponse response = QueryProviderMetadataResponse.newBuilder()
                .setResponseTime(TimestampUtility.getTimestampNow())
                .setExceptionalResult(exceptionalResult)
                .build();

        return response;
    }

    public static QueryProviderMetadataResponse queryProviderMetadataResponseReject(String msg) {
        return queryProviderMetadataResponseExceptionalResult(
                msg, ExceptionalResult.ExceptionalResultStatus.RESULT_STATUS_REJECT);
    }

    public static QueryProviderMetadataResponse queryProviderMetadataResponseError(String msg) {
        return queryProviderMetadataResponseExceptionalResult(
                msg, ExceptionalResult.ExceptionalResultStatus.RESULT_STATUS_ERROR);
    }

    public static QueryProviderMetadataResponse queryProviderMetadataResponseEmpty() {
        return queryProviderMetadataResponseExceptionalResult(
                "query returned no data", ExceptionalResult.ExceptionalResultStatus.RESULT_STATUS_EMPTY);
    }

    public static QueryProviderMetadataResponse queryProviderMetadataResponse(
            QueryProviderMetadataResponse.MetadataResult metadataResult
    ) {
        return QueryProviderMetadataResponse.newBuilder()
                .setResponseTime(TimestampUtility.getTimestampNow())
                .setMetadataResult(metadataResult)
                .build();
    }

    public static void sendQueryProviderMetadataResponseReject(
            String msg, StreamObserver<QueryProviderMetadataResponse> responseObserver) {

        final QueryProviderMetadataResponse response = queryProviderMetadataResponseReject(msg);
        responseObserver.onNext(response);
        responseObserver.onCompleted();
    }

    public static void sendQueryProviderMetadataResponseError(
            String msg, StreamObserver<QueryProviderMetadataResponse> responseObserver
    ) {
        final QueryProviderMetadataResponse response = queryProviderMetadataResponseError(msg);
        responseObserver.onNext(response);
        responseObserver.onCompleted();
    }

    public static void sendQueryProviderMetadataResponseEmpty(StreamObserver<QueryProviderMetadataResponse> responseObserver) {
        final QueryProviderMetadataResponse response = queryProviderMetadataResponseEmpty();
        responseObserver.onNext(response);
        responseObserver.onCompleted();
    }

    public static void sendQueryProviderMetadataResponse(
            QueryProviderMetadataResponse.MetadataResult metadataResult,
            StreamObserver<QueryProviderMetadataResponse> responseObserver
    ) {
        final QueryProviderMetadataResponse response  = queryProviderMetadataResponse(metadataResult);
        responseObserver.onNext(response);
        responseObserver.onCompleted();
    }
    
    @Override
    public void queryProviderMetadata(
            QueryProviderMetadataRequest request, StreamObserver<QueryProviderMetadataResponse> responseObserver
    ) {
        // check that request contains non-empty providerId
        if (request.getProviderId().isBlank()) {
            final String errorMsg = "QueryProviderMetadataRequest.providerId must be specified";
            sendQueryProviderMetadataResponseReject(errorMsg, responseObserver);
            return;
        }

        handler.handleQueryProviderMetadata(request, responseObserver);
    }

}
