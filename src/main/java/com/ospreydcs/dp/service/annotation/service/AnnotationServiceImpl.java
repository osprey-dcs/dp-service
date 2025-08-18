package com.ospreydcs.dp.service.annotation.service;

import com.ospreydcs.dp.grpc.v1.annotation.*;
import com.ospreydcs.dp.grpc.v1.common.ExceptionalResult;
import com.ospreydcs.dp.service.annotation.handler.AnnotationValidationUtility;
import com.ospreydcs.dp.service.annotation.handler.interfaces.AnnotationHandlerInterface;
import com.ospreydcs.dp.service.annotation.handler.model.HandlerExportDataRequest;
import com.ospreydcs.dp.service.common.model.ResultStatus;
import com.ospreydcs.dp.service.common.protobuf.TimestampUtility;
import io.grpc.stub.StreamObserver;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;

public class AnnotationServiceImpl extends DpAnnotationServiceGrpc.DpAnnotationServiceImplBase {

    // static variables
    private static final Logger logger = LogManager.getLogger();

    // instance variables
    private AnnotationHandlerInterface handler;

    public boolean init(AnnotationHandlerInterface handler) {
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

    private static SaveDataSetResponse saveDataSetResponseReject(String msg) {

        final ExceptionalResult exceptionalResult =
                ExceptionalResult.newBuilder()
                        .setExceptionalResultStatus(
                                ExceptionalResult.ExceptionalResultStatus.RESULT_STATUS_REJECT)
                        .setMessage(msg)
                        .build();

        final SaveDataSetResponse response = SaveDataSetResponse.newBuilder()
                .setResponseTime(TimestampUtility.getTimestampNow())
                .setExceptionalResult(exceptionalResult)
                .build();

        return response;
    }

    public static void sendSaveDataSetResponseReject(
            String errorMsg,
            StreamObserver<SaveDataSetResponse> responseObserver
    ) {
        final SaveDataSetResponse response = saveDataSetResponseReject(errorMsg);
        responseObserver.onNext(response);
        responseObserver.onCompleted();
    }

    private static SaveDataSetResponse saveDataSetResponseError(String msg) {

        final ExceptionalResult exceptionalResult =
                ExceptionalResult.newBuilder()
                        .setExceptionalResultStatus(ExceptionalResult.ExceptionalResultStatus.RESULT_STATUS_ERROR)
                        .setMessage(msg)
                        .build();

        final SaveDataSetResponse response = SaveDataSetResponse.newBuilder()
                .setResponseTime(TimestampUtility.getTimestampNow())
                .setExceptionalResult(exceptionalResult)
                .build();

        return response;
    }

    public static void sendSaveDataSetResponseError(
            String errorMsg, StreamObserver<SaveDataSetResponse> responseObserver
    ) {
        final SaveDataSetResponse response = saveDataSetResponseError(errorMsg);
        responseObserver.onNext(response);
        responseObserver.onCompleted();
    }

    private static SaveDataSetResponse saveDataSetResponseSuccess(String dataSetId) {

        final SaveDataSetResponse.SaveDataSetResult result =
                SaveDataSetResponse.SaveDataSetResult.newBuilder()
                        .setDataSetId(dataSetId)
                        .build();

        final SaveDataSetResponse response = SaveDataSetResponse.newBuilder()
                .setResponseTime(TimestampUtility.getTimestampNow())
                .setCreateDataSetResult(result)
                .build();

        return response;
    }

    public static void sendSaveDataSetResponseSuccess(
            String dataSetId, StreamObserver<SaveDataSetResponse> responseObserver
    ) {
        final SaveDataSetResponse response = saveDataSetResponseSuccess(dataSetId);
        responseObserver.onNext(response);
        responseObserver.onCompleted();
    }

    @Override
    public void saveDataSet(
            SaveDataSetRequest request,
            StreamObserver<SaveDataSetResponse> responseObserver
    ) {
        logger.info("id: {} saveDataSet request received", responseObserver.hashCode());

        final DataSet dataSet = request.getDataSet();
        if (dataSet == null) {
            final String errorMsg = "SaveDataSetRequest.dataSet must be specified";
            sendSaveDataSetResponseReject(errorMsg, responseObserver);
        }

        // validate DataSet
        ResultStatus resultStatus = AnnotationValidationUtility.validateDataSet(dataSet);
        if (resultStatus.isError) {
            logger.debug("id: {} SaveDataSetRequest.dataSet validation failed: {}",
                    responseObserver.hashCode(),
                    resultStatus.msg);
            sendSaveDataSetResponseReject(
                    resultStatus.msg,
                    responseObserver);
            return;
        }

        // handle request
        handler.handleSaveDataSet(request, responseObserver);
    }

    private static QueryDataSetsResponse queryDataSetsResponseExceptionalResult(
            String msg,
            ExceptionalResult.ExceptionalResultStatus status
    ) {
        final ExceptionalResult exceptionalResult = ExceptionalResult.newBuilder()
                .setExceptionalResultStatus(status)
                .setMessage(msg)
                .build();

        final QueryDataSetsResponse response = QueryDataSetsResponse.newBuilder()
                .setResponseTime(TimestampUtility.getTimestampNow())
                .setExceptionalResult(exceptionalResult)
                .build();

        return response;
    }

    public static QueryDataSetsResponse queryDataSetsResponseReject(String msg) {
        return queryDataSetsResponseExceptionalResult(
                msg, ExceptionalResult.ExceptionalResultStatus.RESULT_STATUS_REJECT);
    }

    public static QueryDataSetsResponse queryDataSetsResponseError(String msg) {
        return queryDataSetsResponseExceptionalResult(
                msg, ExceptionalResult.ExceptionalResultStatus.RESULT_STATUS_ERROR);
    }

    public static QueryDataSetsResponse queryDataSetsResponse(
            QueryDataSetsResponse.DataSetsResult dataSetsResult
    ) {
        return QueryDataSetsResponse.newBuilder()
                .setResponseTime(TimestampUtility.getTimestampNow())
                .setDataSetsResult(dataSetsResult)
                .build();
    }

    public static void sendQueryDataSetsResponseReject(
            String msg, StreamObserver<QueryDataSetsResponse> responseObserver) {

        final QueryDataSetsResponse response = queryDataSetsResponseReject(msg);
        responseObserver.onNext(response);
        responseObserver.onCompleted();
    }

    public static void sendQueryDataSetsResponseError(
            String msg, StreamObserver<QueryDataSetsResponse> responseObserver
    ) {
        final QueryDataSetsResponse response = queryDataSetsResponseError(msg);
        responseObserver.onNext(response);
        responseObserver.onCompleted();
    }

    public static void sendQueryDataSetsResponse(
            QueryDataSetsResponse.DataSetsResult dataSetsResult,
            StreamObserver<QueryDataSetsResponse> responseObserver
    ) {
        final QueryDataSetsResponse response  = queryDataSetsResponse(dataSetsResult);
        responseObserver.onNext(response);
        responseObserver.onCompleted();
    }

    @Override
    public void queryDataSets(
            QueryDataSetsRequest request,
            StreamObserver<QueryDataSetsResponse> responseObserver
    ) {
        logger.info("id: {} queryDataSets request received", responseObserver.hashCode());

        // check that request contains non-empty list of criteria
        final List<QueryDataSetsRequest.QueryDataSetsCriterion> criterionList = request.getCriteriaList();
        if (criterionList.size() == 0) {
            final String errorMsg = "QueryDataSetsRequest.criteria list must not be empty";
            sendQueryDataSetsResponseReject(errorMsg, responseObserver);
        }

        // validate query criteria
        for (QueryDataSetsRequest.QueryDataSetsCriterion criterion : criterionList) {

            switch (criterion.getCriterionCase()) {

                case IDCRITERION -> {
                    final QueryDataSetsRequest.QueryDataSetsCriterion.IdCriterion idCriterion
                            = criterion.getIdCriterion();
                    if (idCriterion.getId().isBlank()) {
                        final String errorMsg =
                                "QueryDataSetsRequest.criteria.IdCriterion id must be specified";
                        sendQueryDataSetsResponseReject(errorMsg, responseObserver);
                        return;
                    }
                }

                case OWNERCRITERION -> {
                    final QueryDataSetsRequest.QueryDataSetsCriterion.OwnerCriterion ownerCriterion
                            = criterion.getOwnerCriterion();
                    if (ownerCriterion.getOwnerId().isBlank()) {
                        final String errorMsg =
                                "QueryDataSetsRequest.criteria.OwnerCriterion ownerId must be specified";
                        sendQueryDataSetsResponseReject(errorMsg, responseObserver);
                        return;
                    }
                }

                case TEXTCRITERION -> {
                    final QueryDataSetsRequest.QueryDataSetsCriterion.TextCriterion textCriterion
                            = criterion.getTextCriterion();
                    if (textCriterion.getText().isBlank()) {
                        final String errorMsg =
                                "QueryDataSetsRequest.criteria.TextCriterion text must be specified";
                        sendQueryDataSetsResponseReject(errorMsg, responseObserver);
                        return;
                    }
                }

                case PVNAMECRITERION -> {
                    final QueryDataSetsRequest.QueryDataSetsCriterion.PvNameCriterion pvNameCriterion
                            = criterion.getPvNameCriterion();
                    if (pvNameCriterion.getName().isBlank()) {
                        final String errorMsg =
                                "QueryDataSetsRequest.criteria.PvNameCriterion name must be specified";
                        sendQueryDataSetsResponseReject(errorMsg, responseObserver);
                        return;
                    }
                }

                case CRITERION_NOT_SET -> {
                    final String errorMsg =
                            "QueryDataSetsRequest.criteria criterion case not set";
                    sendQueryDataSetsResponseReject(errorMsg, responseObserver);
                    return;
                }
            }
        }

        handler.handleQueryDataSets(request, responseObserver);
    }

    private static CreateAnnotationResponse createAnnotationResponseReject(String msg) {
        final ExceptionalResult exceptionalResult =
                ExceptionalResult.newBuilder()
                        .setExceptionalResultStatus(
                                ExceptionalResult.ExceptionalResultStatus.RESULT_STATUS_REJECT)
                        .setMessage(msg)
                        .build();

        final CreateAnnotationResponse response = CreateAnnotationResponse.newBuilder()
                .setResponseTime(TimestampUtility.getTimestampNow())
                .setExceptionalResult(exceptionalResult)
                .build();

        return response;
    }

    public static void sendCreateAnnotationResponseReject(
            String errorMsg,
            StreamObserver<CreateAnnotationResponse> responseObserver
    ) {
        final CreateAnnotationResponse response = createAnnotationResponseReject(errorMsg);
        responseObserver.onNext(response);
        responseObserver.onCompleted();
    }

    private static CreateAnnotationResponse createAnnotationResponseError(String msg) {

        final ExceptionalResult exceptionalResult =
                ExceptionalResult.newBuilder()
                        .setExceptionalResultStatus(ExceptionalResult.ExceptionalResultStatus.RESULT_STATUS_ERROR)
                        .setMessage(msg)
                        .build();

        final CreateAnnotationResponse response = CreateAnnotationResponse.newBuilder()
                .setResponseTime(TimestampUtility.getTimestampNow())
                .setExceptionalResult(exceptionalResult)
                .build();

        return response;
    }

    public static void sendCreateAnnotationResponseError(
            String errorMsg, StreamObserver<CreateAnnotationResponse> responseObserver
    ) {
        final CreateAnnotationResponse response = createAnnotationResponseError(errorMsg);
        responseObserver.onNext(response);
        responseObserver.onCompleted();
    }

    private static CreateAnnotationResponse createAnnotationResponseSuccess(String annotationId) {

        final CreateAnnotationResponse.CreateAnnotationResult result =
                CreateAnnotationResponse.CreateAnnotationResult.newBuilder()
                        .setAnnotationId(annotationId)
                        .build();

        final CreateAnnotationResponse response = CreateAnnotationResponse.newBuilder()
                .setResponseTime(TimestampUtility.getTimestampNow())
                .setCreateAnnotationResult(result)
                .build();

        return response;
    }

    public static void sendCreateAnnotationResponseSuccess(
            String annotationId, StreamObserver<CreateAnnotationResponse> responseObserver) {
        final CreateAnnotationResponse response = createAnnotationResponseSuccess(annotationId);
        responseObserver.onNext(response);
        responseObserver.onCompleted();
    }

    @Override
    public void createAnnotation(
            CreateAnnotationRequest request,
            StreamObserver<CreateAnnotationResponse> responseObserver
    ) {
        logger.info(
                "id: {} createAnnotation request received with name: {}",
                responseObserver.hashCode(),
                request.getName());

        // perform validation of base annotation details
        // validate common annotation details
        final ResultStatus resultStatus =
                AnnotationValidationUtility.validateCreateAnnotationRequest(request);
        if (resultStatus.isError) {
            logger.debug(
                    "id: {} createAnnotation validation failed: ",
                    responseObserver.hashCode(),
                    resultStatus.msg);
            sendCreateAnnotationResponseReject(
                    resultStatus.msg,
                    responseObserver);
            return;
        }

        // handle request
        handler.handleCreateAnnotation(request, responseObserver);
    }

    private static QueryAnnotationsResponse queryAnnotationsResponseExceptionalResult(
            String msg,
            ExceptionalResult.ExceptionalResultStatus status
    ) {
        final ExceptionalResult exceptionalResult = ExceptionalResult.newBuilder()
                .setExceptionalResultStatus(status)
                .setMessage(msg)
                .build();

        final QueryAnnotationsResponse response = QueryAnnotationsResponse.newBuilder()
                .setResponseTime(TimestampUtility.getTimestampNow())
                .setExceptionalResult(exceptionalResult)
                .build();

        return response;
    }

    public static QueryAnnotationsResponse queryAnnotationsResponseReject(String msg) {
        return queryAnnotationsResponseExceptionalResult(
                msg, ExceptionalResult.ExceptionalResultStatus.RESULT_STATUS_REJECT);
    }

    public static QueryAnnotationsResponse queryAnnotationsResponseError(String msg) {
        return queryAnnotationsResponseExceptionalResult(
                msg, ExceptionalResult.ExceptionalResultStatus.RESULT_STATUS_ERROR);
    }

    public static QueryAnnotationsResponse queryAnnotationsResponse(
            QueryAnnotationsResponse.AnnotationsResult annotationsResult
    ) {
        return QueryAnnotationsResponse.newBuilder()
                .setResponseTime(TimestampUtility.getTimestampNow())
                .setAnnotationsResult(annotationsResult)
                .build();
    }

    public static void sendQueryAnnotationsResponseReject(
            String msg, StreamObserver<QueryAnnotationsResponse> responseObserver) {

        final QueryAnnotationsResponse response = queryAnnotationsResponseReject(msg);
        responseObserver.onNext(response);
        responseObserver.onCompleted();
    }

    public static void sendQueryAnnotationsResponseError(
            String msg, StreamObserver<QueryAnnotationsResponse> responseObserver
    ) {
        final QueryAnnotationsResponse response = queryAnnotationsResponseError(msg);
        responseObserver.onNext(response);
        responseObserver.onCompleted();
    }

    public static void sendQueryAnnotationsResponse(
            QueryAnnotationsResponse.AnnotationsResult annotationsResult,
            StreamObserver<QueryAnnotationsResponse> responseObserver
    ) {
        final QueryAnnotationsResponse response  = queryAnnotationsResponse(annotationsResult);
        responseObserver.onNext(response);
        responseObserver.onCompleted();
    }

    @Override
    public void queryAnnotations(
            QueryAnnotationsRequest request,
            StreamObserver<QueryAnnotationsResponse> responseObserver
    ) {
        logger.info("id: {} queryAnnotations request received", responseObserver.hashCode());

        // check that request contains non-empty list of criteria
        final List<QueryAnnotationsRequest.QueryAnnotationsCriterion> criterionList = request.getCriteriaList();
        if (criterionList.size() == 0) {
            final String errorMsg = "QueryAnnotationsRequest.criteria list must not be empty";
            sendQueryAnnotationsResponseReject(errorMsg, responseObserver);
        }

        // validate query criteria
        for (QueryAnnotationsRequest.QueryAnnotationsCriterion criterion : criterionList) {

            switch (criterion.getCriterionCase()) {

                case IDCRITERION -> {
                    final QueryAnnotationsRequest.QueryAnnotationsCriterion.IdCriterion idCriterion
                            = criterion.getIdCriterion();
                    if (idCriterion.getId().isBlank()) {
                        final String errorMsg =
                                "QueryAnnotationsRequest.criteria.IdCriterion id must be specified";
                        sendQueryAnnotationsResponseReject(errorMsg, responseObserver);
                        return;
                    }
                }

                case OWNERCRITERION -> {
                    final QueryAnnotationsRequest.QueryAnnotationsCriterion.OwnerCriterion ownerCriterion
                            = criterion.getOwnerCriterion();
                    if (ownerCriterion.getOwnerId().isBlank()) {
                        final String errorMsg =
                                "QueryAnnotationsRequest.criteria.OwnerCriterion ownerId must be specified";
                        sendQueryAnnotationsResponseReject(errorMsg, responseObserver);
                        return;
                    }
                }

                case DATASETSCRITERION -> {
                    final QueryAnnotationsRequest.QueryAnnotationsCriterion.DataSetsCriterion dataSetsCriterion
                            = criterion.getDataSetsCriterion();
                    if (dataSetsCriterion.getDataSetId().isBlank()) {
                        final String errorMsg =
                                "QueryAnnotationsRequest.criteria.DataSetCriterion dataSetId must be specified";
                        sendQueryAnnotationsResponseReject(errorMsg, responseObserver);
                        return;
                    }
                }

                case ANNOTATIONSCRITERION -> {
                    final QueryAnnotationsRequest.QueryAnnotationsCriterion.AnnotationsCriterion annotationsCriterion
                            = criterion.getAnnotationsCriterion();
                    if (annotationsCriterion.getAnnotationId().isBlank()) {
                        final String errorMsg =
                                "QueryAnnotationsRequest.criteria.AssociatedAnnotationIdCriterion id must be specified";
                        sendQueryAnnotationsResponseReject(errorMsg, responseObserver);
                        return;
                    }
                }

                case TEXTCRITERION -> {
                    final QueryAnnotationsRequest.QueryAnnotationsCriterion.TextCriterion commentCriterion
                            = criterion.getTextCriterion();
                    if (commentCriterion.getText().isBlank()) {
                        final String errorMsg =
                                "QueryAnnotationsRequest.criteria.TextCriterion text must be specified";
                        sendQueryAnnotationsResponseReject(errorMsg, responseObserver);
                        return;
                    }
                }

                case TAGSCRITERION -> {
                    final QueryAnnotationsRequest.QueryAnnotationsCriterion.TagsCriterion tagsCriterion
                            = criterion.getTagsCriterion();
                    if (tagsCriterion.getTagValue().isBlank()) {
                        final String errorMsg =
                                "QueryAnnotationsRequest.criteria.TagsCriterion tagValue must be specified";
                        sendQueryAnnotationsResponseReject(errorMsg, responseObserver);
                        return;
                    }
                }

                case ATTRIBUTESCRITERION -> {
                    final QueryAnnotationsRequest.QueryAnnotationsCriterion.AttributesCriterion attributesCriterion
                            = criterion.getAttributesCriterion();
                    if (attributesCriterion.getKey().isBlank()) {
                        final String errorMsg =
                                "QueryAnnotationsRequest.criteria.AttributesCriterion key must be specified";
                        sendQueryAnnotationsResponseReject(errorMsg, responseObserver);
                        return;
                    }
                    if (attributesCriterion.getValue().isBlank()) {
                        final String errorMsg =
                                "QueryAnnotationsRequest.criteria.AttributesCriterion value must be specified";
                        sendQueryAnnotationsResponseReject(errorMsg, responseObserver);
                        return;
                    }
                }

                case CRITERION_NOT_SET -> {
                    final String errorMsg =
                            "QueryAnnotationsRequest.criteria criterion case not set";
                    sendQueryAnnotationsResponseReject(errorMsg, responseObserver);
                    return;
                }
            }
        }

        handler.handleQueryAnnotations(request, responseObserver);
    }

    private static ExportDataResponse exportDataResponseReject(String msg) {

        final ExceptionalResult exceptionalResult =
                ExceptionalResult.newBuilder()
                        .setExceptionalResultStatus(
                                ExceptionalResult.ExceptionalResultStatus.RESULT_STATUS_REJECT)
                        .setMessage(msg)
                        .build();

        final ExportDataResponse response = ExportDataResponse.newBuilder()
                .setResponseTime(TimestampUtility.getTimestampNow())
                .setExceptionalResult(exceptionalResult)
                .build();

        return response;
    }

    public static void sendExportDataResponseReject(
            String errorMsg,
            StreamObserver<ExportDataResponse> responseObserver
    ) {
        final ExportDataResponse response = exportDataResponseReject(errorMsg);
        responseObserver.onNext(response);
        responseObserver.onCompleted();
    }

    private static ExportDataResponse exportDataResponseError(String msg) {

        final ExceptionalResult exceptionalResult =
                ExceptionalResult.newBuilder()
                        .setExceptionalResultStatus(ExceptionalResult.ExceptionalResultStatus.RESULT_STATUS_ERROR)
                        .setMessage(msg)
                        .build();

        final ExportDataResponse response = ExportDataResponse.newBuilder()
                .setResponseTime(TimestampUtility.getTimestampNow())
                .setExceptionalResult(exceptionalResult)
                .build();

        return response;
    }

    public static void sendExportDataResponseError(
            String errorMsg, StreamObserver<ExportDataResponse> responseObserver
    ) {
        final ExportDataResponse response = exportDataResponseError(errorMsg);
        responseObserver.onNext(response);
        responseObserver.onCompleted();
    }

    private static ExportDataResponse exportDataResponseSuccess(String filePath, String fileUrl) {

        final ExportDataResponse.ExportDataResult result =
                ExportDataResponse.ExportDataResult.newBuilder()
                        .setFilePath(filePath)
                        .setFileUrl(fileUrl == null ? "" : fileUrl)
                        .build();

        final ExportDataResponse response = ExportDataResponse.newBuilder()
                .setResponseTime(TimestampUtility.getTimestampNow())
                .setExportDataResult(result)
                .build();

        return response;
    }

    public static void sendExportDataResponseSuccess(
            String filePath, String fileUrl, StreamObserver<ExportDataResponse> responseObserver
    ) {
        final ExportDataResponse response = exportDataResponseSuccess(filePath, fileUrl);
        responseObserver.onNext(response);
        responseObserver.onCompleted();
    }
    
    @Override
    public void exportData(
            ExportDataRequest request,
            StreamObserver<ExportDataResponse> responseObserver
    ) {
        logger.info("id: {} exportData request received", responseObserver.hashCode());

        // validate request
        ResultStatus resultStatus = AnnotationValidationUtility.validateExportDataRequest(request);
        if (resultStatus.isError) {
            logger.debug("id: {} ExportDataRequest validation failed: {}",
                    responseObserver.hashCode(),
                    resultStatus.msg);
            sendExportDataResponseReject(
                    resultStatus.msg,
                    responseObserver);
            return;
        }

        // handle request
        HandlerExportDataRequest handlerRequest = new HandlerExportDataRequest(request, responseObserver);
        handler.handleExportData(handlerRequest);

    }
}
