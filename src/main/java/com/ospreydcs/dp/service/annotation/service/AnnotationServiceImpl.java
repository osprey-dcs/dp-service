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
import org.bson.types.ObjectId;

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
                .setSaveDataSetResult(result)
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

        // validate request
        ResultStatus resultStatus = AnnotationValidationUtility.validateSaveDataSetRequest(request);
        if (resultStatus.isError) {
            logger.debug("id: {} SaveDataSetRequest validation failed: {}",
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

    /**
     * True when any entry in a criterion value list is blank.  A blank entry must be rejected, not
     * silently dropped: the query filter builder treats validated lists as-is, and a blank entry
     * that survives to a prefix/contains regex or vanishes from an in() filter turns the criterion
     * into a silent match-all (#243) -- a wrong answer wearing the appearance of a filter.
     */
    private static boolean containsBlank(List<String> values) {
        return values.stream().anyMatch(String::isBlank);
    }

    /**
     * True when any entry is not a parseable ObjectId hex string.  An unvalidated id would throw
     * IllegalArgumentException from the ObjectId constructor inside the worker thread, where
     * QueueHandlerBase swallows it and the caller's response stream hangs until deadline.
     */
    private static boolean containsInvalidObjectId(List<String> ids) {
        return ids.stream().anyMatch(id -> !ObjectId.isValid(id));
    }

    @Override
    public void queryDataSets(
            QueryDataSetsRequest request,
            StreamObserver<QueryDataSetsResponse> responseObserver
    ) {
        logger.info("id: {} queryDataSets request received", responseObserver.hashCode());

        // An empty criteria list is match-all by contract, not an error, so there is deliberately
        // no list-level emptiness check here.  Per-criterion validation below is unaffected: a
        // criterion that IS supplied must still be well-formed.

        // validate query criteria
        for (QueryDataSetsRequest.QueryDataSetsCriterion criterion : request.getCriteriaList()) {

            switch (criterion.getCriterionCase()) {

                case IDCRITERION -> {
                    if (criterion.getIdCriterion().getIdsList().isEmpty()) {
                        final String errorMsg =
                                "QueryDataSetsRequest.criteria.IdCriterion must specify at least one id";
                        sendQueryDataSetsResponseReject(errorMsg, responseObserver);
                        return;
                    }
                    if (containsInvalidObjectId(criterion.getIdCriterion().getIdsList())) {
                        final String errorMsg =
                                "QueryDataSetsRequest.criteria.IdCriterion ids must be valid ObjectId hex strings";
                        sendQueryDataSetsResponseReject(errorMsg, responseObserver);
                        return;
                    }
                }

                case OWNERCRITERION -> {
                    if (criterion.getOwnerCriterion().getOwnerIdsList().isEmpty()) {
                        final String errorMsg =
                                "QueryDataSetsRequest.criteria.OwnerCriterion must specify at least one ownerId";
                        sendQueryDataSetsResponseReject(errorMsg, responseObserver);
                        return;
                    }
                    if (containsBlank(criterion.getOwnerCriterion().getOwnerIdsList())) {
                        final String errorMsg =
                                "QueryDataSetsRequest.criteria.OwnerCriterion ownerIds must not contain blank entries";
                        sendQueryDataSetsResponseReject(errorMsg, responseObserver);
                        return;
                    }
                }

                case NAMECRITERION -> {
                    final QueryDataSetsRequest.QueryDataSetsCriterion.NameCriterion nameCriterion =
                            criterion.getNameCriterion();
                    if (nameCriterion.getExactList().isEmpty()
                            && nameCriterion.getPrefixList().isEmpty()
                            && nameCriterion.getContainsList().isEmpty()) {
                        final String errorMsg =
                                "QueryDataSetsRequest.criteria.NameCriterion must specify at least one of: exact, prefix, contains";
                        sendQueryDataSetsResponseReject(errorMsg, responseObserver);
                        return;
                    }
                    // A blank prefix/contains entry would build a match-everything regex (#243), so
                    // blank entries in any of the three lists are rejected, never dropped.
                    if (containsBlank(nameCriterion.getExactList())
                            || containsBlank(nameCriterion.getPrefixList())
                            || containsBlank(nameCriterion.getContainsList())) {
                        final String errorMsg =
                                "QueryDataSetsRequest.criteria.NameCriterion entries must not be blank";
                        sendQueryDataSetsResponseReject(errorMsg, responseObserver);
                        return;
                    }
                }

                case TEXTCRITERION -> {
                    if (criterion.getTextCriterion().getText().isBlank()) {
                        final String errorMsg =
                                "QueryDataSetsRequest.criteria.TextCriterion text must be specified";
                        sendQueryDataSetsResponseReject(errorMsg, responseObserver);
                        return;
                    }
                }

                case PVNAMECRITERION -> {
                    if (criterion.getPvNameCriterion().getNamesList().isEmpty()) {
                        final String errorMsg =
                                "QueryDataSetsRequest.criteria.PvNameCriterion must specify at least one name";
                        sendQueryDataSetsResponseReject(errorMsg, responseObserver);
                        return;
                    }
                    if (containsBlank(criterion.getPvNameCriterion().getNamesList())) {
                        final String errorMsg =
                                "QueryDataSetsRequest.criteria.PvNameCriterion names must not contain blank entries";
                        sendQueryDataSetsResponseReject(errorMsg, responseObserver);
                        return;
                    }
                }

                case TAGSCRITERION -> {
                    if (criterion.getTagsCriterion().getValuesList().isEmpty()) {
                        final String errorMsg =
                                "QueryDataSetsRequest.criteria.TagsCriterion must specify at least one value";
                        sendQueryDataSetsResponseReject(errorMsg, responseObserver);
                        return;
                    }
                    if (containsBlank(criterion.getTagsCriterion().getValuesList())) {
                        final String errorMsg =
                                "QueryDataSetsRequest.criteria.TagsCriterion values must not contain blank entries";
                        sendQueryDataSetsResponseReject(errorMsg, responseObserver);
                        return;
                    }
                }

                case ATTRIBUTESCRITERION -> {
                    if (criterion.getAttributesCriterion().getKey().isBlank()) {
                        final String errorMsg =
                                "QueryDataSetsRequest.criteria.AttributesCriterion key must be specified";
                        sendQueryDataSetsResponseReject(errorMsg, responseObserver);
                        return;
                    }
                    // an empty values list is a legitimate key-existence search; blank entries are not
                    if (containsBlank(criterion.getAttributesCriterion().getValuesList())) {
                        final String errorMsg =
                                "QueryDataSetsRequest.criteria.AttributesCriterion values must not contain blank entries";
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

    // =========================================================
    // getDataSet
    // =========================================================

    private static GetDataSetResponse getDataSetResponseExceptionalResult(
            String msg, ExceptionalResult.ExceptionalResultStatus status) {
        final ExceptionalResult exceptionalResult = ExceptionalResult.newBuilder()
                .setExceptionalResultStatus(status)
                .setMessage(msg)
                .build();
        return GetDataSetResponse.newBuilder()
                .setResponseTime(TimestampUtility.getTimestampNow())
                .setExceptionalResult(exceptionalResult)
                .build();
    }

    public static void sendGetDataSetResponseReject(
            String msg, StreamObserver<GetDataSetResponse> responseObserver) {
        responseObserver.onNext(getDataSetResponseExceptionalResult(
                msg, ExceptionalResult.ExceptionalResultStatus.RESULT_STATUS_REJECT));
        responseObserver.onCompleted();
    }

    public static void sendGetDataSetResponseError(
            String msg, StreamObserver<GetDataSetResponse> responseObserver) {
        responseObserver.onNext(getDataSetResponseExceptionalResult(
                msg, ExceptionalResult.ExceptionalResultStatus.RESULT_STATUS_ERROR));
        responseObserver.onCompleted();
    }

    public static void sendGetDataSetResponseSuccess(
            DataSet dataSet,
            StreamObserver<GetDataSetResponse> responseObserver) {
        final GetDataSetResponse.GetDataSetResult result =
                GetDataSetResponse.GetDataSetResult.newBuilder()
                        .setDataSet(dataSet)
                        .build();
        responseObserver.onNext(GetDataSetResponse.newBuilder()
                .setResponseTime(TimestampUtility.getTimestampNow())
                .setGetDataSetResult(result)
                .build());
        responseObserver.onCompleted();
    }

    @Override
    public void getDataSet(
            GetDataSetRequest request,
            StreamObserver<GetDataSetResponse> responseObserver
    ) {
        logger.info("id: {} getDataSet request received dataSetId: {}",
                responseObserver.hashCode(), request.getDataSetId());
        handler.handleGetDataSet(request, responseObserver);
    }

    // =========================================================
    // deleteDataSet
    // =========================================================

    private static DeleteDataSetResponse deleteDataSetResponseExceptionalResult(
            String msg, ExceptionalResult.ExceptionalResultStatus status) {
        final ExceptionalResult exceptionalResult = ExceptionalResult.newBuilder()
                .setExceptionalResultStatus(status)
                .setMessage(msg)
                .build();
        return DeleteDataSetResponse.newBuilder()
                .setResponseTime(TimestampUtility.getTimestampNow())
                .setExceptionalResult(exceptionalResult)
                .build();
    }

    public static void sendDeleteDataSetResponseReject(
            String msg, StreamObserver<DeleteDataSetResponse> responseObserver) {
        responseObserver.onNext(deleteDataSetResponseExceptionalResult(
                msg, ExceptionalResult.ExceptionalResultStatus.RESULT_STATUS_REJECT));
        responseObserver.onCompleted();
    }

    public static void sendDeleteDataSetResponseError(
            String msg, StreamObserver<DeleteDataSetResponse> responseObserver) {
        responseObserver.onNext(deleteDataSetResponseExceptionalResult(
                msg, ExceptionalResult.ExceptionalResultStatus.RESULT_STATUS_ERROR));
        responseObserver.onCompleted();
    }

    public static void sendDeleteDataSetResponseSuccess(
            String dataSetId, StreamObserver<DeleteDataSetResponse> responseObserver) {
        final DeleteDataSetResponse.DeleteDataSetResult result =
                DeleteDataSetResponse.DeleteDataSetResult.newBuilder()
                        .setDataSetId(dataSetId)
                        .build();
        responseObserver.onNext(DeleteDataSetResponse.newBuilder()
                .setResponseTime(TimestampUtility.getTimestampNow())
                .setDeleteDataSetResult(result)
                .build());
        responseObserver.onCompleted();
    }

    @Override
    public void deleteDataSet(
            DeleteDataSetRequest request,
            StreamObserver<DeleteDataSetResponse> responseObserver
    ) {
        logger.info("id: {} deleteDataSet request received dataSetId: {}",
                responseObserver.hashCode(), request.getDataSetId());
        handler.handleDeleteDataSet(request, responseObserver);
    }

    // =========================================================
    // patchDataSet (stub — not yet implemented)
    // =========================================================

    @Override
    public void patchDataSet(
            PatchDataSetRequest request,
            StreamObserver<PatchDataSetResponse> responseObserver
    ) {
        logger.info("id: {} patchDataSet request received", responseObserver.hashCode());
        final ExceptionalResult exceptionalResult = ExceptionalResult.newBuilder()
                .setExceptionalResultStatus(ExceptionalResult.ExceptionalResultStatus.RESULT_STATUS_ERROR)
                .setMessage("patchDataSet() is not yet implemented")
                .build();
        responseObserver.onNext(PatchDataSetResponse.newBuilder()
                .setResponseTime(TimestampUtility.getTimestampNow())
                .setExceptionalResult(exceptionalResult)
                .build());
        responseObserver.onCompleted();
    }

    private static SaveAnnotationResponse saveAnnotationResponseReject(String msg) {
        final ExceptionalResult exceptionalResult =
                ExceptionalResult.newBuilder()
                        .setExceptionalResultStatus(
                                ExceptionalResult.ExceptionalResultStatus.RESULT_STATUS_REJECT)
                        .setMessage(msg)
                        .build();

        final SaveAnnotationResponse response = SaveAnnotationResponse.newBuilder()
                .setResponseTime(TimestampUtility.getTimestampNow())
                .setExceptionalResult(exceptionalResult)
                .build();

        return response;
    }

    public static void sendSaveAnnotationResponseReject(
            String errorMsg,
            StreamObserver<SaveAnnotationResponse> responseObserver
    ) {
        final SaveAnnotationResponse response = saveAnnotationResponseReject(errorMsg);
        responseObserver.onNext(response);
        responseObserver.onCompleted();
    }

    private static SaveAnnotationResponse saveAnnotationResponseError(String msg) {

        final ExceptionalResult exceptionalResult =
                ExceptionalResult.newBuilder()
                        .setExceptionalResultStatus(ExceptionalResult.ExceptionalResultStatus.RESULT_STATUS_ERROR)
                        .setMessage(msg)
                        .build();

        final SaveAnnotationResponse response = SaveAnnotationResponse.newBuilder()
                .setResponseTime(TimestampUtility.getTimestampNow())
                .setExceptionalResult(exceptionalResult)
                .build();

        return response;
    }

    public static void sendSaveAnnotationResponseError(
            String errorMsg, StreamObserver<SaveAnnotationResponse> responseObserver
    ) {
        final SaveAnnotationResponse response = saveAnnotationResponseError(errorMsg);
        responseObserver.onNext(response);
        responseObserver.onCompleted();
    }

    private static SaveAnnotationResponse saveAnnotationResponseSuccess(String annotationId, String calculationsId) {

        final SaveAnnotationResponse.SaveAnnotationResult.Builder resultBuilder =
                SaveAnnotationResponse.SaveAnnotationResult.newBuilder()
                        .setAnnotationId(annotationId);

        // calculationsId is returned alongside annotationId when the request carried calculations,
        // so the addressing key used by getCalculations(), CalculationsSpec, and ColumnProvenance
        // is available without a further round trip (annotation.proto SaveAnnotationResult)
        if (calculationsId != null) {
            resultBuilder.setCalculationsId(calculationsId);
        }

        final SaveAnnotationResponse.SaveAnnotationResult result = resultBuilder
                        .build();

        final SaveAnnotationResponse response = SaveAnnotationResponse.newBuilder()
                .setResponseTime(TimestampUtility.getTimestampNow())
                .setSaveAnnotationResult(result)
                .build();

        return response;
    }

    public static void sendSaveAnnotationResponseSuccess(
            String annotationId,
            String calculationsId,
            StreamObserver<SaveAnnotationResponse> responseObserver
    ) {
        final SaveAnnotationResponse response = saveAnnotationResponseSuccess(annotationId, calculationsId);
        responseObserver.onNext(response);
        responseObserver.onCompleted();
    }

    @Override
    public void saveAnnotation(
            SaveAnnotationRequest request,
            StreamObserver<SaveAnnotationResponse> responseObserver
    ) {
        logger.info(
                "id: {} saveAnnotation request received with name: {}",
                responseObserver.hashCode(),
                request.getName());

        // perform validation of base annotation details
        // validate common annotation details
        final ResultStatus resultStatus =
                AnnotationValidationUtility.validateSaveAnnotationRequest(request);
        if (resultStatus.isError) {
            logger.debug(
                    "id: {} saveAnnotation validation failed: ",
                    responseObserver.hashCode(),
                    resultStatus.msg);
            sendSaveAnnotationResponseReject(
                    resultStatus.msg,
                    responseObserver);
            return;
        }

        // handle request
        handler.handleSaveAnnotation(request, responseObserver);
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

        // An empty criteria list is match-all by contract, not an error, so there is deliberately
        // no list-level emptiness check here.  Per-criterion validation below is unaffected: a
        // criterion that IS supplied must still be well-formed.

        // validate query criteria
        for (QueryAnnotationsRequest.QueryAnnotationsCriterion criterion : request.getCriteriaList()) {

            switch (criterion.getCriterionCase()) {

                case IDCRITERION -> {
                    if (criterion.getIdCriterion().getIdsList().isEmpty()) {
                        final String errorMsg =
                                "QueryAnnotationsRequest.criteria.IdCriterion must specify at least one id";
                        sendQueryAnnotationsResponseReject(errorMsg, responseObserver);
                        return;
                    }
                    if (containsInvalidObjectId(criterion.getIdCriterion().getIdsList())) {
                        final String errorMsg =
                                "QueryAnnotationsRequest.criteria.IdCriterion ids must be valid ObjectId hex strings";
                        sendQueryAnnotationsResponseReject(errorMsg, responseObserver);
                        return;
                    }
                }

                case OWNERCRITERION -> {
                    if (criterion.getOwnerCriterion().getOwnerIdsList().isEmpty()) {
                        final String errorMsg =
                                "QueryAnnotationsRequest.criteria.OwnerCriterion must specify at least one ownerId";
                        sendQueryAnnotationsResponseReject(errorMsg, responseObserver);
                        return;
                    }
                    if (containsBlank(criterion.getOwnerCriterion().getOwnerIdsList())) {
                        final String errorMsg =
                                "QueryAnnotationsRequest.criteria.OwnerCriterion ownerIds must not contain blank entries";
                        sendQueryAnnotationsResponseReject(errorMsg, responseObserver);
                        return;
                    }
                }

                case DATASETSCRITERION -> {
                    if (criterion.getDataSetsCriterion().getDataSetIdsList().isEmpty()) {
                        final String errorMsg =
                                "QueryAnnotationsRequest.criteria.DataSetsCriterion must specify at least one dataSetId";
                        sendQueryAnnotationsResponseReject(errorMsg, responseObserver);
                        return;
                    }
                    if (containsBlank(criterion.getDataSetsCriterion().getDataSetIdsList())) {
                        final String errorMsg =
                                "QueryAnnotationsRequest.criteria.DataSetsCriterion dataSetIds must not contain blank entries";
                        sendQueryAnnotationsResponseReject(errorMsg, responseObserver);
                        return;
                    }
                }

                case ANNOTATIONSCRITERION -> {
                    if (criterion.getAnnotationsCriterion().getAnnotationIdsList().isEmpty()) {
                        final String errorMsg =
                                "QueryAnnotationsRequest.criteria.AnnotationsCriterion must specify at least one annotationId";
                        sendQueryAnnotationsResponseReject(errorMsg, responseObserver);
                        return;
                    }
                    if (containsBlank(criterion.getAnnotationsCriterion().getAnnotationIdsList())) {
                        final String errorMsg =
                                "QueryAnnotationsRequest.criteria.AnnotationsCriterion annotationIds must not contain blank entries";
                        sendQueryAnnotationsResponseReject(errorMsg, responseObserver);
                        return;
                    }
                }

                case NAMECRITERION -> {
                    final QueryAnnotationsRequest.QueryAnnotationsCriterion.NameCriterion nameCriterion =
                            criterion.getNameCriterion();
                    if (nameCriterion.getExactList().isEmpty()
                            && nameCriterion.getPrefixList().isEmpty()
                            && nameCriterion.getContainsList().isEmpty()) {
                        final String errorMsg =
                                "QueryAnnotationsRequest.criteria.NameCriterion must specify at least one of: exact, prefix, contains";
                        sendQueryAnnotationsResponseReject(errorMsg, responseObserver);
                        return;
                    }
                    // A blank prefix/contains entry would build a match-everything regex (#243), so
                    // blank entries in any of the three lists are rejected, never dropped.
                    if (containsBlank(nameCriterion.getExactList())
                            || containsBlank(nameCriterion.getPrefixList())
                            || containsBlank(nameCriterion.getContainsList())) {
                        final String errorMsg =
                                "QueryAnnotationsRequest.criteria.NameCriterion entries must not be blank";
                        sendQueryAnnotationsResponseReject(errorMsg, responseObserver);
                        return;
                    }
                }

                case TEXTCRITERION -> {
                    if (criterion.getTextCriterion().getText().isBlank()) {
                        final String errorMsg =
                                "QueryAnnotationsRequest.criteria.TextCriterion text must be specified";
                        sendQueryAnnotationsResponseReject(errorMsg, responseObserver);
                        return;
                    }
                }

                case TAGSCRITERION -> {
                    if (criterion.getTagsCriterion().getValuesList().isEmpty()) {
                        final String errorMsg =
                                "QueryAnnotationsRequest.criteria.TagsCriterion must specify at least one value";
                        sendQueryAnnotationsResponseReject(errorMsg, responseObserver);
                        return;
                    }
                    if (containsBlank(criterion.getTagsCriterion().getValuesList())) {
                        final String errorMsg =
                                "QueryAnnotationsRequest.criteria.TagsCriterion values must not contain blank entries";
                        sendQueryAnnotationsResponseReject(errorMsg, responseObserver);
                        return;
                    }
                }

                case ATTRIBUTESCRITERION -> {
                    if (criterion.getAttributesCriterion().getKey().isBlank()) {
                        final String errorMsg =
                                "QueryAnnotationsRequest.criteria.AttributesCriterion key must be specified";
                        sendQueryAnnotationsResponseReject(errorMsg, responseObserver);
                        return;
                    }
                    // an empty values list is a legitimate key-existence search; blank entries are not
                    if (containsBlank(criterion.getAttributesCriterion().getValuesList())) {
                        final String errorMsg =
                                "QueryAnnotationsRequest.criteria.AttributesCriterion values must not contain blank entries";
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

    // =========================================================
    // getAnnotation
    // =========================================================

    private static GetAnnotationResponse getAnnotationResponseExceptionalResult(
            String msg, ExceptionalResult.ExceptionalResultStatus status) {
        final ExceptionalResult exceptionalResult = ExceptionalResult.newBuilder()
                .setExceptionalResultStatus(status)
                .setMessage(msg)
                .build();
        return GetAnnotationResponse.newBuilder()
                .setResponseTime(TimestampUtility.getTimestampNow())
                .setExceptionalResult(exceptionalResult)
                .build();
    }

    public static void sendGetAnnotationResponseReject(
            String msg, StreamObserver<GetAnnotationResponse> responseObserver) {
        responseObserver.onNext(getAnnotationResponseExceptionalResult(
                msg, ExceptionalResult.ExceptionalResultStatus.RESULT_STATUS_REJECT));
        responseObserver.onCompleted();
    }

    public static void sendGetAnnotationResponseError(
            String msg, StreamObserver<GetAnnotationResponse> responseObserver) {
        responseObserver.onNext(getAnnotationResponseExceptionalResult(
                msg, ExceptionalResult.ExceptionalResultStatus.RESULT_STATUS_ERROR));
        responseObserver.onCompleted();
    }

    public static void sendGetAnnotationResponseSuccess(
            Annotation annotation,
            StreamObserver<GetAnnotationResponse> responseObserver) {
        final GetAnnotationResponse.GetAnnotationResult result =
                GetAnnotationResponse.GetAnnotationResult.newBuilder()
                        .setAnnotation(annotation)
                        .build();
        responseObserver.onNext(GetAnnotationResponse.newBuilder()
                .setResponseTime(TimestampUtility.getTimestampNow())
                .setGetAnnotationResult(result)
                .build());
        responseObserver.onCompleted();
    }

    @Override
    public void getAnnotation(
            GetAnnotationRequest request,
            StreamObserver<GetAnnotationResponse> responseObserver
    ) {
        logger.info("id: {} getAnnotation request received annotationId: {}",
                responseObserver.hashCode(), request.getAnnotationId());
        handler.handleGetAnnotation(request, responseObserver);
    }

    // =========================================================
    // deleteAnnotation
    // =========================================================

    private static DeleteAnnotationResponse deleteAnnotationResponseExceptionalResult(
            String msg, ExceptionalResult.ExceptionalResultStatus status) {
        final ExceptionalResult exceptionalResult = ExceptionalResult.newBuilder()
                .setExceptionalResultStatus(status)
                .setMessage(msg)
                .build();
        return DeleteAnnotationResponse.newBuilder()
                .setResponseTime(TimestampUtility.getTimestampNow())
                .setExceptionalResult(exceptionalResult)
                .build();
    }

    public static void sendDeleteAnnotationResponseReject(
            String msg, StreamObserver<DeleteAnnotationResponse> responseObserver) {
        responseObserver.onNext(deleteAnnotationResponseExceptionalResult(
                msg, ExceptionalResult.ExceptionalResultStatus.RESULT_STATUS_REJECT));
        responseObserver.onCompleted();
    }

    public static void sendDeleteAnnotationResponseError(
            String msg, StreamObserver<DeleteAnnotationResponse> responseObserver) {
        responseObserver.onNext(deleteAnnotationResponseExceptionalResult(
                msg, ExceptionalResult.ExceptionalResultStatus.RESULT_STATUS_ERROR));
        responseObserver.onCompleted();
    }

    public static void sendDeleteAnnotationResponseSuccess(
            String annotationId, StreamObserver<DeleteAnnotationResponse> responseObserver) {
        final DeleteAnnotationResponse.DeleteAnnotationResult result =
                DeleteAnnotationResponse.DeleteAnnotationResult.newBuilder()
                        .setAnnotationId(annotationId)
                        .build();
        responseObserver.onNext(DeleteAnnotationResponse.newBuilder()
                .setResponseTime(TimestampUtility.getTimestampNow())
                .setDeleteAnnotationResult(result)
                .build());
        responseObserver.onCompleted();
    }

    @Override
    public void deleteAnnotation(
            DeleteAnnotationRequest request,
            StreamObserver<DeleteAnnotationResponse> responseObserver
    ) {
        logger.info("id: {} deleteAnnotation request received annotationId: {}",
                responseObserver.hashCode(), request.getAnnotationId());
        handler.handleDeleteAnnotation(request, responseObserver);
    }

    // =========================================================
    // patchAnnotation (stub — not yet implemented)
    // =========================================================

    @Override
    public void patchAnnotation(
            PatchAnnotationRequest request,
            StreamObserver<PatchAnnotationResponse> responseObserver
    ) {
        logger.info("id: {} patchAnnotation request received", responseObserver.hashCode());
        final ExceptionalResult exceptionalResult = ExceptionalResult.newBuilder()
                .setExceptionalResultStatus(ExceptionalResult.ExceptionalResultStatus.RESULT_STATUS_ERROR)
                .setMessage("patchAnnotation() is not yet implemented")
                .build();
        responseObserver.onNext(PatchAnnotationResponse.newBuilder()
                .setResponseTime(TimestampUtility.getTimestampNow())
                .setExceptionalResult(exceptionalResult)
                .build());
        responseObserver.onCompleted();
    }

    // =========================================================
    // getCalculations
    // =========================================================

    private static GetCalculationsResponse getCalculationsResponseExceptionalResult(
            String msg, ExceptionalResult.ExceptionalResultStatus status) {
        final ExceptionalResult exceptionalResult = ExceptionalResult.newBuilder()
                .setExceptionalResultStatus(status)
                .setMessage(msg)
                .build();
        return GetCalculationsResponse.newBuilder()
                .setResponseTime(TimestampUtility.getTimestampNow())
                .setExceptionalResult(exceptionalResult)
                .build();
    }

    public static void sendGetCalculationsResponseReject(
            String msg, StreamObserver<GetCalculationsResponse> responseObserver) {
        responseObserver.onNext(getCalculationsResponseExceptionalResult(
                msg, ExceptionalResult.ExceptionalResultStatus.RESULT_STATUS_REJECT));
        responseObserver.onCompleted();
    }

    public static void sendGetCalculationsResponseError(
            String msg, StreamObserver<GetCalculationsResponse> responseObserver) {
        responseObserver.onNext(getCalculationsResponseExceptionalResult(
                msg, ExceptionalResult.ExceptionalResultStatus.RESULT_STATUS_ERROR));
        responseObserver.onCompleted();
    }

    public static void sendGetCalculationsResponseSuccess(
            Calculations calculations,
            StreamObserver<GetCalculationsResponse> responseObserver) {
        final GetCalculationsResponse.GetCalculationsResult result =
                GetCalculationsResponse.GetCalculationsResult.newBuilder()
                        .setCalculations(calculations)
                        .build();
        responseObserver.onNext(GetCalculationsResponse.newBuilder()
                .setResponseTime(TimestampUtility.getTimestampNow())
                .setGetCalculationsResult(result)
                .build());
        responseObserver.onCompleted();
    }

    @Override
    public void getCalculations(
            GetCalculationsRequest request,
            StreamObserver<GetCalculationsResponse> responseObserver
    ) {
        logger.info("id: {} getCalculations request received calculationsId: {}",
                responseObserver.hashCode(), request.getCalculationsId());
        handler.handleGetCalculations(request, responseObserver);
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

    // =========================================================
    // savePvMetadata
    // =========================================================

    private static SavePvMetadataResponse savePvMetadataResponseExceptionalResult(
            String msg, ExceptionalResult.ExceptionalResultStatus status) {
        final ExceptionalResult exceptionalResult = ExceptionalResult.newBuilder()
                .setExceptionalResultStatus(status)
                .setMessage(msg)
                .build();
        return SavePvMetadataResponse.newBuilder()
                .setResponseTime(TimestampUtility.getTimestampNow())
                .setExceptionalResult(exceptionalResult)
                .build();
    }

    public static void sendSavePvMetadataResponseReject(
            String msg, StreamObserver<SavePvMetadataResponse> responseObserver) {
        responseObserver.onNext(savePvMetadataResponseExceptionalResult(
                msg, ExceptionalResult.ExceptionalResultStatus.RESULT_STATUS_REJECT));
        responseObserver.onCompleted();
    }

    public static void sendSavePvMetadataResponseError(
            String msg, StreamObserver<SavePvMetadataResponse> responseObserver) {
        responseObserver.onNext(savePvMetadataResponseExceptionalResult(
                msg, ExceptionalResult.ExceptionalResultStatus.RESULT_STATUS_ERROR));
        responseObserver.onCompleted();
    }

    public static void sendSavePvMetadataResponseSuccess(
            String pvName, StreamObserver<SavePvMetadataResponse> responseObserver) {
        final SavePvMetadataResponse.SavePvMetadataResult result =
                SavePvMetadataResponse.SavePvMetadataResult.newBuilder()
                        .setPvName(pvName)
                        .build();
        responseObserver.onNext(SavePvMetadataResponse.newBuilder()
                .setResponseTime(TimestampUtility.getTimestampNow())
                .setSavePvMetadataResult(result)
                .build());
        responseObserver.onCompleted();
    }

    @Override
    public void savePvMetadata(
            SavePvMetadataRequest request,
            StreamObserver<SavePvMetadataResponse> responseObserver
    ) {
        logger.info("id: {} savePvMetadata request received pvName: {}",
                responseObserver.hashCode(), request.getPvName());
        handler.handleSavePvMetadata(request, responseObserver);
    }

    // =========================================================
    // queryPvMetadata
    // =========================================================

    private static QueryPvMetadataResponse queryPvMetadataResponseExceptionalResult(
            String msg, ExceptionalResult.ExceptionalResultStatus status) {
        final ExceptionalResult exceptionalResult = ExceptionalResult.newBuilder()
                .setExceptionalResultStatus(status)
                .setMessage(msg)
                .build();
        return QueryPvMetadataResponse.newBuilder()
                .setResponseTime(TimestampUtility.getTimestampNow())
                .setExceptionalResult(exceptionalResult)
                .build();
    }

    public static void sendQueryPvMetadataResponseReject(
            String msg, StreamObserver<QueryPvMetadataResponse> responseObserver) {
        responseObserver.onNext(queryPvMetadataResponseExceptionalResult(
                msg, ExceptionalResult.ExceptionalResultStatus.RESULT_STATUS_REJECT));
        responseObserver.onCompleted();
    }

    public static void sendQueryPvMetadataResponseError(
            String msg, StreamObserver<QueryPvMetadataResponse> responseObserver) {
        responseObserver.onNext(queryPvMetadataResponseExceptionalResult(
                msg, ExceptionalResult.ExceptionalResultStatus.RESULT_STATUS_ERROR));
        responseObserver.onCompleted();
    }

    public static void sendQueryPvMetadataResponse(
            QueryPvMetadataResponse.PvMetadataResult result,
            StreamObserver<QueryPvMetadataResponse> responseObserver) {
        responseObserver.onNext(QueryPvMetadataResponse.newBuilder()
                .setResponseTime(TimestampUtility.getTimestampNow())
                .setPvMetadataResult(result)
                .build());
        responseObserver.onCompleted();
    }

    @Override
    public void queryPvMetadata(
            QueryPvMetadataRequest request,
            StreamObserver<QueryPvMetadataResponse> responseObserver
    ) {
        logger.info("id: {} queryPvMetadata request received", responseObserver.hashCode());
        handler.handleQueryPvMetadata(request, responseObserver);
    }

    // =========================================================
    // getPvMetadata
    // =========================================================

    private static GetPvMetadataResponse getPvMetadataResponseExceptionalResult(
            String msg, ExceptionalResult.ExceptionalResultStatus status) {
        final ExceptionalResult exceptionalResult = ExceptionalResult.newBuilder()
                .setExceptionalResultStatus(status)
                .setMessage(msg)
                .build();
        return GetPvMetadataResponse.newBuilder()
                .setResponseTime(TimestampUtility.getTimestampNow())
                .setExceptionalResult(exceptionalResult)
                .build();
    }

    public static void sendGetPvMetadataResponseReject(
            String msg, StreamObserver<GetPvMetadataResponse> responseObserver) {
        responseObserver.onNext(getPvMetadataResponseExceptionalResult(
                msg, ExceptionalResult.ExceptionalResultStatus.RESULT_STATUS_REJECT));
        responseObserver.onCompleted();
    }

    public static void sendGetPvMetadataResponseError(
            String msg, StreamObserver<GetPvMetadataResponse> responseObserver) {
        responseObserver.onNext(getPvMetadataResponseExceptionalResult(
                msg, ExceptionalResult.ExceptionalResultStatus.RESULT_STATUS_ERROR));
        responseObserver.onCompleted();
    }

    public static void sendGetPvMetadataResponseSuccess(
            com.ospreydcs.dp.grpc.v1.common.PvMetadata pvMetadata,
            StreamObserver<GetPvMetadataResponse> responseObserver) {
        final GetPvMetadataResponse.GetPvMetadataResult result =
                GetPvMetadataResponse.GetPvMetadataResult.newBuilder()
                        .setPvMetadata(pvMetadata)
                        .build();
        responseObserver.onNext(GetPvMetadataResponse.newBuilder()
                .setResponseTime(TimestampUtility.getTimestampNow())
                .setGetPvMetadataResult(result)
                .build());
        responseObserver.onCompleted();
    }

    @Override
    public void getPvMetadata(
            GetPvMetadataRequest request,
            StreamObserver<GetPvMetadataResponse> responseObserver
    ) {
        logger.info("id: {} getPvMetadata request received pvNameOrAlias: {}",
                responseObserver.hashCode(), request.getPvNameOrAlias());
        handler.handleGetPvMetadata(request, responseObserver);
    }

    // =========================================================
    // deletePvMetadata
    // =========================================================

    private static DeletePvMetadataResponse deletePvMetadataResponseExceptionalResult(
            String msg, ExceptionalResult.ExceptionalResultStatus status) {
        final ExceptionalResult exceptionalResult = ExceptionalResult.newBuilder()
                .setExceptionalResultStatus(status)
                .setMessage(msg)
                .build();
        return DeletePvMetadataResponse.newBuilder()
                .setResponseTime(TimestampUtility.getTimestampNow())
                .setExceptionalResult(exceptionalResult)
                .build();
    }

    public static void sendDeletePvMetadataResponseReject(
            String msg, StreamObserver<DeletePvMetadataResponse> responseObserver) {
        responseObserver.onNext(deletePvMetadataResponseExceptionalResult(
                msg, ExceptionalResult.ExceptionalResultStatus.RESULT_STATUS_REJECT));
        responseObserver.onCompleted();
    }

    public static void sendDeletePvMetadataResponseError(
            String msg, StreamObserver<DeletePvMetadataResponse> responseObserver) {
        responseObserver.onNext(deletePvMetadataResponseExceptionalResult(
                msg, ExceptionalResult.ExceptionalResultStatus.RESULT_STATUS_ERROR));
        responseObserver.onCompleted();
    }

    public static void sendDeletePvMetadataResponseSuccess(
            String pvName, StreamObserver<DeletePvMetadataResponse> responseObserver) {
        final DeletePvMetadataResponse.DeletePvMetadataResult result =
                DeletePvMetadataResponse.DeletePvMetadataResult.newBuilder()
                        .setPvName(pvName)
                        .build();
        responseObserver.onNext(DeletePvMetadataResponse.newBuilder()
                .setResponseTime(TimestampUtility.getTimestampNow())
                .setDeletePvMetadataResult(result)
                .build());
        responseObserver.onCompleted();
    }

    @Override
    public void deletePvMetadata(
            DeletePvMetadataRequest request,
            StreamObserver<DeletePvMetadataResponse> responseObserver
    ) {
        logger.info("id: {} deletePvMetadata request received pvNameOrAlias: {}",
                responseObserver.hashCode(), request.getPvNameOrAlias());
        handler.handleDeletePvMetadata(request, responseObserver);
    }

    // =========================================================
    // saveConfiguration
    // =========================================================

    private static SaveConfigurationResponse saveConfigurationResponseExceptionalResult(
            String msg, ExceptionalResult.ExceptionalResultStatus status) {
        final ExceptionalResult exceptionalResult = ExceptionalResult.newBuilder()
                .setExceptionalResultStatus(status)
                .setMessage(msg)
                .build();
        return SaveConfigurationResponse.newBuilder()
                .setResponseTime(TimestampUtility.getTimestampNow())
                .setExceptionalResult(exceptionalResult)
                .build();
    }

    public static void sendSaveConfigurationResponseReject(
            String msg, StreamObserver<SaveConfigurationResponse> responseObserver) {
        responseObserver.onNext(saveConfigurationResponseExceptionalResult(
                msg, ExceptionalResult.ExceptionalResultStatus.RESULT_STATUS_REJECT));
        responseObserver.onCompleted();
    }

    public static void sendSaveConfigurationResponseError(
            String msg, StreamObserver<SaveConfigurationResponse> responseObserver) {
        responseObserver.onNext(saveConfigurationResponseExceptionalResult(
                msg, ExceptionalResult.ExceptionalResultStatus.RESULT_STATUS_ERROR));
        responseObserver.onCompleted();
    }

    public static void sendSaveConfigurationResponseSuccess(
            String configurationName, StreamObserver<SaveConfigurationResponse> responseObserver) {
        final SaveConfigurationResponse.SaveConfigurationResult result =
                SaveConfigurationResponse.SaveConfigurationResult.newBuilder()
                        .setConfigurationName(configurationName)
                        .build();
        responseObserver.onNext(SaveConfigurationResponse.newBuilder()
                .setResponseTime(TimestampUtility.getTimestampNow())
                .setSaveConfigurationResult(result)
                .build());
        responseObserver.onCompleted();
    }

    @Override
    public void saveConfiguration(
            SaveConfigurationRequest request,
            StreamObserver<SaveConfigurationResponse> responseObserver
    ) {
        logger.info("id: {} saveConfiguration request received configurationName: {}",
                responseObserver.hashCode(), request.getConfigurationName());
        handler.handleSaveConfiguration(request, responseObserver);
    }

    // =========================================================
    // getConfiguration
    // =========================================================

    private static GetConfigurationResponse getConfigurationResponseExceptionalResult(
            String msg, ExceptionalResult.ExceptionalResultStatus status) {
        final ExceptionalResult exceptionalResult = ExceptionalResult.newBuilder()
                .setExceptionalResultStatus(status)
                .setMessage(msg)
                .build();
        return GetConfigurationResponse.newBuilder()
                .setResponseTime(TimestampUtility.getTimestampNow())
                .setExceptionalResult(exceptionalResult)
                .build();
    }

    public static void sendGetConfigurationResponseReject(
            String msg, StreamObserver<GetConfigurationResponse> responseObserver) {
        responseObserver.onNext(getConfigurationResponseExceptionalResult(
                msg, ExceptionalResult.ExceptionalResultStatus.RESULT_STATUS_REJECT));
        responseObserver.onCompleted();
    }

    public static void sendGetConfigurationResponseError(
            String msg, StreamObserver<GetConfigurationResponse> responseObserver) {
        responseObserver.onNext(getConfigurationResponseExceptionalResult(
                msg, ExceptionalResult.ExceptionalResultStatus.RESULT_STATUS_ERROR));
        responseObserver.onCompleted();
    }

    public static void sendGetConfigurationResponseSuccess(
            com.ospreydcs.dp.grpc.v1.common.Configuration configuration,
            StreamObserver<GetConfigurationResponse> responseObserver) {
        final GetConfigurationResponse.GetConfigurationResult result =
                GetConfigurationResponse.GetConfigurationResult.newBuilder()
                        .setConfiguration(configuration)
                        .build();
        responseObserver.onNext(GetConfigurationResponse.newBuilder()
                .setResponseTime(TimestampUtility.getTimestampNow())
                .setGetConfigurationResult(result)
                .build());
        responseObserver.onCompleted();
    }

    @Override
    public void getConfiguration(
            GetConfigurationRequest request,
            StreamObserver<GetConfigurationResponse> responseObserver
    ) {
        logger.info("id: {} getConfiguration request received configurationName: {}",
                responseObserver.hashCode(), request.getConfigurationName());
        handler.handleGetConfiguration(request, responseObserver);
    }

    // =========================================================
    // queryConfigurations
    // =========================================================

    private static QueryConfigurationsResponse queryConfigurationsResponseExceptionalResult(
            String msg, ExceptionalResult.ExceptionalResultStatus status) {
        final ExceptionalResult exceptionalResult = ExceptionalResult.newBuilder()
                .setExceptionalResultStatus(status)
                .setMessage(msg)
                .build();
        return QueryConfigurationsResponse.newBuilder()
                .setResponseTime(TimestampUtility.getTimestampNow())
                .setExceptionalResult(exceptionalResult)
                .build();
    }

    public static void sendQueryConfigurationsResponseReject(
            String msg, StreamObserver<QueryConfigurationsResponse> responseObserver) {
        responseObserver.onNext(queryConfigurationsResponseExceptionalResult(
                msg, ExceptionalResult.ExceptionalResultStatus.RESULT_STATUS_REJECT));
        responseObserver.onCompleted();
    }

    public static void sendQueryConfigurationsResponseError(
            String msg, StreamObserver<QueryConfigurationsResponse> responseObserver) {
        responseObserver.onNext(queryConfigurationsResponseExceptionalResult(
                msg, ExceptionalResult.ExceptionalResultStatus.RESULT_STATUS_ERROR));
        responseObserver.onCompleted();
    }

    public static void sendQueryConfigurationsResponse(
            QueryConfigurationsResponse.QueryConfigurationsResult result,
            StreamObserver<QueryConfigurationsResponse> responseObserver) {
        responseObserver.onNext(QueryConfigurationsResponse.newBuilder()
                .setResponseTime(TimestampUtility.getTimestampNow())
                .setQueryConfigurationsResult(result)
                .build());
        responseObserver.onCompleted();
    }

    @Override
    public void queryConfigurations(
            QueryConfigurationsRequest request,
            StreamObserver<QueryConfigurationsResponse> responseObserver
    ) {
        logger.info("id: {} queryConfigurations request received", responseObserver.hashCode());
        handler.handleQueryConfigurations(request, responseObserver);
    }

    // =========================================================
    // deleteConfiguration
    // =========================================================

    private static DeleteConfigurationResponse deleteConfigurationResponseExceptionalResult(
            String msg, ExceptionalResult.ExceptionalResultStatus status) {
        final ExceptionalResult exceptionalResult = ExceptionalResult.newBuilder()
                .setExceptionalResultStatus(status)
                .setMessage(msg)
                .build();
        return DeleteConfigurationResponse.newBuilder()
                .setResponseTime(TimestampUtility.getTimestampNow())
                .setExceptionalResult(exceptionalResult)
                .build();
    }

    public static void sendDeleteConfigurationResponseReject(
            String msg, StreamObserver<DeleteConfigurationResponse> responseObserver) {
        responseObserver.onNext(deleteConfigurationResponseExceptionalResult(
                msg, ExceptionalResult.ExceptionalResultStatus.RESULT_STATUS_REJECT));
        responseObserver.onCompleted();
    }

    public static void sendDeleteConfigurationResponseError(
            String msg, StreamObserver<DeleteConfigurationResponse> responseObserver) {
        responseObserver.onNext(deleteConfigurationResponseExceptionalResult(
                msg, ExceptionalResult.ExceptionalResultStatus.RESULT_STATUS_ERROR));
        responseObserver.onCompleted();
    }

    public static void sendDeleteConfigurationResponseSuccess(
            String configurationName, StreamObserver<DeleteConfigurationResponse> responseObserver) {
        final DeleteConfigurationResponse.DeleteConfigurationResult result =
                DeleteConfigurationResponse.DeleteConfigurationResult.newBuilder()
                        .setConfigurationName(configurationName)
                        .build();
        responseObserver.onNext(DeleteConfigurationResponse.newBuilder()
                .setResponseTime(TimestampUtility.getTimestampNow())
                .setDeleteConfigurationResult(result)
                .build());
        responseObserver.onCompleted();
    }

    @Override
    public void deleteConfiguration(
            DeleteConfigurationRequest request,
            StreamObserver<DeleteConfigurationResponse> responseObserver
    ) {
        logger.info("id: {} deleteConfiguration request received configurationName: {}",
                responseObserver.hashCode(), request.getConfigurationName());
        handler.handleDeleteConfiguration(request, responseObserver);
    }

    // =========================================================
    // patchConfiguration (stub — not yet implemented)
    // =========================================================

    @Override
    public void patchConfiguration(
            PatchConfigurationRequest request,
            StreamObserver<PatchConfigurationResponse> responseObserver
    ) {
        logger.info("id: {} patchConfiguration request received", responseObserver.hashCode());
        final ExceptionalResult exceptionalResult = ExceptionalResult.newBuilder()
                .setExceptionalResultStatus(ExceptionalResult.ExceptionalResultStatus.RESULT_STATUS_ERROR)
                .setMessage("patchConfiguration() is not yet implemented")
                .build();
        responseObserver.onNext(PatchConfigurationResponse.newBuilder()
                .setResponseTime(TimestampUtility.getTimestampNow())
                .setExceptionalResult(exceptionalResult)
                .build());
        responseObserver.onCompleted();
    }

    // =========================================================
    // bulkSaveConfiguration (stub — not yet implemented)
    // =========================================================

    @Override
    public void bulkSaveConfiguration(
            BulkSaveConfigurationRequest request,
            StreamObserver<BulkSaveConfigurationResponse> responseObserver
    ) {
        logger.info("id: {} bulkSaveConfiguration request received", responseObserver.hashCode());
        final ExceptionalResult exceptionalResult = ExceptionalResult.newBuilder()
                .setExceptionalResultStatus(ExceptionalResult.ExceptionalResultStatus.RESULT_STATUS_ERROR)
                .setMessage("bulkSaveConfiguration() is not yet implemented")
                .build();
        responseObserver.onNext(BulkSaveConfigurationResponse.newBuilder()
                .setResponseTime(TimestampUtility.getTimestampNow())
                .setExceptionalResult(exceptionalResult)
                .build());
        responseObserver.onCompleted();
    }

    // =========================================================
    // saveConfigurationActivation
    // =========================================================

    private static SaveConfigurationActivationResponse saveConfigurationActivationResponseExceptionalResult(
            String msg, ExceptionalResult.ExceptionalResultStatus status) {
        final ExceptionalResult exceptionalResult = ExceptionalResult.newBuilder()
                .setExceptionalResultStatus(status)
                .setMessage(msg)
                .build();
        return SaveConfigurationActivationResponse.newBuilder()
                .setResponseTime(TimestampUtility.getTimestampNow())
                .setExceptionalResult(exceptionalResult)
                .build();
    }

    public static void sendSaveConfigurationActivationResponseReject(
            String msg, StreamObserver<SaveConfigurationActivationResponse> responseObserver) {
        responseObserver.onNext(saveConfigurationActivationResponseExceptionalResult(
                msg, ExceptionalResult.ExceptionalResultStatus.RESULT_STATUS_REJECT));
        responseObserver.onCompleted();
    }

    public static void sendSaveConfigurationActivationResponseError(
            String msg, StreamObserver<SaveConfigurationActivationResponse> responseObserver) {
        responseObserver.onNext(saveConfigurationActivationResponseExceptionalResult(
                msg, ExceptionalResult.ExceptionalResultStatus.RESULT_STATUS_ERROR));
        responseObserver.onCompleted();
    }

    public static void sendSaveConfigurationActivationResponseSuccess(
            String clientActivationId, StreamObserver<SaveConfigurationActivationResponse> responseObserver) {
        final SaveConfigurationActivationResponse.SaveConfigurationActivationResult result =
                SaveConfigurationActivationResponse.SaveConfigurationActivationResult.newBuilder()
                        .setClientActivationId(clientActivationId)
                        .build();
        responseObserver.onNext(SaveConfigurationActivationResponse.newBuilder()
                .setResponseTime(TimestampUtility.getTimestampNow())
                .setSaveConfigurationActivationResult(result)
                .build());
        responseObserver.onCompleted();
    }

    @Override
    public void saveConfigurationActivation(
            SaveConfigurationActivationRequest request,
            StreamObserver<SaveConfigurationActivationResponse> responseObserver
    ) {
        logger.info("id: {} saveConfigurationActivation request received configurationName: {}",
                responseObserver.hashCode(), request.getConfigurationName());
        handler.handleSaveConfigurationActivation(request, responseObserver);
    }

    // =========================================================
    // getConfigurationActivation
    // =========================================================

    private static GetConfigurationActivationResponse getConfigurationActivationResponseExceptionalResult(
            String msg, ExceptionalResult.ExceptionalResultStatus status) {
        final ExceptionalResult exceptionalResult = ExceptionalResult.newBuilder()
                .setExceptionalResultStatus(status)
                .setMessage(msg)
                .build();
        return GetConfigurationActivationResponse.newBuilder()
                .setResponseTime(TimestampUtility.getTimestampNow())
                .setExceptionalResult(exceptionalResult)
                .build();
    }

    public static void sendGetConfigurationActivationResponseReject(
            String msg, StreamObserver<GetConfigurationActivationResponse> responseObserver) {
        responseObserver.onNext(getConfigurationActivationResponseExceptionalResult(
                msg, ExceptionalResult.ExceptionalResultStatus.RESULT_STATUS_REJECT));
        responseObserver.onCompleted();
    }

    public static void sendGetConfigurationActivationResponseError(
            String msg, StreamObserver<GetConfigurationActivationResponse> responseObserver) {
        responseObserver.onNext(getConfigurationActivationResponseExceptionalResult(
                msg, ExceptionalResult.ExceptionalResultStatus.RESULT_STATUS_ERROR));
        responseObserver.onCompleted();
    }

    public static void sendGetConfigurationActivationResponseSuccess(
            com.ospreydcs.dp.grpc.v1.common.ConfigurationActivation activation,
            StreamObserver<GetConfigurationActivationResponse> responseObserver) {
        final GetConfigurationActivationResponse.GetConfigurationActivationResult result =
                GetConfigurationActivationResponse.GetConfigurationActivationResult.newBuilder()
                        .setConfigurationActivation(activation)
                        .build();
        responseObserver.onNext(GetConfigurationActivationResponse.newBuilder()
                .setResponseTime(TimestampUtility.getTimestampNow())
                .setGetConfigurationActivationResult(result)
                .build());
        responseObserver.onCompleted();
    }

    @Override
    public void getConfigurationActivation(
            GetConfigurationActivationRequest request,
            StreamObserver<GetConfigurationActivationResponse> responseObserver
    ) {
        logger.info("id: {} getConfigurationActivation request received", responseObserver.hashCode());
        handler.handleGetConfigurationActivation(request, responseObserver);
    }

    // =========================================================
    // queryConfigurationActivations
    // =========================================================

    private static QueryConfigurationActivationsResponse queryConfigurationActivationsResponseExceptionalResult(
            String msg, ExceptionalResult.ExceptionalResultStatus status) {
        final ExceptionalResult exceptionalResult = ExceptionalResult.newBuilder()
                .setExceptionalResultStatus(status)
                .setMessage(msg)
                .build();
        return QueryConfigurationActivationsResponse.newBuilder()
                .setResponseTime(TimestampUtility.getTimestampNow())
                .setExceptionalResult(exceptionalResult)
                .build();
    }

    public static void sendQueryConfigurationActivationsResponseReject(
            String msg, StreamObserver<QueryConfigurationActivationsResponse> responseObserver) {
        responseObserver.onNext(queryConfigurationActivationsResponseExceptionalResult(
                msg, ExceptionalResult.ExceptionalResultStatus.RESULT_STATUS_REJECT));
        responseObserver.onCompleted();
    }

    public static void sendQueryConfigurationActivationsResponseError(
            String msg, StreamObserver<QueryConfigurationActivationsResponse> responseObserver) {
        responseObserver.onNext(queryConfigurationActivationsResponseExceptionalResult(
                msg, ExceptionalResult.ExceptionalResultStatus.RESULT_STATUS_ERROR));
        responseObserver.onCompleted();
    }

    public static void sendQueryConfigurationActivationsResponse(
            QueryConfigurationActivationsResponse.QueryConfigurationActivationsResult result,
            StreamObserver<QueryConfigurationActivationsResponse> responseObserver) {
        responseObserver.onNext(QueryConfigurationActivationsResponse.newBuilder()
                .setResponseTime(TimestampUtility.getTimestampNow())
                .setQueryConfigurationActivationsResult(result)
                .build());
        responseObserver.onCompleted();
    }

    @Override
    public void queryConfigurationActivations(
            QueryConfigurationActivationsRequest request,
            StreamObserver<QueryConfigurationActivationsResponse> responseObserver
    ) {
        logger.info("id: {} queryConfigurationActivations request received", responseObserver.hashCode());
        handler.handleQueryConfigurationActivations(request, responseObserver);
    }

    // =========================================================
    // deleteConfigurationActivation
    // =========================================================

    private static DeleteConfigurationActivationResponse deleteConfigurationActivationResponseExceptionalResult(
            String msg, ExceptionalResult.ExceptionalResultStatus status) {
        final ExceptionalResult exceptionalResult = ExceptionalResult.newBuilder()
                .setExceptionalResultStatus(status)
                .setMessage(msg)
                .build();
        return DeleteConfigurationActivationResponse.newBuilder()
                .setResponseTime(TimestampUtility.getTimestampNow())
                .setExceptionalResult(exceptionalResult)
                .build();
    }

    public static void sendDeleteConfigurationActivationResponseReject(
            String msg, StreamObserver<DeleteConfigurationActivationResponse> responseObserver) {
        responseObserver.onNext(deleteConfigurationActivationResponseExceptionalResult(
                msg, ExceptionalResult.ExceptionalResultStatus.RESULT_STATUS_REJECT));
        responseObserver.onCompleted();
    }

    public static void sendDeleteConfigurationActivationResponseError(
            String msg, StreamObserver<DeleteConfigurationActivationResponse> responseObserver) {
        responseObserver.onNext(deleteConfigurationActivationResponseExceptionalResult(
                msg, ExceptionalResult.ExceptionalResultStatus.RESULT_STATUS_ERROR));
        responseObserver.onCompleted();
    }

    public static void sendDeleteConfigurationActivationResponseSuccess(
            String clientActivationId, StreamObserver<DeleteConfigurationActivationResponse> responseObserver) {
        final DeleteConfigurationActivationResponse.DeleteConfigurationActivationResult result =
                DeleteConfigurationActivationResponse.DeleteConfigurationActivationResult.newBuilder()
                        .setClientActivationId(clientActivationId)
                        .build();
        responseObserver.onNext(DeleteConfigurationActivationResponse.newBuilder()
                .setResponseTime(TimestampUtility.getTimestampNow())
                .setDeleteConfigurationActivationResult(result)
                .build());
        responseObserver.onCompleted();
    }

    @Override
    public void deleteConfigurationActivation(
            DeleteConfigurationActivationRequest request,
            StreamObserver<DeleteConfigurationActivationResponse> responseObserver
    ) {
        logger.info("id: {} deleteConfigurationActivation request received", responseObserver.hashCode());
        handler.handleDeleteConfigurationActivation(request, responseObserver);
    }

    // =========================================================
    // patchConfigurationActivation (stub — not yet implemented)
    // =========================================================

    @Override
    public void patchConfigurationActivation(
            PatchConfigurationActivationRequest request,
            StreamObserver<PatchConfigurationActivationResponse> responseObserver
    ) {
        logger.info("id: {} patchConfigurationActivation request received", responseObserver.hashCode());
        final ExceptionalResult exceptionalResult = ExceptionalResult.newBuilder()
                .setExceptionalResultStatus(ExceptionalResult.ExceptionalResultStatus.RESULT_STATUS_ERROR)
                .setMessage("patchConfigurationActivation() is not yet implemented")
                .build();
        responseObserver.onNext(PatchConfigurationActivationResponse.newBuilder()
                .setResponseTime(TimestampUtility.getTimestampNow())
                .setExceptionalResult(exceptionalResult)
                .build());
        responseObserver.onCompleted();
    }

    // =========================================================
    // bulkSaveConfigurationActivation (stub — not yet implemented)
    // =========================================================

    @Override
    public void bulkSaveConfigurationActivation(
            BulkSaveConfigurationActivationRequest request,
            StreamObserver<BulkSaveConfigurationActivationResponse> responseObserver
    ) {
        logger.info("id: {} bulkSaveConfigurationActivation request received", responseObserver.hashCode());
        final ExceptionalResult exceptionalResult = ExceptionalResult.newBuilder()
                .setExceptionalResultStatus(ExceptionalResult.ExceptionalResultStatus.RESULT_STATUS_ERROR)
                .setMessage("bulkSaveConfigurationActivation() is not yet implemented")
                .build();
        responseObserver.onNext(BulkSaveConfigurationActivationResponse.newBuilder()
                .setResponseTime(TimestampUtility.getTimestampNow())
                .setExceptionalResult(exceptionalResult)
                .build());
        responseObserver.onCompleted();
    }

    // =========================================================
    // getActiveConfigurations
    // =========================================================

    private static GetActiveConfigurationsResponse getActiveConfigurationsResponseExceptionalResult(
            String msg, ExceptionalResult.ExceptionalResultStatus status) {
        final ExceptionalResult exceptionalResult = ExceptionalResult.newBuilder()
                .setExceptionalResultStatus(status)
                .setMessage(msg)
                .build();
        return GetActiveConfigurationsResponse.newBuilder()
                .setResponseTime(TimestampUtility.getTimestampNow())
                .setExceptionalResult(exceptionalResult)
                .build();
    }

    public static void sendGetActiveConfigurationsResponseReject(
            String msg, StreamObserver<GetActiveConfigurationsResponse> responseObserver) {
        responseObserver.onNext(getActiveConfigurationsResponseExceptionalResult(
                msg, ExceptionalResult.ExceptionalResultStatus.RESULT_STATUS_REJECT));
        responseObserver.onCompleted();
    }

    public static void sendGetActiveConfigurationsResponseError(
            String msg, StreamObserver<GetActiveConfigurationsResponse> responseObserver) {
        responseObserver.onNext(getActiveConfigurationsResponseExceptionalResult(
                msg, ExceptionalResult.ExceptionalResultStatus.RESULT_STATUS_ERROR));
        responseObserver.onCompleted();
    }

    public static void sendGetActiveConfigurationsResponse(
            GetActiveConfigurationsResponse.GetActiveConfigurationsResult result,
            StreamObserver<GetActiveConfigurationsResponse> responseObserver) {
        responseObserver.onNext(GetActiveConfigurationsResponse.newBuilder()
                .setResponseTime(TimestampUtility.getTimestampNow())
                .setGetActiveConfigurationsResult(result)
                .build());
        responseObserver.onCompleted();
    }

    @Override
    public void getActiveConfigurations(
            GetActiveConfigurationsRequest request,
            StreamObserver<GetActiveConfigurationsResponse> responseObserver
    ) {
        logger.info("id: {} getActiveConfigurations request received", responseObserver.hashCode());
        handler.handleGetActiveConfigurations(request, responseObserver);
    }

    // =========================================================
    // patchPvMetadata (stub — not yet implemented)
    // =========================================================

    @Override
    public void patchPvMetadata(
            PatchPvMetadataRequest request,
            StreamObserver<PatchPvMetadataResponse> responseObserver
    ) {
        logger.info("id: {} patchPvMetadata request received", responseObserver.hashCode());
        final ExceptionalResult exceptionalResult = ExceptionalResult.newBuilder()
                .setExceptionalResultStatus(ExceptionalResult.ExceptionalResultStatus.RESULT_STATUS_ERROR)
                .setMessage("patchPvMetadata is not yet implemented")
                .build();
        responseObserver.onNext(PatchPvMetadataResponse.newBuilder()
                .setResponseTime(TimestampUtility.getTimestampNow())
                .setExceptionalResult(exceptionalResult)
                .build());
        responseObserver.onCompleted();
    }

    // =========================================================
    // bulkSavePvMetadata (stub — not yet implemented)
    // =========================================================

    @Override
    public void bulkSavePvMetadata(
            BulkSavePvMetadataRequest request,
            StreamObserver<BulkSavePvMetadataResponse> responseObserver
    ) {
        logger.info("id: {} bulkSavePvMetadata request received", responseObserver.hashCode());
        final ExceptionalResult exceptionalResult = ExceptionalResult.newBuilder()
                .setExceptionalResultStatus(ExceptionalResult.ExceptionalResultStatus.RESULT_STATUS_ERROR)
                .setMessage("bulkSavePvMetadata is not yet implemented")
                .build();
        responseObserver.onNext(BulkSavePvMetadataResponse.newBuilder()
                .setResponseTime(TimestampUtility.getTimestampNow())
                .setExceptionalResult(exceptionalResult)
                .build());
        responseObserver.onCompleted();
    }

    // =========================================================
    // saveSampleStatuses
    // =========================================================

    private static SaveSampleStatusesResponse saveSampleStatusesResponseExceptionalResult(
            String msg, ExceptionalResult.ExceptionalResultStatus status) {
        final ExceptionalResult exceptionalResult = ExceptionalResult.newBuilder()
                .setExceptionalResultStatus(status)
                .setMessage(msg)
                .build();
        return SaveSampleStatusesResponse.newBuilder()
                .setResponseTime(TimestampUtility.getTimestampNow())
                .setExceptionalResult(exceptionalResult)
                .build();
    }

    public static void sendSaveSampleStatusesResponseReject(
            String msg, StreamObserver<SaveSampleStatusesResponse> responseObserver) {
        responseObserver.onNext(saveSampleStatusesResponseExceptionalResult(
                msg, ExceptionalResult.ExceptionalResultStatus.RESULT_STATUS_REJECT));
        responseObserver.onCompleted();
    }

    public static void sendSaveSampleStatusesResponseError(
            String msg, StreamObserver<SaveSampleStatusesResponse> responseObserver) {
        responseObserver.onNext(saveSampleStatusesResponseExceptionalResult(
                msg, ExceptionalResult.ExceptionalResultStatus.RESULT_STATUS_ERROR));
        responseObserver.onCompleted();
    }

    public static void sendSaveSampleStatusesResponseSuccess(
            long savedCount, StreamObserver<SaveSampleStatusesResponse> responseObserver) {
        final SaveSampleStatusesResponse.SaveSampleStatusesResult result =
                SaveSampleStatusesResponse.SaveSampleStatusesResult.newBuilder()
                        .setSavedCount(savedCount)
                        .build();
        responseObserver.onNext(SaveSampleStatusesResponse.newBuilder()
                .setResponseTime(TimestampUtility.getTimestampNow())
                .setSaveSampleStatusesResult(result)
                .build());
        responseObserver.onCompleted();
    }

    @Override
    public void saveSampleStatuses(
            SaveSampleStatusesRequest request,
            StreamObserver<SaveSampleStatusesResponse> responseObserver
    ) {
        logger.info("id: {} saveSampleStatuses request received frames: {}",
                responseObserver.hashCode(), request.getFramesCount());
        handler.handleSaveSampleStatuses(request, responseObserver);
    }

    // =========================================================
    // querySampleStatuses / querySampleStatusesStream
    // =========================================================

    private static QuerySampleStatusesResponse querySampleStatusesResponseExceptionalResult(
            String msg, ExceptionalResult.ExceptionalResultStatus status) {
        final ExceptionalResult exceptionalResult = ExceptionalResult.newBuilder()
                .setExceptionalResultStatus(status)
                .setMessage(msg)
                .build();
        return QuerySampleStatusesResponse.newBuilder()
                .setResponseTime(TimestampUtility.getTimestampNow())
                .setExceptionalResult(exceptionalResult)
                .build();
    }

    public static void sendQuerySampleStatusesResponseReject(
            String msg, StreamObserver<QuerySampleStatusesResponse> responseObserver) {
        responseObserver.onNext(querySampleStatusesResponseExceptionalResult(
                msg, ExceptionalResult.ExceptionalResultStatus.RESULT_STATUS_REJECT));
        responseObserver.onCompleted();
    }

    public static void sendQuerySampleStatusesResponseError(
            String msg, StreamObserver<QuerySampleStatusesResponse> responseObserver) {
        responseObserver.onNext(querySampleStatusesResponseExceptionalResult(
                msg, ExceptionalResult.ExceptionalResultStatus.RESULT_STATUS_ERROR));
        responseObserver.onCompleted();
    }

    public static void sendQuerySampleStatusesResponse(
            QuerySampleStatusesResponse.QuerySampleStatusesResult result,
            StreamObserver<QuerySampleStatusesResponse> responseObserver) {
        responseObserver.onNext(QuerySampleStatusesResponse.newBuilder()
                .setResponseTime(TimestampUtility.getTimestampNow())
                .setQuerySampleStatusesResult(result)
                .build());
        responseObserver.onCompleted();
    }

    public static void sendQuerySampleStatusesResponseStreamChunk(
            QuerySampleStatusesResponse.QuerySampleStatusesResult result,
            StreamObserver<QuerySampleStatusesResponse> responseObserver) {
        responseObserver.onNext(QuerySampleStatusesResponse.newBuilder()
                .setResponseTime(TimestampUtility.getTimestampNow())
                .setQuerySampleStatusesResult(result)
                .build());
    }

    public static void sendQuerySampleStatusesResponseStreamComplete(
            StreamObserver<QuerySampleStatusesResponse> responseObserver) {
        responseObserver.onCompleted();
    }

    @Override
    public void querySampleStatuses(
            QuerySampleStatusesRequest request,
            StreamObserver<QuerySampleStatusesResponse> responseObserver
    ) {
        logger.info("id: {} querySampleStatuses request received", responseObserver.hashCode());
        handler.handleQuerySampleStatuses(request, responseObserver);
    }

    @Override
    public void querySampleStatusesStream(
            QuerySampleStatusesRequest request,
            StreamObserver<QuerySampleStatusesResponse> responseObserver
    ) {
        logger.info("id: {} querySampleStatusesStream request received", responseObserver.hashCode());
        handler.handleQuerySampleStatusesStream(request, responseObserver);
    }

    // =========================================================
    // deleteSampleStatuses
    // =========================================================

    private static DeleteSampleStatusesResponse deleteSampleStatusesResponseExceptionalResult(
            String msg, ExceptionalResult.ExceptionalResultStatus status) {
        final ExceptionalResult exceptionalResult = ExceptionalResult.newBuilder()
                .setExceptionalResultStatus(status)
                .setMessage(msg)
                .build();
        return DeleteSampleStatusesResponse.newBuilder()
                .setResponseTime(TimestampUtility.getTimestampNow())
                .setExceptionalResult(exceptionalResult)
                .build();
    }

    public static void sendDeleteSampleStatusesResponseReject(
            String msg, StreamObserver<DeleteSampleStatusesResponse> responseObserver) {
        responseObserver.onNext(deleteSampleStatusesResponseExceptionalResult(
                msg, ExceptionalResult.ExceptionalResultStatus.RESULT_STATUS_REJECT));
        responseObserver.onCompleted();
    }

    public static void sendDeleteSampleStatusesResponseError(
            String msg, StreamObserver<DeleteSampleStatusesResponse> responseObserver) {
        responseObserver.onNext(deleteSampleStatusesResponseExceptionalResult(
                msg, ExceptionalResult.ExceptionalResultStatus.RESULT_STATUS_ERROR));
        responseObserver.onCompleted();
    }

    public static void sendDeleteSampleStatusesResponseSuccess(
            long deletedCount, StreamObserver<DeleteSampleStatusesResponse> responseObserver) {
        final DeleteSampleStatusesResponse.DeleteSampleStatusesResult result =
                DeleteSampleStatusesResponse.DeleteSampleStatusesResult.newBuilder()
                        .setDeletedCount(deletedCount)
                        .build();
        responseObserver.onNext(DeleteSampleStatusesResponse.newBuilder()
                .setResponseTime(TimestampUtility.getTimestampNow())
                .setDeleteSampleStatusesResult(result)
                .build());
        responseObserver.onCompleted();
    }

    @Override
    public void deleteSampleStatuses(
            DeleteSampleStatusesRequest request,
            StreamObserver<DeleteSampleStatusesResponse> responseObserver
    ) {
        logger.info("id: {} deleteSampleStatuses request received domain: {} layer: {}",
                responseObserver.hashCode(), request.getDomain(), request.getLayer());
        handler.handleDeleteSampleStatuses(request, responseObserver);
    }

    // =========================================================
    // saveSampleStatusDomain (stub — not yet implemented)
    // =========================================================

    @Override
    public void saveSampleStatusDomain(
            SaveSampleStatusDomainRequest request,
            StreamObserver<SaveSampleStatusDomainResponse> responseObserver
    ) {
        logger.info("id: {} saveSampleStatusDomain request received", responseObserver.hashCode());
        final ExceptionalResult exceptionalResult = ExceptionalResult.newBuilder()
                .setExceptionalResultStatus(ExceptionalResult.ExceptionalResultStatus.RESULT_STATUS_ERROR)
                .setMessage("saveSampleStatusDomain is not yet implemented")
                .build();
        responseObserver.onNext(SaveSampleStatusDomainResponse.newBuilder()
                .setResponseTime(TimestampUtility.getTimestampNow())
                .setExceptionalResult(exceptionalResult)
                .build());
        responseObserver.onCompleted();
    }

    // =========================================================
    // querySampleStatusDomains (stub — not yet implemented)
    // =========================================================

    @Override
    public void querySampleStatusDomains(
            QuerySampleStatusDomainsRequest request,
            StreamObserver<QuerySampleStatusDomainsResponse> responseObserver
    ) {
        logger.info("id: {} querySampleStatusDomains request received", responseObserver.hashCode());
        final ExceptionalResult exceptionalResult = ExceptionalResult.newBuilder()
                .setExceptionalResultStatus(ExceptionalResult.ExceptionalResultStatus.RESULT_STATUS_ERROR)
                .setMessage("querySampleStatusDomains is not yet implemented")
                .build();
        responseObserver.onNext(QuerySampleStatusDomainsResponse.newBuilder()
                .setResponseTime(TimestampUtility.getTimestampNow())
                .setExceptionalResult(exceptionalResult)
                .build());
        responseObserver.onCompleted();
    }
}
