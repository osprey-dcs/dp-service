package com.ospreydcs.dp.client;

import com.ospreydcs.dp.grpc.v1.common.ExceptionalResult;
import com.ospreydcs.dp.client.result.*;
import com.ospreydcs.dp.grpc.v1.annotation.*;
import com.ospreydcs.dp.grpc.v1.common.CalculationsSpec;
import com.ospreydcs.dp.grpc.v1.common.Timestamp;
import com.ospreydcs.dp.service.common.protobuf.AttributesUtility;
import io.grpc.ManagedChannel;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

public class AnnotationClient extends ServiceApiClientBase {

    // static variables
    private static final Logger logger = LogManager.getLogger();

    public AnnotationClient(ManagedChannel channel) {
        super(channel);
    }

    public record AnnotationDataBlock(
            long beginSeconds,
            long beginNanos,
            long endSeconds,
            long endNanos,
            List<String> pvNames) {
    }

    public record AnnotationDataSet(
            String id,
            String name,
            String ownerId,
            String description,
            List<AnnotationDataBlock> dataBlocks) {
    }

    public record SaveDataSetParams(AnnotationDataSet dataSet) {
    }
    
    public static class SaveDataSetResponseObserver
            extends ApiResponseObserverBase<SaveDataSetResponse> {

        private final List<String> dataSetIdList = Collections.synchronizedList(new ArrayList<>());

        @Override
        protected boolean hasExceptionalResult(SaveDataSetResponse response) {
            return response.hasExceptionalResult();
        }

        @Override
        protected ExceptionalResult getExceptionalResult(SaveDataSetResponse response) {
            return response.getExceptionalResult();
        }

        @Override
        protected boolean handleResult(SaveDataSetResponse response) {
            dataSetIdList.add(response.getSaveDataSetResult().getDataSetId());
            return true;
        }

        public String getDataSetId() {
            if (dataSetIdList.isEmpty()) {
                return null;
            } else {
                return dataSetIdList.get(0);
            }
        }
    }

    public static class QueryDataSetsParams {

        public String idCriterion = null;
        public String ownerCriterion = null;
        public String textCriterion = null;
        public String pvNameCriterion = null;

        public void setIdCriterion(String idCriterion) {
            this.idCriterion = idCriterion;
        }

        public void setOwnerCriterion(String ownerCriterion) {
            this.ownerCriterion = ownerCriterion;
        }

        public void setTextCriterion(String commentCriterion) {
            this.textCriterion = commentCriterion;
        }

        public void setPvNameCriterion(String pvNameCriterion) {
            this.pvNameCriterion = pvNameCriterion;
        }
    }

    public static class QueryDataSetsResponseObserver
            extends ApiResponseObserverBase<QueryDataSetsResponse> {

        private final List<DataSet> dataSetsList =
                Collections.synchronizedList(new ArrayList<>());

        @Override
        protected boolean hasExceptionalResult(QueryDataSetsResponse response) {
            return response.hasExceptionalResult();
        }

        @Override
        protected ExceptionalResult getExceptionalResult(QueryDataSetsResponse response) {
            return response.getExceptionalResult();
        }

        @Override
        protected boolean handleResult(QueryDataSetsResponse response) {

            if (!response.hasDataSetsResult()) {
                recordFailure(observerName() + " response does not contain DataSetsResult");
                return false;
            }

            dataSetsList.addAll(response.getDataSetsResult().getDataSetsList());
            return true;
        }

        public List<DataSet> getDataSetsList() {
            return dataSetsList;
        }
    }

    public record SaveAnnotationRequestParams(
            String id,
            String ownerId,
            String name,
            List<String> dataSetIds,
            List<String> annotationIds,
            String comment,
            List<String> tags,
            Map<String, String> attributeMap,
            Calculations calculations
    ) {
    }

    public static class SaveAnnotationResponseObserver
            extends ApiResponseObserverBase<SaveAnnotationResponse> {

        private final List<String> annotationIdList = Collections.synchronizedList(new ArrayList<>());

        @Override
        protected boolean hasExceptionalResult(SaveAnnotationResponse response) {
            return response.hasExceptionalResult();
        }

        @Override
        protected ExceptionalResult getExceptionalResult(SaveAnnotationResponse response) {
            return response.getExceptionalResult();
        }

        @Override
        protected boolean handleResult(SaveAnnotationResponse response) {
            annotationIdList.add(response.getSaveAnnotationResult().getAnnotationId());
            return true;
        }

        public String getAnnotationId() {
            if (annotationIdList.isEmpty()) {
                return null;
            } else {
                return annotationIdList.get(0);
            }
        }
    }

    public static class QueryAnnotationsParams {

        public String idCriterion = null;
        public String ownerCriterion = null;
        public String datasetsCriterion = null;
        public String annotationsCriterion = null;
        public String textCriterion = null;
        public String tagsCriterion = null;
        public String attributesCriterionKey = null;
        public String attributesCriterionValue = null;
        public String eventCriterion = null;

        public void setIdCriterion(String idCriterion) {
            this.idCriterion = idCriterion;
        }

        public void setOwnerCriterion(String ownerCriterion) {
            this.ownerCriterion = ownerCriterion;
        }

        public void setDatasetsCriterion(String datasetsCriterion) {
            this.datasetsCriterion = datasetsCriterion;
        }

        public void setAnnotationsCriterion(String annotationsCriterion) {
            this.annotationsCriterion = annotationsCriterion;
        }

        public void setTextCriterion(String commentCriterion) {
            this.textCriterion = commentCriterion;
        }

        public void setTagsCriterion(String tagsCriterion) {
            this.tagsCriterion = tagsCriterion;
        }

        public void setAttributesCriterion(String attributeCriterionKey, String attributeCriterionValue) {
            this.attributesCriterionKey = attributeCriterionKey;
            this.attributesCriterionValue = attributeCriterionValue;
        }

    }

    public static class QueryAnnotationsResponseObserver
            extends ApiResponseObserverBase<QueryAnnotationsResponse> {

        private final List<QueryAnnotationsResponse.AnnotationsResult.Annotation> annotationsList =
                Collections.synchronizedList(new ArrayList<>());

        @Override
        protected boolean hasExceptionalResult(QueryAnnotationsResponse response) {
            return response.hasExceptionalResult();
        }

        @Override
        protected ExceptionalResult getExceptionalResult(QueryAnnotationsResponse response) {
            return response.getExceptionalResult();
        }

        @Override
        protected boolean handleResult(QueryAnnotationsResponse response) {

            if (!response.hasAnnotationsResult()) {
                recordFailure(observerName() + " response does not contain AnnotationsResult");
                return false;
            }

            annotationsList.addAll(response.getAnnotationsResult().getAnnotationsList());
            return true;
        }

        public List<QueryAnnotationsResponse.AnnotationsResult.Annotation> getAnnotationsList() {
            return annotationsList;
        }
    }

    public record ExportDataRequestParams(
            String dataSetId,
            CalculationsSpec calculationsSpec,
            ExportDataRequest.ExportOutputFormat outputFormat
    ) {
    }

    public static class ExportDataResponseObserver
            extends ApiResponseObserverBase<ExportDataResponse> {

        private final List<ExportDataResponse.ExportDataResult> resultList =
                Collections.synchronizedList(new ArrayList<>());

        @Override
        protected boolean hasExceptionalResult(ExportDataResponse response) {
            return response.hasExceptionalResult();
        }

        @Override
        protected ExceptionalResult getExceptionalResult(ExportDataResponse response) {
            return response.getExceptionalResult();
        }

        @Override
        protected boolean handleResult(ExportDataResponse response) {

            if (!response.hasExportDataResult()) {
                recordFailure(observerName() + " response does not contain ExportDataResult");
                return false;
            }

            resultList.add(response.getExportDataResult());
            return true;
        }

        public ExportDataResponse.ExportDataResult getResult() {
            if (resultList.isEmpty()) {
                return null;
            } else {
                return resultList.get(0);
            }
        }
    }

    public static SaveDataSetRequest buildSaveDataSetRequest(SaveDataSetParams params) {

        com.ospreydcs.dp.grpc.v1.annotation.DataSet.Builder dataSetBuilder
                = com.ospreydcs.dp.grpc.v1.annotation.DataSet.newBuilder();

        for (AnnotationDataBlock block : params.dataSet.dataBlocks) {

            Timestamp.Builder beginTimeBuilder = Timestamp.newBuilder();
            beginTimeBuilder.setEpochSeconds(block.beginSeconds);
            beginTimeBuilder.setNanoseconds(block.beginNanos);

            Timestamp.Builder endTimeBuilder = Timestamp.newBuilder();
            endTimeBuilder.setEpochSeconds(block.endSeconds);
            endTimeBuilder.setNanoseconds(block.endNanos);

            com.ospreydcs.dp.grpc.v1.annotation.DataBlock.Builder dataBlockBuilder
                    = com.ospreydcs.dp.grpc.v1.annotation.DataBlock.newBuilder();
            dataBlockBuilder.setBeginTime(beginTimeBuilder);
            dataBlockBuilder.setEndTime(endTimeBuilder);
            dataBlockBuilder.addAllPvNames(block.pvNames);
            dataBlockBuilder.build();

            dataSetBuilder.addDataBlocks(dataBlockBuilder);
        }

        if (params.dataSet.id != null) {
            dataSetBuilder.setId(params.dataSet.id);
        }

        dataSetBuilder.setName(params.dataSet.name);
        dataSetBuilder.setDescription(params.dataSet.description);
        dataSetBuilder.setOwnerId(params.dataSet.ownerId);

        dataSetBuilder.build();

        SaveDataSetRequest.Builder requestBuilder = SaveDataSetRequest.newBuilder();
        requestBuilder.setDataSet(dataSetBuilder);

        return requestBuilder.build();
    }

    public SaveDataSetApiResult sendSaveDataSet(
            SaveDataSetRequest request
    ) {
        final DpAnnotationServiceGrpc.DpAnnotationServiceStub asyncStub =
                DpAnnotationServiceGrpc.newStub(channel);

        final SaveDataSetResponseObserver responseObserver =
                new SaveDataSetResponseObserver();

        // send request in separate thread to better simulate out of process grpc,
        // otherwise service handles request in this thread
        new Thread(() -> {
            asyncStub.saveDataSet(request, responseObserver);
        }).start();

        responseObserver.await();

        if (responseObserver.isError()) {
            return new SaveDataSetApiResult(
                    true, responseObserver.getErrorMessage(), responseObserver.getApiResultStatus());
        } else {
            return new SaveDataSetApiResult(responseObserver.getDataSetId());
        }
    }

    public SaveDataSetApiResult saveDataSet(
            SaveDataSetParams params
    ) {
        final SaveDataSetRequest request = buildSaveDataSetRequest(params);
        return sendSaveDataSet(request);
    }

    public static QueryDataSetsRequest buildQueryDataSetsRequest(
            QueryDataSetsParams params
    ) {
        QueryDataSetsRequest.Builder requestBuilder = QueryDataSetsRequest.newBuilder();

        // add id criteria
        if (params.idCriterion != null) {
            QueryDataSetsRequest.QueryDataSetsCriterion.IdCriterion idCriterion =
                    QueryDataSetsRequest.QueryDataSetsCriterion.IdCriterion.newBuilder()
                            .setId(params.idCriterion)
                            .build();
            QueryDataSetsRequest.QueryDataSetsCriterion idQueryDataSetsCriterion =
                    QueryDataSetsRequest.QueryDataSetsCriterion.newBuilder()
                            .setIdCriterion(idCriterion)
                            .build();
            requestBuilder.addCriteria(idQueryDataSetsCriterion);
        }

        // add owner criteria
        if (params.ownerCriterion != null) {
            QueryDataSetsRequest.QueryDataSetsCriterion.OwnerCriterion ownerCriterion =
                    QueryDataSetsRequest.QueryDataSetsCriterion.OwnerCriterion.newBuilder()
                            .setOwnerId(params.ownerCriterion)
                            .build();
            QueryDataSetsRequest.QueryDataSetsCriterion ownerQueryDataSetsCriterion =
                    QueryDataSetsRequest.QueryDataSetsCriterion.newBuilder()
                            .setOwnerCriterion(ownerCriterion)
                            .build();
            requestBuilder.addCriteria(ownerQueryDataSetsCriterion);
        }

        // add description criteria
        if (params.textCriterion != null) {
            QueryDataSetsRequest.QueryDataSetsCriterion.TextCriterion textCriterion =
                    QueryDataSetsRequest.QueryDataSetsCriterion.TextCriterion.newBuilder()
                            .setText(params.textCriterion)
                            .build();
            QueryDataSetsRequest.QueryDataSetsCriterion descriptionQueryDataSetsCriterion =
                    QueryDataSetsRequest.QueryDataSetsCriterion.newBuilder()
                            .setTextCriterion(textCriterion)
                            .build();
            requestBuilder.addCriteria(descriptionQueryDataSetsCriterion);
        }

        // add pvName criteria
        if (params.pvNameCriterion != null) {
            QueryDataSetsRequest.QueryDataSetsCriterion.PvNameCriterion pvNameCriterion =
                    QueryDataSetsRequest.QueryDataSetsCriterion.PvNameCriterion.newBuilder()
                            .setName(params.pvNameCriterion)
                            .build();
            QueryDataSetsRequest.QueryDataSetsCriterion pvNameQueryDataSetsCriterion =
                    QueryDataSetsRequest.QueryDataSetsCriterion.newBuilder()
                            .setPvNameCriterion(pvNameCriterion)
                            .build();
            requestBuilder.addCriteria(pvNameQueryDataSetsCriterion);
        }

        return requestBuilder.build();
    }

    public QueryDataSetsApiResult sendQueryDataSets(
            QueryDataSetsRequest request
    ) {
        final DpAnnotationServiceGrpc.DpAnnotationServiceStub asyncStub =
                DpAnnotationServiceGrpc.newStub(channel);

        final QueryDataSetsResponseObserver responseObserver = new QueryDataSetsResponseObserver();

        // send request in separate thread to better simulate out of process grpc,
        // otherwise service handles request in this thread
        new Thread(() -> {
            asyncStub.queryDataSets(request, responseObserver);
        }).start();

        responseObserver.await();

        if (responseObserver.isError()) {
            return new QueryDataSetsApiResult(
                    true, responseObserver.getErrorMessage(), responseObserver.getApiResultStatus());
        } else {
            return new QueryDataSetsApiResult(responseObserver.getDataSetsList());
        }
    }

    public QueryDataSetsApiResult queryDataSets(
            QueryDataSetsParams queryParams
    ) {
        final QueryDataSetsRequest request = buildQueryDataSetsRequest(queryParams);
        return sendQueryDataSets(request);
    }

    public static SaveAnnotationRequest buildSaveAnnotationRequest(SaveAnnotationRequestParams params) {

        SaveAnnotationRequest.Builder requestBuilder = SaveAnnotationRequest.newBuilder();

        if (params.id != null) {
            requestBuilder.setId(params.id);
        }

        // handle required annotation fields
        requestBuilder.setOwnerId(params.ownerId);
        requestBuilder.addAllDataSetIds(params.dataSetIds);
        requestBuilder.setName(params.name);

        // handle optional annotation fields
        if (params.annotationIds != null) {
            requestBuilder.addAllAnnotationIds(params.annotationIds);
        }
        if (params.comment != null) {
            requestBuilder.setComment(params.comment);
        }
        if (params.tags != null) {
            requestBuilder.addAllTags(params.tags);
        }
        if (params.attributeMap != null) {
            requestBuilder.addAllAttributes(AttributesUtility.attributeListFromMap(params.attributeMap));
        }
        if (params.calculations != null) {
            requestBuilder.setCalculations(params.calculations);
        }

        return requestBuilder.build();
    }

    public SaveAnnotationApiResult sendSaveAnnotation(
            SaveAnnotationRequest request
    ) {
        final DpAnnotationServiceGrpc.DpAnnotationServiceStub asyncStub =
                DpAnnotationServiceGrpc.newStub(channel);

        final SaveAnnotationResponseObserver responseObserver = new SaveAnnotationResponseObserver();

        // send request in separate thread to better simulate out of process grpc,
        // otherwise service handles request in this thread
        new Thread(() -> {
            asyncStub.saveAnnotation(request, responseObserver);
        }).start();

        responseObserver.await();

        if (responseObserver.isError()) {
            return new SaveAnnotationApiResult(
                    true, responseObserver.getErrorMessage(), responseObserver.getApiResultStatus());
        } else {
            return new SaveAnnotationApiResult(responseObserver.getAnnotationId());
        }
    }

    public SaveAnnotationApiResult saveAnnotation(
            SaveAnnotationRequestParams params
    ) {
        final SaveAnnotationRequest request = buildSaveAnnotationRequest(params);

        return sendSaveAnnotation(request);
    }

    public static QueryAnnotationsRequest buildQueryAnnotationsRequest(
            final QueryAnnotationsParams params
    ) {
        QueryAnnotationsRequest.Builder requestBuilder = QueryAnnotationsRequest.newBuilder();

        // handle IdCriterion
        if (params.idCriterion != null) {
            QueryAnnotationsRequest.QueryAnnotationsCriterion.IdCriterion idCriterion =
                    QueryAnnotationsRequest.QueryAnnotationsCriterion.IdCriterion.newBuilder()
                            .setId(params.idCriterion)
                            .build();
            QueryAnnotationsRequest.QueryAnnotationsCriterion idQueryAnnotationsCriterion =
                    QueryAnnotationsRequest.QueryAnnotationsCriterion.newBuilder()
                            .setIdCriterion(idCriterion)
                            .build();
            requestBuilder.addCriteria(idQueryAnnotationsCriterion);
        }

        // handle OwnerCriterion
        if (params.ownerCriterion != null) {
            QueryAnnotationsRequest.QueryAnnotationsCriterion.OwnerCriterion ownerCriterion =
                    QueryAnnotationsRequest.QueryAnnotationsCriterion.OwnerCriterion.newBuilder()
                            .setOwnerId(params.ownerCriterion)
                            .build();
            QueryAnnotationsRequest.QueryAnnotationsCriterion ownerQueryAnnotationsCriterion =
                    QueryAnnotationsRequest.QueryAnnotationsCriterion.newBuilder()
                            .setOwnerCriterion(ownerCriterion)
                            .build();
            requestBuilder.addCriteria(ownerQueryAnnotationsCriterion);
        }

        // handle DataSetsCriterion
        if (params.datasetsCriterion != null) {
            QueryAnnotationsRequest.QueryAnnotationsCriterion.DataSetsCriterion dataSetsCriterion =
                    QueryAnnotationsRequest.QueryAnnotationsCriterion.DataSetsCriterion.newBuilder()
                            .setDataSetId(params.datasetsCriterion)
                            .build();
            QueryAnnotationsRequest.QueryAnnotationsCriterion datasetIdQueryAnnotationsCriterion =
                    QueryAnnotationsRequest.QueryAnnotationsCriterion.newBuilder()
                            .setDataSetsCriterion(dataSetsCriterion)
                            .build();
            requestBuilder.addCriteria(datasetIdQueryAnnotationsCriterion);
        }

        // handle AnnotationsCriterion
        if (params.annotationsCriterion != null) {
            QueryAnnotationsRequest.QueryAnnotationsCriterion.AnnotationsCriterion annotationsCriterion =
                    QueryAnnotationsRequest.QueryAnnotationsCriterion.AnnotationsCriterion.newBuilder()
                            .setAnnotationId(params.annotationsCriterion)
                            .build();
            QueryAnnotationsRequest.QueryAnnotationsCriterion associatedAnnotationQueryAnnotationsCriterion =
                    QueryAnnotationsRequest.QueryAnnotationsCriterion.newBuilder()
                            .setAnnotationsCriterion(annotationsCriterion)
                            .build();
            requestBuilder.addCriteria(associatedAnnotationQueryAnnotationsCriterion);
        }

        // handle TextCriterion
        if (params.textCriterion != null) {
            QueryAnnotationsRequest.QueryAnnotationsCriterion.TextCriterion textCriterion =
                    QueryAnnotationsRequest.QueryAnnotationsCriterion.TextCriterion.newBuilder()
                            .setText(params.textCriterion)
                            .build();
            QueryAnnotationsRequest.QueryAnnotationsCriterion commentQueryAnnotationsCriterion =
                    QueryAnnotationsRequest.QueryAnnotationsCriterion.newBuilder()
                            .setTextCriterion(textCriterion)
                            .build();
            requestBuilder.addCriteria(commentQueryAnnotationsCriterion);
        }

        // handle TagsCriterion
        if (params.tagsCriterion != null) {
            QueryAnnotationsRequest.QueryAnnotationsCriterion.TagsCriterion tagsCriterion =
                    QueryAnnotationsRequest.QueryAnnotationsCriterion.TagsCriterion.newBuilder()
                            .setTagValue(params.tagsCriterion)
                            .build();
            QueryAnnotationsRequest.QueryAnnotationsCriterion tagsQueryAnnotationsCriterion =
                    QueryAnnotationsRequest.QueryAnnotationsCriterion.newBuilder()
                            .setTagsCriterion(tagsCriterion)
                            .build();
            requestBuilder.addCriteria(tagsQueryAnnotationsCriterion);
        }

        // handle AttributesCriterion
        if (params.attributesCriterionKey != null && params.attributesCriterionValue != null) {
            QueryAnnotationsRequest.QueryAnnotationsCriterion.AttributesCriterion attributesCriterion =
                    QueryAnnotationsRequest.QueryAnnotationsCriterion.AttributesCriterion.newBuilder()
                            .setKey(params.attributesCriterionKey)
                            .setValue(params.attributesCriterionValue)
                            .build();
            QueryAnnotationsRequest.QueryAnnotationsCriterion attributesQueryAnnotationsCriterion =
                    QueryAnnotationsRequest.QueryAnnotationsCriterion.newBuilder()
                            .setAttributesCriterion(attributesCriterion)
                            .build();
            requestBuilder.addCriteria(attributesQueryAnnotationsCriterion);
        }

        return requestBuilder.build();
    }

    public QueryAnnotationsApiResult sendQueryAnnotations(
            QueryAnnotationsRequest request
    ) {
        final DpAnnotationServiceGrpc.DpAnnotationServiceStub asyncStub =
                DpAnnotationServiceGrpc.newStub(channel);

        final QueryAnnotationsResponseObserver responseObserver = new QueryAnnotationsResponseObserver();

        // send request in separate thread to better simulate out of process grpc,
        // otherwise service handles request in this thread
        new Thread(() -> {
            asyncStub.queryAnnotations(request, responseObserver);
        }).start();

        responseObserver.await();

        if (responseObserver.isError()) {
            return new QueryAnnotationsApiResult(
                    true, responseObserver.getErrorMessage(), responseObserver.getApiResultStatus());
        } else {
            return new QueryAnnotationsApiResult(responseObserver.getAnnotationsList());
        }
    }

    public QueryAnnotationsApiResult queryAnnotations(
            QueryAnnotationsParams queryParams
    ) {
        final QueryAnnotationsRequest request = buildQueryAnnotationsRequest(queryParams);

        return sendQueryAnnotations(request);
    }

    public static ExportDataRequest buildExportDataRequest(
            ExportDataRequestParams params
    ) {
        ExportDataRequest.Builder requestBuilder = ExportDataRequest.newBuilder();

        // set datasetId if specified
        if (params.dataSetId != null) {
            requestBuilder.setDataSetId(params.dataSetId);
        }

        // create calculationsSpec if calculationsId is specified
        if (params.calculationsSpec != null) {
            requestBuilder.setCalculationsSpec(params.calculationsSpec);
        }

        // set output format
        requestBuilder.setOutputFormat(params.outputFormat);

        return requestBuilder.build();
    }

    protected ExportDataApiResult sendExportData(
            ExportDataRequest request
    ) {
        final DpAnnotationServiceGrpc.DpAnnotationServiceStub asyncStub =
                DpAnnotationServiceGrpc.newStub(channel);

        final ExportDataResponseObserver responseObserver =
                new ExportDataResponseObserver();

        // start performance measurment timer
        final Instant t0 = Instant.now();

        // send request in separate thread to better simulate out of process grpc,
        // otherwise service handles request in this thread
        new Thread(() -> {
            asyncStub.exportData(request, responseObserver);
        }).start();

        responseObserver.await();

        // stop performance measurement timer
        final Instant t1 = Instant.now();
        final long dtMillis = t0.until(t1, ChronoUnit.MILLIS);
        final double secondsElapsed = dtMillis / 1_000.0;
        System.out.println("export format " + request.getOutputFormat().name() + " elapsed seconds: " + secondsElapsed);

        if (responseObserver.isError()) {
            return new ExportDataApiResult(
                    true, responseObserver.getErrorMessage(), responseObserver.getApiResultStatus());
        } else {
            return new ExportDataApiResult(responseObserver.getResult());
        }
    }

    public ExportDataApiResult exportData(
            ExportDataRequestParams params
    ) {
        final ExportDataRequest request = buildExportDataRequest(params);
        return sendExportData(request);
    }

    /*
     * Parameters for savePvMetadata().  Only pvName is required; the remaining fields are optional
     * and are omitted from the request when null or empty.  Attributes are supplied as a map, to
     * match how the rest of this client layer accepts them, and are converted to the repeated
     * Attribute field by buildSavePvMetadataRequest().
     *
     * Because savePvMetadata() is a full-replace upsert, this record must express the complete
     * desired state of the record, not just the fields being changed.  See savePvMetadata().
     */
    public record SavePvMetadataParams(
            String pvName,
            List<String> aliases,
            List<String> tags,
            Map<String, String> attributeMap,
            String description,
            String modifiedBy
    ) {
    }

    public static class SavePvMetadataResponseObserver
            extends ApiResponseObserverBase<SavePvMetadataResponse> {

        private final List<String> pvNameList = Collections.synchronizedList(new ArrayList<>());

        @Override
        protected boolean hasExceptionalResult(SavePvMetadataResponse response) {
            return response.hasExceptionalResult();
        }

        @Override
        protected ExceptionalResult getExceptionalResult(SavePvMetadataResponse response) {
            return response.getExceptionalResult();
        }

        @Override
        protected boolean handleResult(SavePvMetadataResponse response) {
            pvNameList.add(response.getSavePvMetadataResult().getPvName());
            return true;
        }

        public String getPvName() {
            if (pvNameList.isEmpty()) {
                return null;
            } else {
                return pvNameList.get(0);
            }
        }
    }

    public static SavePvMetadataRequest buildSavePvMetadataRequest(SavePvMetadataParams params) {

        final SavePvMetadataRequest.Builder requestBuilder = SavePvMetadataRequest.newBuilder();

        // handle required field
        if (params.pvName() != null) {
            requestBuilder.setPvName(params.pvName());
        }

        // handle optional fields, leaving them unset when not supplied.  The server does not
        // distinguish an unset repeated or string field from an empty one, but leaving them unset
        // keeps the request minimal and matches the other builders in this class.
        if (params.aliases() != null) {
            requestBuilder.addAllAliases(params.aliases());
        }
        if (params.tags() != null) {
            // tags are lowercased, deduplicated and sorted server-side; no client normalization
            requestBuilder.addAllTags(params.tags());
        }
        if (params.attributeMap() != null) {
            requestBuilder.addAllAttributes(AttributesUtility.attributeListFromMap(params.attributeMap()));
        }
        if (params.description() != null) {
            requestBuilder.setDescription(params.description());
        }
        if (params.modifiedBy() != null) {
            requestBuilder.setModifiedBy(params.modifiedBy());
        }

        return requestBuilder.build();
    }

    public SavePvMetadataApiResult sendSavePvMetadata(
            SavePvMetadataRequest request
    ) {
        final DpAnnotationServiceGrpc.DpAnnotationServiceStub asyncStub =
                DpAnnotationServiceGrpc.newStub(channel);

        final SavePvMetadataResponseObserver responseObserver = new SavePvMetadataResponseObserver();

        // send request in separate thread to better simulate out of process grpc,
        // otherwise service handles request in this thread
        new Thread(() -> {
            asyncStub.savePvMetadata(request, responseObserver);
        }).start();

        responseObserver.await();

        if (responseObserver.isError()) {
            return new SavePvMetadataApiResult(
                    true, responseObserver.getErrorMessage(), responseObserver.getApiResultStatus());
        } else {
            return new SavePvMetadataApiResult(responseObserver.getPvName());
        }
    }

    /**
     * Creates or updates the PV metadata record for the specified canonical PV name.
     *
     * This is a full-replace upsert: aliases, tags, attributes, description and modifiedBy are all
     * replaced by the values in params on every save, and fields omitted from params are not
     * preserved from an existing record.  Callers updating an existing record must therefore supply
     * the complete desired state rather than only the fields being changed.
     *
     * Server-side rejections (a blank pvName, duplicate attribute keys, a pvName already registered
     * as another record's alias, or an alias already used by another record) are returned via
     * resultStatus.isError and resultStatus.msg rather than thrown.  On success, the result carries
     * the canonical pvName of the saved record.
     */
    public SavePvMetadataApiResult savePvMetadata(
            SavePvMetadataParams params
    ) {
        final SavePvMetadataRequest request = buildSavePvMetadataRequest(params);
        return sendSavePvMetadata(request);
    }

    /*
     * Parameters for saveConfiguration().  configurationName and category are required; the
     * remaining fields are optional and are omitted from the request when null.  Attributes are
     * supplied as a map, to match how the rest of this client layer accepts them, and are converted
     * to the repeated Attribute field by buildSaveConfigurationRequest().
     *
     * Because saveConfiguration() is a full-replace upsert, this record must express the complete
     * desired state of the record, not just the fields being changed.  See saveConfiguration().
     */
    public record SaveConfigurationParams(
            String configurationName,
            String category,
            String description,
            String parentConfigurationName,
            List<String> tags,
            Map<String, String> attributeMap,
            String modifiedBy
    ) {
    }

    /*
     * Parameters for saveConfigurationActivation().  configurationName and startTime are required;
     * the remaining fields are optional and are omitted from the request when null.
     *
     * clientActivationId is optional: when null or blank, the server generates an identifier and
     * returns it in the result.  endTime is optional and nullable, and omitting it produces an
     * open-ended activation interval.  Note that these two fields are "optional" by different
     * protobuf mechanisms: clientActivationId is a plain string with no field presence, so unset
     * and empty are indistinguishable on the wire, while endTime is a message field with real
     * presence.  buildSaveConfigurationActivationRequest() must therefore leave endTime entirely
     * unset rather than setting a zero-valued Timestamp, which would mark the field present and
     * describe an activation ending at the epoch.
     *
     * Because saveConfigurationActivation() is a full-replace upsert, this record must express the
     * complete desired state of the record.  See saveConfigurationActivation().
     */
    public record SaveConfigurationActivationParams(
            String clientActivationId,
            String configurationName,
            Timestamp startTime,
            Timestamp endTime,
            String description,
            List<String> tags,
            Map<String, String> attributeMap,
            String modifiedBy
    ) {
    }

    public static class SaveConfigurationResponseObserver
            extends ApiResponseObserverBase<SaveConfigurationResponse> {

        private final List<String> configurationNameList = Collections.synchronizedList(new ArrayList<>());

        @Override
        protected boolean hasExceptionalResult(SaveConfigurationResponse response) {
            return response.hasExceptionalResult();
        }

        @Override
        protected ExceptionalResult getExceptionalResult(SaveConfigurationResponse response) {
            return response.getExceptionalResult();
        }

        @Override
        protected boolean handleResult(SaveConfigurationResponse response) {
            configurationNameList.add(response.getSaveConfigurationResult().getConfigurationName());
            return true;
        }

        public String getConfigurationName() {
            if (configurationNameList.isEmpty()) {
                return null;
            } else {
                return configurationNameList.get(0);
            }
        }
    }

    public static class SaveConfigurationActivationResponseObserver
            extends ApiResponseObserverBase<SaveConfigurationActivationResponse> {

        private final List<String> clientActivationIdList = Collections.synchronizedList(new ArrayList<>());

        @Override
        protected boolean hasExceptionalResult(SaveConfigurationActivationResponse response) {
            return response.hasExceptionalResult();
        }

        @Override
        protected ExceptionalResult getExceptionalResult(SaveConfigurationActivationResponse response) {
            return response.getExceptionalResult();
        }

        @Override
        protected boolean handleResult(SaveConfigurationActivationResponse response) {
            clientActivationIdList.add(response.getSaveConfigurationActivationResult().getClientActivationId());
            return true;
        }

        public String getClientActivationId() {
            if (clientActivationIdList.isEmpty()) {
                return null;
            } else {
                return clientActivationIdList.get(0);
            }
        }
    }

    public static class GetConfigurationResponseObserver
            extends ApiResponseObserverBase<GetConfigurationResponse> {

        private final List<com.ospreydcs.dp.grpc.v1.common.Configuration> configurationList =
                Collections.synchronizedList(new ArrayList<>());

        @Override
        protected boolean hasExceptionalResult(GetConfigurationResponse response) {
            return response.hasExceptionalResult();
        }

        @Override
        protected ExceptionalResult getExceptionalResult(GetConfigurationResponse response) {
            return response.getExceptionalResult();
        }

        @Override
        protected boolean handleResult(GetConfigurationResponse response) {
            configurationList.add(response.getGetConfigurationResult().getConfiguration());
            return true;
        }

        public com.ospreydcs.dp.grpc.v1.common.Configuration getConfiguration() {
            if (configurationList.isEmpty()) {
                return null;
            } else {
                return configurationList.get(0);
            }
        }
    }

    public static SaveConfigurationRequest buildSaveConfigurationRequest(SaveConfigurationParams params) {

        final SaveConfigurationRequest.Builder requestBuilder = SaveConfigurationRequest.newBuilder();

        // handle required fields
        if (params.configurationName() != null) {
            requestBuilder.setConfigurationName(params.configurationName());
        }
        if (params.category() != null) {
            requestBuilder.setCategory(params.category());
        }

        // handle optional fields, leaving them unset when not supplied.  The server does not
        // distinguish an unset repeated or string field from an empty one, but leaving them unset
        // keeps the request minimal and matches the other builders in this class.
        if (params.description() != null) {
            requestBuilder.setDescription(params.description());
        }
        if (params.parentConfigurationName() != null) {
            // note that the server does not validate that the parent configuration exists
            requestBuilder.setParentConfigurationName(params.parentConfigurationName());
        }
        if (params.tags() != null) {
            // tags are lowercased, deduplicated and sorted server-side; no client normalization
            requestBuilder.addAllTags(params.tags());
        }
        if (params.attributeMap() != null) {
            requestBuilder.addAllAttributes(AttributesUtility.attributeListFromMap(params.attributeMap()));
        }
        if (params.modifiedBy() != null) {
            requestBuilder.setModifiedBy(params.modifiedBy());
        }

        return requestBuilder.build();
    }

    public static SaveConfigurationActivationRequest buildSaveConfigurationActivationRequest(
            SaveConfigurationActivationParams params
    ) {
        final SaveConfigurationActivationRequest.Builder requestBuilder =
                SaveConfigurationActivationRequest.newBuilder();

        // handle required fields
        if (params.configurationName() != null) {
            requestBuilder.setConfigurationName(params.configurationName());
        }
        if (params.startTime() != null) {
            requestBuilder.setStartTime(params.startTime());
        }

        // handle optional fields, leaving them unset when not supplied.
        if (params.clientActivationId() != null) {
            // the server generates an identifier when this is absent or blank
            requestBuilder.setClientActivationId(params.clientActivationId());
        }
        if (params.endTime() != null) {
            // endTime is a message field with real field presence, so it must be left entirely
            // unset for an open-ended activation; setting a zero-valued Timestamp here would mark
            // the field present and describe an activation ending at the epoch
            requestBuilder.setEndTime(params.endTime());
        }
        if (params.description() != null) {
            requestBuilder.setDescription(params.description());
        }
        if (params.tags() != null) {
            // tags are lowercased, deduplicated and sorted server-side; no client normalization
            requestBuilder.addAllTags(params.tags());
        }
        if (params.attributeMap() != null) {
            requestBuilder.addAllAttributes(AttributesUtility.attributeListFromMap(params.attributeMap()));
        }
        if (params.modifiedBy() != null) {
            requestBuilder.setModifiedBy(params.modifiedBy());
        }

        return requestBuilder.build();
    }

    public static GetConfigurationRequest buildGetConfigurationRequest(String configurationName) {

        final GetConfigurationRequest.Builder requestBuilder = GetConfigurationRequest.newBuilder();

        if (configurationName != null) {
            requestBuilder.setConfigurationName(configurationName);
        }

        return requestBuilder.build();
    }

    public SaveConfigurationApiResult sendSaveConfiguration(
            SaveConfigurationRequest request
    ) {
        final DpAnnotationServiceGrpc.DpAnnotationServiceStub asyncStub =
                DpAnnotationServiceGrpc.newStub(channel);

        final SaveConfigurationResponseObserver responseObserver = new SaveConfigurationResponseObserver();

        // send request in separate thread to better simulate out of process grpc,
        // otherwise service handles request in this thread
        new Thread(() -> {
            asyncStub.saveConfiguration(request, responseObserver);
        }).start();

        responseObserver.await();

        if (responseObserver.isError()) {
            return new SaveConfigurationApiResult(
                    true, responseObserver.getErrorMessage(), responseObserver.getApiResultStatus());
        } else {
            return new SaveConfigurationApiResult(responseObserver.getConfigurationName());
        }
    }

    /**
     * Creates or updates the machine configuration record for the specified configuration name.
     *
     * This is a full-replace upsert: category, description, parentConfigurationName, tags,
     * attributes and modifiedBy are all replaced by the values in params on every save, and fields
     * omitted from params are not preserved from an existing record.  Callers updating an existing
     * record must therefore supply the complete desired state rather than only the fields being
     * changed.
     *
     * Server-side rejections (a blank configurationName or category, duplicate attribute keys, or a
     * category change while activations exist for the configuration) are returned via
     * resultStatus.isError and resultStatus.msg rather than thrown.  On success, the result carries
     * the canonical configurationName of the saved record.
     *
     * Note that parentConfigurationName is stored without any existence check, so a reference to a
     * configuration that does not exist is accepted silently.
     */
    public SaveConfigurationApiResult saveConfiguration(
            SaveConfigurationParams params
    ) {
        final SaveConfigurationRequest request = buildSaveConfigurationRequest(params);
        return sendSaveConfiguration(request);
    }

    public SaveConfigurationActivationApiResult sendSaveConfigurationActivation(
            SaveConfigurationActivationRequest request
    ) {
        final DpAnnotationServiceGrpc.DpAnnotationServiceStub asyncStub =
                DpAnnotationServiceGrpc.newStub(channel);

        final SaveConfigurationActivationResponseObserver responseObserver =
                new SaveConfigurationActivationResponseObserver();

        // send request in separate thread to better simulate out of process grpc,
        // otherwise service handles request in this thread
        new Thread(() -> {
            asyncStub.saveConfigurationActivation(request, responseObserver);
        }).start();

        responseObserver.await();

        if (responseObserver.isError()) {
            return new SaveConfigurationActivationApiResult(
                    true, responseObserver.getErrorMessage(), responseObserver.getApiResultStatus());
        } else {
            return new SaveConfigurationActivationApiResult(responseObserver.getClientActivationId());
        }
    }

    /**
     * Creates or updates an activation recording the time interval during which a configuration was
     * active.
     *
     * This is a full-replace upsert keyed by clientActivationId: startTime, endTime, description,
     * tags, attributes and modifiedBy are all replaced by the values in params on every save, and
     * fields omitted from params are not preserved from an existing record.  Callers updating an
     * existing record must therefore supply the complete desired state rather than only the fields
     * being changed.
     *
     * When params supplies no clientActivationId, the server generates one and returns it in the
     * result; that value is the caller's only handle on the new record.  An omitted endTime
     * produces an open-ended activation.
     *
     * Server-side rejections are returned via resultStatus.isError and resultStatus.msg rather than
     * thrown.  These include a blank configurationName, a missing startTime, an endTime that is not
     * after startTime, a configurationName that does not resolve to an existing Configuration, and
     * an activation overlapping an existing one.  Note that the overlap check rejects on a matching
     * configurationName OR a matching category, since the configuration's category is denormalized
     * onto each activation, so two different configurations sharing a category cannot have
     * overlapping activations.
     */
    public SaveConfigurationActivationApiResult saveConfigurationActivation(
            SaveConfigurationActivationParams params
    ) {
        final SaveConfigurationActivationRequest request = buildSaveConfigurationActivationRequest(params);
        return sendSaveConfigurationActivation(request);
    }

    public GetConfigurationApiResult sendGetConfiguration(
            GetConfigurationRequest request
    ) {
        final DpAnnotationServiceGrpc.DpAnnotationServiceStub asyncStub =
                DpAnnotationServiceGrpc.newStub(channel);

        final GetConfigurationResponseObserver responseObserver = new GetConfigurationResponseObserver();

        // send request in separate thread to better simulate out of process grpc,
        // otherwise service handles request in this thread
        new Thread(() -> {
            asyncStub.getConfiguration(request, responseObserver);
        }).start();

        responseObserver.await();

        if (responseObserver.isError()) {
            return new GetConfigurationApiResult(
                    true, responseObserver.getErrorMessage(), responseObserver.getApiResultStatus());
        } else {
            return new GetConfigurationApiResult(responseObserver.getConfiguration());
        }
    }

    /**
     * Retrieves the machine configuration record for the specified configuration name.
     *
     * Note that a missing record is NOT reported as an empty successful result.  The server rejects
     * the request, so a name that does not exist returns resultStatus.isError == true with the
     * message "no Configuration record found for: <name>".  This is the established convention for
     * all of the single-record getters in this API: an empty collection is a normal answer, but a
     * missing singleton is a rejected request.
     *
     * The server does distinguish a rejection (RESULT_STATUS_REJECT) from a genuine failure
     * (RESULT_STATUS_ERROR), but ApiResultBase currently flattens both to resultStatus.isError, so
     * a caller using this method as an existence check cannot presently tell "does not exist" from
     * "the service is unreachable" without inspecting resultStatus.msg.  Surfacing the underlying
     * status is tracked separately.
     */
    public GetConfigurationApiResult getConfiguration(
            String configurationName
    ) {
        final GetConfigurationRequest request = buildGetConfigurationRequest(configurationName);
        return sendGetConfiguration(request);
    }

}
