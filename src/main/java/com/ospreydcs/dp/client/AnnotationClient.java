package com.ospreydcs.dp.client;

import com.ospreydcs.dp.client.result.SaveAnnotationApiResult;
import com.ospreydcs.dp.client.result.SaveDataSetApiResult;
import com.ospreydcs.dp.grpc.v1.annotation.*;
import com.ospreydcs.dp.grpc.v1.common.Timestamp;
import com.ospreydcs.dp.service.common.protobuf.AttributesUtility;
import com.ospreydcs.dp.service.common.protobuf.EventMetadataUtility;
import io.grpc.ManagedChannel;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

public class AnnotationClient extends ServiceApiClientBase {

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
    
    public static class SaveDataSetResponseObserver implements StreamObserver<SaveDataSetResponse> {

        // instance variables
        private final CountDownLatch finishLatch = new CountDownLatch(1);
        private final AtomicBoolean isError = new AtomicBoolean(false);
        private final List<String> errorMessageList = Collections.synchronizedList(new ArrayList<>());
        private final List<String> dataSetIdList = Collections.synchronizedList(new ArrayList<>());

        public void await() {
            try {
                finishLatch.await(1, TimeUnit.MINUTES);
            } catch (InterruptedException e) {
                final String errorMsg = "InterruptedException waiting for finishLatch";
                System.err.println(errorMsg);
                isError.set(true);
                errorMessageList.add(errorMsg);
            }
        }

        public boolean isError() { return isError.get(); }

        public String getErrorMessage() {
            if (!errorMessageList.isEmpty()) {
                return errorMessageList.get(0);
            } else {
                return "";
            }
        }

        public String getDataSetId() {
            if (!dataSetIdList.isEmpty()) {
                return dataSetIdList.get(0);
            } else {
                return null;
            }
        }

        @Override
        public void onNext(SaveDataSetResponse response) {

            // handle response in separate thread to better simulate out of process grpc,
            // otherwise response is handled in same thread as service handler that sent it
            new Thread(() -> {

                if (response.hasExceptionalResult()) {
                    final String errorMsg = "onNext received exceptional response: "
                            + response.getExceptionalResult().getMessage();
                    System.err.println(errorMsg);
                    isError.set(true);
                    errorMessageList.add(errorMsg);
                    finishLatch.countDown();
                    return;
                }

                final SaveDataSetResponse.SaveDataSetResult result = response.getSaveDataSetResult();

                // flag error if already received a response
                if (!dataSetIdList.isEmpty()) {
                    final String errorMsg = "onNext received more than one response";
                    System.err.println(errorMsg);
                    isError.set(true);
                    errorMessageList.add(errorMsg);

                } else {
                    dataSetIdList.add(result.getDataSetId());
                    finishLatch.countDown();
                }
            }).start();

        }

        @Override
        public void onError(Throwable t) {
            // handle response in separate thread to better simulate out of process grpc,
            // otherwise response is handled in same thread as service handler that sent it
            new Thread(() -> {
                final Status status = Status.fromThrowable(t);
                final String errorMsg = "onError error: " + status;
                System.err.println(errorMsg);
                isError.set(true);
                errorMessageList.add(errorMsg);
                finishLatch.countDown();
            }).start();
        }

        @Override
        public void onCompleted() {
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
            EventMetadataUtility.EventMetadataParams eventMetadataParams,
            Calculations calculations
    ) {
    }

    public static class SaveAnnotationResponseObserver implements StreamObserver<SaveAnnotationResponse> {

        // instance variables
        private final CountDownLatch finishLatch = new CountDownLatch(1);
        private final AtomicBoolean isError = new AtomicBoolean(false);
        private final List<String> errorMessageList = Collections.synchronizedList(new ArrayList<>());
        private final List<String> annotationIdList = Collections.synchronizedList(new ArrayList<>());

        public void await() {
            try {
                finishLatch.await(1, TimeUnit.MINUTES);
            } catch (InterruptedException e) {
                final String errorMsg = "InterruptedException waiting for finishLatch";
                System.err.println(errorMsg);
                isError.set(true);
                errorMessageList.add(errorMsg);
            }
        }

        public boolean isError() { return isError.get(); }

        public String getErrorMessage() {
            if (!errorMessageList.isEmpty()) {
                return errorMessageList.get(0);
            } else {
                return "";
            }
        }

        public String getAnnotationId() {
            if (!annotationIdList.isEmpty()) {
                return annotationIdList.get(0);
            } else {
                return null;
            }
        }

        @Override
        public void onNext(SaveAnnotationResponse response) {

            // handle response in separate thread to better simulate out of process grpc,
            // otherwise response is handled in same thread as service handler that sent it
            new Thread(() -> {

                if (response.hasExceptionalResult()) {
                    final String errorMsg = "onNext received exceptional response: "
                            + response.getExceptionalResult().getMessage();
                    System.err.println(errorMsg);
                    isError.set(true);
                    errorMessageList.add(errorMsg);
                    finishLatch.countDown();
                    return;
                }

                final SaveAnnotationResponse.SaveAnnotationResult result = response.getSaveAnnotationResult();

                // flag error if already received a response
                if (!annotationIdList.isEmpty()) {
                    final String errorMsg = "onNext received more than one response";
                    System.err.println(errorMsg);
                    isError.set(true);
                    errorMessageList.add(errorMsg);

                } else {
                    annotationIdList.add(result.getAnnotationId());
                    finishLatch.countDown();
                }
            }).start();

        }

        @Override
        public void onError(Throwable t) {
            // handle response in separate thread to better simulate out of process grpc,
            // otherwise response is handled in same thread as service handler that sent it
            new Thread(() -> {
                final Status status = Status.fromThrowable(t);
                final String errorMsg = "onError error: " + status;
                System.err.println(errorMsg);
                isError.set(true);
                errorMessageList.add(errorMsg);
                finishLatch.countDown();
            }).start();
        }

        @Override
        public void onCompleted() {
        }
    }

    // static variables
    private static final Logger logger = LogManager.getLogger();

    public AnnotationClient(ManagedChannel channel) {
        super(channel);
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
            return new SaveDataSetApiResult(true, responseObserver.getErrorMessage());
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
        if (params.eventMetadataParams != null) {
            requestBuilder.setEventMetadata(EventMetadataUtility.eventMetadataFromParams(params.eventMetadataParams));
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
            return new SaveAnnotationApiResult(true, responseObserver.getErrorMessage());
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
}
