package com.ospreydcs.dp.service.annotation;

import com.ospreydcs.dp.grpc.v1.annotation.CreateAnnotationRequest;
import com.ospreydcs.dp.grpc.v1.annotation.CreateAnnotationResponse;
import com.ospreydcs.dp.grpc.v1.common.Attribute;
import com.ospreydcs.dp.grpc.v1.common.CommentAnnotation;
import com.ospreydcs.dp.grpc.v1.common.Timestamp;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;

import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class AnnotationTestBase {

    public static class CreateAnnotationRequestParams {
        public final int authorId;
        public final List<String> tags;
        public final Map<String,String> attributeMap;
        public final AnnotationDataSet dataSet;
        public CreateAnnotationRequestParams(int authorId, List<String> tags, Map<String, String> attributeMap, AnnotationDataSet dataSet) {
            this.authorId = authorId;
            this.tags = tags;
            this.attributeMap = attributeMap;
            this.dataSet = dataSet;
        }
    }

    public static class AnnotationDataSet {
        public final List<AnnotationDataBlock> dataBuckets;
        public AnnotationDataSet(List<AnnotationDataBlock> dataBuckets) {
            this.dataBuckets = dataBuckets;
        }
    }

    public static class AnnotationDataBlock {
        public final long beginSeconds;
        public final long beginNanos;
        public final long endSeconds;
        public final long endNanos;
        public final List<String> pvNames;
        public AnnotationDataBlock(long beginSeconds, long beginNanos, long endSeconds, long endNanos, List<String> pvNames) {
            this.beginSeconds = beginSeconds;
            this.beginNanos = beginNanos;
            this.endSeconds = endSeconds;
            this.endNanos = endNanos;
            this.pvNames = pvNames;
        }

    }

    public static class CreateCommentAnnotationParams extends CreateAnnotationRequestParams {
        public final String comment;
        public CreateCommentAnnotationParams(
                int authorId, List<String> tags, Map<String, String> attributeMap, AnnotationDataSet dataSet, String comment) {
            super(authorId, tags, attributeMap, dataSet);
            this.comment = comment;
        }
    }

    private static CreateAnnotationRequest.Builder createAnnotationRequestBuilder(CreateAnnotationRequestParams params) {

        com.ospreydcs.dp.grpc.v1.common.DataSet.Builder dataSetBuilder
                = com.ospreydcs.dp.grpc.v1.common.DataSet.newBuilder();

        for (AnnotationDataBlock block : params.dataSet.dataBuckets) {

            Timestamp.Builder beginTimeBuilder = Timestamp.newBuilder();
            beginTimeBuilder.setEpochSeconds(block.beginSeconds);
            beginTimeBuilder.setNanoseconds(block.beginNanos);

            Timestamp.Builder endTimeBuilder = Timestamp.newBuilder();
            endTimeBuilder.setEpochSeconds(block.endSeconds);
            endTimeBuilder.setNanoseconds(block.endNanos);

            com.ospreydcs.dp.grpc.v1.common.DataBlock.Builder dataBlockBuilder
                    = com.ospreydcs.dp.grpc.v1.common.DataBlock.newBuilder();
            dataBlockBuilder.setBeginTime(beginTimeBuilder);
            dataBlockBuilder.setEndTime(endTimeBuilder);
            dataBlockBuilder.addAllPvNames(block.pvNames);
            dataBlockBuilder.build();

            dataSetBuilder.addDataBlocks(dataBlockBuilder);
        }

        dataSetBuilder.build();

        CreateAnnotationRequest.Builder requestBuilder = CreateAnnotationRequest.newBuilder();
        requestBuilder.setAuthorId(params.authorId);
        requestBuilder.addAllTags(params.tags);

        for (var attributeEntry : params.attributeMap.entrySet()) {
            final Attribute attribute = Attribute.newBuilder()
                    .setName(attributeEntry.getKey())
                    .setValue(attributeEntry.getValue())
                    .build();
            requestBuilder.addAttributes(attribute);
        }

        requestBuilder.setDataSet(dataSetBuilder);

        return requestBuilder;
    }

    public static CreateAnnotationRequest buildCreateCommentAnnotationRequest(CreateCommentAnnotationParams params) {

        CreateAnnotationRequest.Builder requestBuilder = createAnnotationRequestBuilder(params);

        CommentAnnotation.Builder commentBuilder = CommentAnnotation.newBuilder();
        commentBuilder.setComment(params.comment);
        commentBuilder.build();

        requestBuilder.setCommentAnnotation(commentBuilder);
        return requestBuilder.build();
    }

    public static class CreateAnnotationResponseObserver implements StreamObserver<CreateAnnotationResponse> {

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
        public void onNext(CreateAnnotationResponse response) {

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

                assertTrue(response.hasCreateAnnotationResult());
                final CreateAnnotationResponse.CreateAnnotationResult result = response.getCreateAnnotationResult();
                assertNotNull(result);

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

}
