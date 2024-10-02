package com.ospreydcs.dp.service.annotation;

import ch.systemsx.cisd.hdf5.IHDF5Reader;
import com.ospreydcs.dp.grpc.v1.annotation.*;
import com.ospreydcs.dp.grpc.v1.common.Timestamp;
import com.ospreydcs.dp.service.common.bson.bucket.BucketDocument;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;

import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.ospreydcs.dp.service.annotation.utility.DatasetExportHdf5File.*;
import static org.junit.Assert.*;

public class AnnotationTestBase {

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

    public static class AnnotationDataSet {
        public final String name;
        public final String ownerId;
        public final String description;
        public final List<AnnotationDataBlock> dataBlocks;
        public AnnotationDataSet(
                String name, String ownerId, String description, List<AnnotationDataBlock> dataBlocks
        ) {
            this.name = name;
            this.ownerId = ownerId;
            this.description = description;
            this.dataBlocks = dataBlocks;
        }
    }

    public static class CreateDataSetParams {
        public final AnnotationDataSet dataSet;
        public CreateDataSetParams(AnnotationDataSet dataSet) {
            this.dataSet = dataSet;
        }
    }

    public static class CreateAnnotationRequestParams {
        public final String ownerId;
        public final String dataSetId;
        public CreateAnnotationRequestParams(String ownerId, String dataSetId) {
            this.ownerId = ownerId;
            this.dataSetId = dataSetId;
        }
    }

    public static class CreateCommentAnnotationParams extends CreateAnnotationRequestParams {
        public final String comment;
        public CreateCommentAnnotationParams(
                String ownerId, String dataSetId, String comment) {
            super(ownerId, dataSetId);
            this.comment = comment;
        }
    }

    public static class CreateDataSetResponseObserver implements StreamObserver<CreateDataSetResponse> {

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
        public void onNext(CreateDataSetResponse response) {

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

                assertTrue(response.hasCreateDataSetResult());
                final CreateDataSetResponse.CreateDataSetResult result = response.getCreateDataSetResult();
                assertNotNull(result);

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

    public static class QueryDataSetsResponseObserver implements StreamObserver<QueryDataSetsResponse> {

        // instance variables
        private final CountDownLatch finishLatch = new CountDownLatch(1);
        private final AtomicBoolean isError = new AtomicBoolean(false);
        private final List<String> errorMessageList = Collections.synchronizedList(new ArrayList<>());
        private final List<DataSet> dataSetsList =
                Collections.synchronizedList(new ArrayList<>());

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

        public List<DataSet> getDataSetsList() {
            return dataSetsList;
        }

        @Override
        public void onNext(QueryDataSetsResponse response) {

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

                assertTrue(response.hasDataSetsResult());
                List<DataSet> responseDataSetsList =
                        response.getDataSetsResult().getDataSetsList();

                // flag error if already received a response
                if (!dataSetsList.isEmpty()) {
                    final String errorMsg = "onNext received more than one response";
                    System.err.println(errorMsg);
                    isError.set(true);
                    errorMessageList.add(errorMsg);

                } else {
                    dataSetsList.addAll(responseDataSetsList);
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

    public static class QueryAnnotationsResponseObserver implements StreamObserver<QueryAnnotationsResponse> {

        // instance variables
        private final CountDownLatch finishLatch = new CountDownLatch(1);
        private final AtomicBoolean isError = new AtomicBoolean(false);
        private final List<String> errorMessageList = Collections.synchronizedList(new ArrayList<>());
        private final List<QueryAnnotationsResponse.AnnotationsResult.Annotation> annotationsList =
                Collections.synchronizedList(new ArrayList<>());

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

        public List<QueryAnnotationsResponse.AnnotationsResult.Annotation> getAnnotationsList() {
            return annotationsList;
        }

        @Override
        public void onNext(QueryAnnotationsResponse response) {

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

                assertTrue(response.hasAnnotationsResult());
                List<QueryAnnotationsResponse.AnnotationsResult.Annotation> responseAnnotationList =
                        response.getAnnotationsResult().getAnnotationsList();

                // flag error if already received a response
                if (!annotationsList.isEmpty()) {
                    final String errorMsg = "onNext received more than one response";
                    System.err.println(errorMsg);
                    isError.set(true);
                    errorMessageList.add(errorMsg);

                } else {
                    annotationsList.addAll(responseAnnotationList);
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

    public static CreateDataSetRequest buildCreateDataSetRequest(CreateDataSetParams params) {

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

        dataSetBuilder.setName(params.dataSet.name);
        dataSetBuilder.setDescription(params.dataSet.description);
        dataSetBuilder.setOwnerId(params.dataSet.ownerId);

        dataSetBuilder.build();

        CreateDataSetRequest.Builder requestBuilder = CreateDataSetRequest.newBuilder();
        requestBuilder.setDataSet(dataSetBuilder);

        return requestBuilder.build();
    }

    public static QueryDataSetsRequest buildQueryDataSetsRequestOwnerDescription(
            String ownerId, 
            String descriptionText
    ) {
        QueryDataSetsRequest.Builder requestBuilder = QueryDataSetsRequest.newBuilder();

        // add owner criteria
        QueryDataSetsRequest.QueryDataSetsCriterion.OwnerCriterion ownerCriterion =
                QueryDataSetsRequest.QueryDataSetsCriterion.OwnerCriterion.newBuilder()
                        .setOwnerId(ownerId)
                        .build();
        QueryDataSetsRequest.QueryDataSetsCriterion ownerQueryDataSetsCriterion =
                QueryDataSetsRequest.QueryDataSetsCriterion.newBuilder()
                        .setOwnerCriterion(ownerCriterion)
                        .build();
        requestBuilder.addCriteria(ownerQueryDataSetsCriterion);

        // add description criteria
        QueryDataSetsRequest.QueryDataSetsCriterion.DescriptionCriterion descriptionCriterion =
                QueryDataSetsRequest.QueryDataSetsCriterion.DescriptionCriterion.newBuilder()
                        .setDescriptionText(descriptionText)
                        .build();
        QueryDataSetsRequest.QueryDataSetsCriterion descriptionQueryDataSetsCriterion =
                QueryDataSetsRequest.QueryDataSetsCriterion.newBuilder()
                        .setDescriptionCriterion(descriptionCriterion)
                        .build();
        requestBuilder.addCriteria(descriptionQueryDataSetsCriterion);

        return requestBuilder.build();
    }

    private static CreateAnnotationRequest.Builder createAnnotationRequestBuilder(
            CreateAnnotationRequestParams params
    ) {
        CreateAnnotationRequest.Builder requestBuilder = CreateAnnotationRequest.newBuilder();
        requestBuilder.setOwnerId(params.ownerId);
        requestBuilder.setDataSetId(params.dataSetId);

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

    public static QueryAnnotationsRequest buildQueryAnnotationsRequestOwnerComment(String ownerId, String commentText) {

        QueryAnnotationsRequest.Builder requestBuilder = QueryAnnotationsRequest.newBuilder();

        // add owner criteria
        QueryAnnotationsRequest.QueryAnnotationsCriterion.OwnerCriterion ownerCriterion =
                QueryAnnotationsRequest.QueryAnnotationsCriterion.OwnerCriterion.newBuilder()
                        .setOwnerId(ownerId)
                        .build();
        QueryAnnotationsRequest.QueryAnnotationsCriterion ownerQueryAnnotationsCriterion =
                QueryAnnotationsRequest.QueryAnnotationsCriterion.newBuilder()
                        .setOwnerCriterion(ownerCriterion)
                        .build();
        requestBuilder.addCriteria(ownerQueryAnnotationsCriterion);

        // add comment criteria
        QueryAnnotationsRequest.QueryAnnotationsCriterion.CommentCriterion commentCriterion =
                QueryAnnotationsRequest.QueryAnnotationsCriterion.CommentCriterion.newBuilder()
                        .setCommentText(commentText)
                        .build();
        QueryAnnotationsRequest.QueryAnnotationsCriterion commentQueryAnnotationsCriteria =
                QueryAnnotationsRequest.QueryAnnotationsCriterion.newBuilder()
                        .setCommentCriterion(commentCriterion)
                        .build();
        requestBuilder.addCriteria(commentQueryAnnotationsCriteria);

        return requestBuilder.build();
    }

    public static void verifyBucketDocumentHdf5Content(IHDF5Reader reader, BucketDocument bucketDocument) {

        final String firstSecondsString = String.format("%012d", bucketDocument.getFirstSeconds());
        final String firstNanosString = String.format("%012d", bucketDocument.getFirstNanos());

        // check paths for pv index
        final String pvsPath = PATH_SEPARATOR + GROUP_PVS;
        final String pvPath = pvsPath + PATH_SEPARATOR + bucketDocument.getPvName();
        assertTrue(reader.object().isGroup(pvPath));
        final String pvBucketPath = pvPath
                + PATH_SEPARATOR
                + GROUP_TIMES
                + PATH_SEPARATOR
                + firstSecondsString
                + PATH_SEPARATOR
                + firstNanosString;
        assertTrue(reader.object().isGroup(pvBucketPath));

        // verify dataset contents accessed via pv index
        verifyBucketDocumentHdf5ContentViaPath(reader, pvBucketPath, bucketDocument);

        // check paths for time index
        final String timesPath = PATH_SEPARATOR + GROUP_TIMES;
        final String timeBucketPath = timesPath
                + PATH_SEPARATOR
                + firstSecondsString
                + PATH_SEPARATOR
                + firstNanosString
                + PATH_SEPARATOR
                + GROUP_PVS
                + PATH_SEPARATOR
                + bucketDocument.getPvName();
        assertTrue(reader.object().isGroup(timeBucketPath));

        // verify dataset contents accessed via time index
        verifyBucketDocumentHdf5ContentViaPath(reader, timeBucketPath, bucketDocument);
    }

    public static void verifyBucketDocumentHdf5ContentViaPath(
            IHDF5Reader reader,
            String pvBucketPath,
            BucketDocument bucketDocument
    ) {
        // verify dataset contents for first seconds/nanos/time
        final String firstSecondsPath = pvBucketPath + PATH_SEPARATOR + DATASET_FIRST_SECONDS;
        assertEquals(bucketDocument.getFirstSeconds(), reader.readLong(firstSecondsPath));
        final String firstNanosPath = pvBucketPath + PATH_SEPARATOR + DATASET_FIRST_NANOS;
        assertEquals(bucketDocument.getFirstNanos(), reader.readLong(firstNanosPath));
        final String firstTimePath = pvBucketPath + PATH_SEPARATOR + DATASET_FIRST_TIME;
        assertEquals(bucketDocument.getFirstTime(), reader.time().readDate(firstTimePath));

        // verify dataset contents for first seconds/nanos/time
        final String lastSecondsPath = pvBucketPath + PATH_SEPARATOR + DATASET_LAST_SECONDS;
        assertEquals(bucketDocument.getLastSeconds(), reader.readLong(lastSecondsPath));
        final String lastNanosPath = pvBucketPath + PATH_SEPARATOR + DATASET_LAST_NANOS;
        assertEquals(bucketDocument.getLastNanos(), reader.readLong(lastNanosPath));
        final String lastTimePath = pvBucketPath + PATH_SEPARATOR + DATASET_LAST_TIME;
        assertEquals(bucketDocument.getLastTime(), reader.time().readDate(lastTimePath));

        // sample period and count
        final String sampleCountPath = pvBucketPath + PATH_SEPARATOR + DATASET_SAMPLE_COUNT;
        assertEquals(bucketDocument.getSampleCount(), reader.readInt(sampleCountPath));
        final String samplePeriodPath = pvBucketPath + PATH_SEPARATOR + DATASET_SAMPLE_PERIOD;
        assertEquals(bucketDocument.getSamplePeriod(), reader.readLong(samplePeriodPath));

        // dataColumnBytes
        final String columnDataPath = pvBucketPath + PATH_SEPARATOR + DATASET_DATA_COLUMN_BYTES;
        assertArrayEquals(bucketDocument.getDataColumnBytes(), reader.readAsByteArray(columnDataPath));

        // dataTimestampsBytes
        final String dataTimestampsPath = pvBucketPath + PATH_SEPARATOR + DATASET_DATA_TIMESTAMPS_BYTES;
        assertArrayEquals(bucketDocument.getDataTimestampsBytes(), reader.readAsByteArray(dataTimestampsPath));

        // attributeMap - write keys to one array and values to another
        final String attributeMapKeysPath = pvBucketPath + PATH_SEPARATOR + DATASET_ATTRIBUTE_MAP_KEYS;
        assertArrayEquals(
                bucketDocument.getAttributeMap().keySet().toArray(new String[0]),
                reader.readStringArray(attributeMapKeysPath));
        final String attributeMapValuesPath = pvBucketPath + PATH_SEPARATOR + DATASET_ATTRIBUTE_MAP_VALUES;
        assertArrayEquals(
                bucketDocument.getAttributeMap().values().toArray(new String[0]),
                reader.readStringArray(attributeMapValuesPath));

        // eventMetadata - description, start/stop times
        final String eventMetadataDescriptionPath =
                pvBucketPath + PATH_SEPARATOR + DATASET_EVENT_METADATA_DESCRIPTION;
        assertEquals(bucketDocument.getEventMetadata().getDescription(), reader.readString(eventMetadataDescriptionPath));
        final String eventMetadataStartSecondsPath =
                pvBucketPath + PATH_SEPARATOR + DATASET_EVENT_METADATA_START_SECONDS;
        assertEquals(bucketDocument.getEventMetadata().getStartSeconds(), reader.readLong(eventMetadataStartSecondsPath));
        final String eventMetadataStartNanosPath =
                pvBucketPath + PATH_SEPARATOR + DATASET_EVENT_METADATA_START_NANOS;
        assertEquals(bucketDocument.getEventMetadata().getStartNanos(), reader.readLong(eventMetadataStartNanosPath));
        final String eventMetadataStopSecondsPath =
                pvBucketPath + PATH_SEPARATOR + DATASET_EVENT_METADATA_STOP_SECONDS;
        assertEquals(bucketDocument.getEventMetadata().getStopSeconds(), reader.readLong(eventMetadataStopSecondsPath));
        final String eventMetadataStopNanosPath =
                pvBucketPath + PATH_SEPARATOR + DATASET_EVENT_METADATA_STOP_NANOS;
        assertEquals(bucketDocument.getEventMetadata().getStopNanos(), reader.readLong(eventMetadataStopNanosPath));

        // providerId
        final String providerIdPath = pvBucketPath + PATH_SEPARATOR + DATASET_PROVIDER_ID;
        assertEquals(bucketDocument.getProviderId(), reader.readString(providerIdPath));

    }

}
