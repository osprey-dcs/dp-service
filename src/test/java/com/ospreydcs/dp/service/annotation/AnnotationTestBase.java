package com.ospreydcs.dp.service.annotation;

import ch.systemsx.cisd.hdf5.IHDF5Reader;
import com.ospreydcs.dp.grpc.v1.annotation.*;
import com.ospreydcs.dp.grpc.v1.common.DataValue;
import com.ospreydcs.dp.grpc.v1.common.Timestamp;
import com.ospreydcs.dp.service.annotation.handler.mongo.export.DatasetExportCsvFile;
import com.ospreydcs.dp.service.annotation.handler.mongo.job.TabularDataExportJob;
import com.ospreydcs.dp.service.common.bson.bucket.BucketDocument;
import com.ospreydcs.dp.service.common.bson.dataset.DataBlockDocument;
import com.ospreydcs.dp.service.common.bson.dataset.DataSetDocument;
import com.ospreydcs.dp.service.common.model.TimestampDataMap;
import de.siegmar.fastcsv.reader.CsvReader;
import de.siegmar.fastcsv.reader.CsvRecord;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import org.apache.poi.openxml4j.exceptions.InvalidFormatException;
import org.apache.poi.openxml4j.opc.OPCPackage;
import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.ospreydcs.dp.service.annotation.handler.mongo.export.DatasetExportHdf5File.*;
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
        public final String name;
        public final List<String> dataSetIds;
        public final String comment;

        public CreateAnnotationRequestParams(String ownerId, String name, List<String> dataSetIds, String comment) {
            this.ownerId = ownerId;
            this.name = name;
            this.dataSetIds = dataSetIds;
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

    public static class ExportDataSetResponseObserver implements StreamObserver<ExportDataSetResponse> {

        // instance variables
        private final CountDownLatch finishLatch = new CountDownLatch(1);
        private final AtomicBoolean isError = new AtomicBoolean(false);
        private final List<String> errorMessageList = Collections.synchronizedList(new ArrayList<>());
        private final List<ExportDataSetResponse.ExportDataSetResult> resultList =
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

        public ExportDataSetResponse.ExportDataSetResult getResult() {
            if (!resultList.isEmpty()) {
                return resultList.get(0);
            } else {
                return null;
            }
        }

        @Override
        public void onNext(ExportDataSetResponse response) {

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

                assertTrue(response.hasExportDataSetResult());
                final ExportDataSetResponse.ExportDataSetResult result = response.getExportDataSetResult();
                assertNotNull(result);

                // flag error if already received a response
                if (!resultList.isEmpty()) {
                    final String errorMsg = "onNext received more than one response";
                    System.err.println(errorMsg);
                    isError.set(true);
                    errorMessageList.add(errorMsg);

                } else {
                    resultList.add(result);
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

    public static QueryDataSetsRequest buildQueryDataSetsRequest(
            String datasetId,
            String ownerId,
            String descriptionText
    ) {
        QueryDataSetsRequest.Builder requestBuilder = QueryDataSetsRequest.newBuilder();

        // add id criteria
        if (datasetId != null) {
            QueryDataSetsRequest.QueryDataSetsCriterion.IdCriterion idCriterion =
                    QueryDataSetsRequest.QueryDataSetsCriterion.IdCriterion.newBuilder()
                            .setId(datasetId)
                            .build();
            QueryDataSetsRequest.QueryDataSetsCriterion idQueryDataSetsCriterion =
                    QueryDataSetsRequest.QueryDataSetsCriterion.newBuilder()
                            .setIdCriterion(idCriterion)
                            .build();
            requestBuilder.addCriteria(idQueryDataSetsCriterion);
        }

        // add owner criteria
        if (ownerId != null) {
            QueryDataSetsRequest.QueryDataSetsCriterion.OwnerCriterion ownerCriterion =
                    QueryDataSetsRequest.QueryDataSetsCriterion.OwnerCriterion.newBuilder()
                            .setOwnerId(ownerId)
                            .build();
            QueryDataSetsRequest.QueryDataSetsCriterion ownerQueryDataSetsCriterion =
                    QueryDataSetsRequest.QueryDataSetsCriterion.newBuilder()
                            .setOwnerCriterion(ownerCriterion)
                            .build();
            requestBuilder.addCriteria(ownerQueryDataSetsCriterion);
        }

        // add description criteria
        if (descriptionText != null) {
            QueryDataSetsRequest.QueryDataSetsCriterion.DescriptionCriterion descriptionCriterion =
                    QueryDataSetsRequest.QueryDataSetsCriterion.DescriptionCriterion.newBuilder()
                            .setDescriptionText(descriptionText)
                            .build();
            QueryDataSetsRequest.QueryDataSetsCriterion descriptionQueryDataSetsCriterion =
                    QueryDataSetsRequest.QueryDataSetsCriterion.newBuilder()
                            .setDescriptionCriterion(descriptionCriterion)
                            .build();
            requestBuilder.addCriteria(descriptionQueryDataSetsCriterion);
        }

        return requestBuilder.build();
    }

    public static CreateAnnotationRequest buildCreateAnnotationRequest(CreateAnnotationRequestParams params) {

        CreateAnnotationRequest.Builder requestBuilder = CreateAnnotationRequest.newBuilder();
        requestBuilder.setOwnerId(params.ownerId);
        requestBuilder.setName(params.name);
        requestBuilder.addAllDataSetIds(params.dataSetIds);
        if (params.comment != null) {
            requestBuilder.setComment(params.comment);
        }
        return requestBuilder.build();
    }

    public static QueryAnnotationsRequest buildQueryAnnotationsRequest(
            String annotationId,
            String ownerId,
            String datasetId,
            String commentText
    ) {
        QueryAnnotationsRequest.Builder requestBuilder = QueryAnnotationsRequest.newBuilder();

        // add id criteria
        if (annotationId != null) {
            QueryAnnotationsRequest.QueryAnnotationsCriterion.IdCriterion idCriterion =
                    QueryAnnotationsRequest.QueryAnnotationsCriterion.IdCriterion.newBuilder()
                            .setId(annotationId)
                            .build();
            QueryAnnotationsRequest.QueryAnnotationsCriterion idQueryAnnotationsCriterion =
                    QueryAnnotationsRequest.QueryAnnotationsCriterion.newBuilder()
                            .setIdCriterion(idCriterion)
                            .build();
            requestBuilder.addCriteria(idQueryAnnotationsCriterion);
        }

        // add owner criteria
        if (ownerId != null) {
            QueryAnnotationsRequest.QueryAnnotationsCriterion.OwnerCriterion ownerCriterion =
                    QueryAnnotationsRequest.QueryAnnotationsCriterion.OwnerCriterion.newBuilder()
                            .setOwnerId(ownerId)
                            .build();
            QueryAnnotationsRequest.QueryAnnotationsCriterion ownerQueryAnnotationsCriterion =
                    QueryAnnotationsRequest.QueryAnnotationsCriterion.newBuilder()
                            .setOwnerCriterion(ownerCriterion)
                            .build();
            requestBuilder.addCriteria(ownerQueryAnnotationsCriterion);
        }

        // add datasetId criteria
        if (datasetId != null) {
            QueryAnnotationsRequest.QueryAnnotationsCriterion.DataSetCriterion dataSetCriterion =
                    QueryAnnotationsRequest.QueryAnnotationsCriterion.DataSetCriterion.newBuilder()
                            .setDataSetId(datasetId)
                            .build();
            QueryAnnotationsRequest.QueryAnnotationsCriterion datasetIdQueryAnnotationsCriterion =
                    QueryAnnotationsRequest.QueryAnnotationsCriterion.newBuilder()
                            .setDataSetCriterion(dataSetCriterion)
                            .build();
            requestBuilder.addCriteria(datasetIdQueryAnnotationsCriterion);
        }

        // add comment criteria
        if (commentText != null) {
            QueryAnnotationsRequest.QueryAnnotationsCriterion.CommentCriterion commentCriterion =
                    QueryAnnotationsRequest.QueryAnnotationsCriterion.CommentCriterion.newBuilder()
                            .setCommentText(commentText)
                            .build();
            QueryAnnotationsRequest.QueryAnnotationsCriterion commentQueryAnnotationsCriteria =
                    QueryAnnotationsRequest.QueryAnnotationsCriterion.newBuilder()
                            .setCommentCriterion(commentCriterion)
                            .build();
            requestBuilder.addCriteria(commentQueryAnnotationsCriteria);
        }

        return requestBuilder.build();
    }

    public static ExportDataSetRequest buildExportDataSetRequest(
            String dataSetId,
            ExportDataSetRequest.ExportOutputFormat outputFormat
    ) {
        ExportDataSetRequest.Builder requestBuilder = ExportDataSetRequest.newBuilder();
        requestBuilder.setDataSetId(dataSetId);
        requestBuilder.setOutputFormat(outputFormat);
        return requestBuilder.build();
    }

    public static void verifyDatasetHdf5Content(IHDF5Reader reader, DataSetDocument dataset) {

        // verify dataset paths
        final String datasetGroup = PATH_SEPARATOR
                + GROUP_DATASET;
        assertTrue(reader.object().isGroup(datasetGroup));

        final String dataBlocksGroup = PATH_SEPARATOR
                + GROUP_DATASET
                + PATH_SEPARATOR
                + GROUP_DATA_BLOCKS;
        assertTrue(reader.object().isGroup(dataBlocksGroup));

        // verify dataset contents
        int dataBlockIndex = 0;
        for (DataBlockDocument dataBlock : dataset.getDataBlocks()) {
            final String dataBlockIndexGroup = dataBlocksGroup
                    + PATH_SEPARATOR
                    + dataBlockIndex;
            assertTrue(reader.object().isGroup(dataBlockIndexGroup));
            final String dataBlockPathBase = dataBlockIndexGroup + PATH_SEPARATOR;
            final String pvNameListPath = dataBlockPathBase + DATASET_BLOCK_PV_NAME_LIST;
            assertArrayEquals(dataBlock.getPvNames().toArray(new String[0]), reader.readStringArray(pvNameListPath));
            final String beginTimeSecondsPath = dataBlockPathBase + DATASET_BLOCK_BEGIN_SECONDS;
            assertEquals(dataBlock.getBeginTimeSeconds(), reader.readLong(beginTimeSecondsPath));
            final String beginTimeNanosPath = dataBlockPathBase + DATASET_BLOCK_BEGIN_NANOS;
            assertEquals(dataBlock.getBeginTimeNanos(), reader.readLong(beginTimeNanosPath));
            final String endTimeSecondsPath = dataBlockPathBase + DATASET_BLOCK_END_SECONDS;
            assertEquals(dataBlock.getEndTimeSeconds(), reader.readLong(endTimeSecondsPath));
            final String endTimeNanosPath = dataBlockPathBase + DATASET_BLOCK_END_NANOS;
            assertEquals(dataBlock.getEndTimeNanos(), reader.readLong(endTimeNanosPath));
            dataBlockIndex = dataBlockIndex + 1;
        }
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
        if (reader.object().getDataSetInformation(attributeMapKeysPath).getSize() > 0) {
            assertArrayEquals(
                    bucketDocument.getAttributeMap().keySet().toArray(new String[0]),
                    reader.readStringArray(attributeMapKeysPath));
            final String attributeMapValuesPath = pvBucketPath + PATH_SEPARATOR + DATASET_ATTRIBUTE_MAP_VALUES;
            assertArrayEquals(
                    bucketDocument.getAttributeMap().values().toArray(new String[0]),
                    reader.readStringArray(attributeMapValuesPath));
        }

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

    public static void verifyCsvContentFromTimestampDataMap(
            ExportDataSetResponse.ExportDataSetResult exportResult,
            TimestampDataMap expectedDataMap
    ) {
        // open csv file and create reader
        final Path exportFilePath = Paths.get(exportResult.getFilePath());
        CsvReader<CsvRecord> csvReader = null;
        try {
            csvReader = CsvReader.builder().ofCsvRecord(exportFilePath);
        } catch (IOException e) {
            fail("IOException reading csv file " + exportResult.getFilePath() + ": " + e.getMessage());
        }
        assertNotNull(csvReader);

        final Iterator<CsvRecord> csvRecordIterator = csvReader.iterator();
        final List<String> expectedColumnNameList = expectedDataMap.getColumnNameList();
        final int expectedNumColumns = 2 + expectedColumnNameList.size();

        // verify header row
        {
            assertTrue(csvRecordIterator.hasNext());
            final CsvRecord csvRecord = csvRecordIterator.next();

            // check number of csv header columns matches expected
            assertEquals(expectedNumColumns, csvRecord.getFieldCount());

            // build list of expected column headers
            final List<String> expectedHeaderValues = new ArrayList<>();
            expectedHeaderValues.add(TabularDataExportJob.COLUMN_HEADER_SECONDS);
            expectedHeaderValues.add(TabularDataExportJob.COLUMN_HEADER_NANOS);
            expectedHeaderValues.addAll(expectedColumnNameList);

            // check content of csv header row matches expected
            final List<String> csvRowValues = csvRecord.getFields();
            assertEquals(expectedHeaderValues, csvRowValues);
        }

        // verify data rows
        {
            final TimestampDataMap.DataRowIterator expectedDataRowIterator = expectedDataMap.dataRowIterator();
            int dataRowCount = 0;
            while (csvRecordIterator.hasNext() && expectedDataRowIterator.hasNext()) {

                // read row from csv file
                final CsvRecord csvRecord = csvRecordIterator.next();
                assertEquals(expectedNumColumns, csvRecord.getFieldCount());
                final List<String> csvRowValues = csvRecord.getFields();

                // read expected row from map structure
                final TimestampDataMap.DataRow expectedDataRow = expectedDataRowIterator.next();

                // verify seconds/nanos match between file and expected
                final long csvSeconds = Long.valueOf(csvRowValues.get(0));
                final long csvNanos = Long.valueOf(csvRowValues.get(1));
                assertEquals(expectedDataRow.seconds(), csvSeconds);
                assertEquals(expectedDataRow.nanos(), csvNanos);

                // compare data values from csv file with expected
                final List<String> csvDataValues = csvRowValues.subList(2, csvRowValues.size());
                for (int columnIndex = 0; columnIndex < csvDataValues.size(); columnIndex++) {
                    final String csvDataValue = csvDataValues.get(columnIndex);
                    final DataValue expectedDataValue = expectedDataRow.dataValues().get(columnIndex);
                    assertEquals(DatasetExportCsvFile.dataValueToString(expectedDataValue), csvDataValue);
                }
                dataRowCount = dataRowCount + 1;
            }
            assertFalse(csvRecordIterator.hasNext());
            assertFalse(expectedDataRowIterator.hasNext());
            assertEquals(expectedDataMap.size(), dataRowCount);
        }
    }

    public static void verifyXlsxContentFromTimestampDataMap(
            ExportDataSetResponse.ExportDataSetResult exportResult,
            TimestampDataMap expectedDataMap
    ) {
        final List<String> expectedColumnNameList = expectedDataMap.getColumnNameList();
        final int expectedNumColumns = 2 + expectedColumnNameList.size();

        // open excel file
        OPCPackage filePackage = null;
        try {
            filePackage = OPCPackage.open(new File(exportResult.getFilePath()));
        } catch (InvalidFormatException e) {
            fail(
                    "InvalidFormatException opening package for excel file "
                            + exportResult.getFilePath() + ": "
                            + e.getMessage());
        }
        assertNotNull(filePackage);

        // open excel workbook
        XSSFWorkbook fileWorkbook = null;
        try {
            fileWorkbook = new XSSFWorkbook(filePackage);
        } catch (IOException e) {
            fail(
                    "IOException creating workbook from excel file "
                            + exportResult.getFilePath() + ": "
                            + e.getMessage());;
        }
        assertNotNull(fileWorkbook);

        // get worksheet
        Sheet fileSheet = fileWorkbook.getSheetAt(0);
        assertNotNull(fileSheet);

        final Iterator<Row> fileRowIterator = fileSheet.rowIterator();
        assertTrue(fileRowIterator.hasNext());

        // verify header row from file
        {
            final Row fileHeaderRow = fileRowIterator.next();
            assertNotNull(fileHeaderRow);
            assertEquals(expectedNumColumns, fileHeaderRow.getLastCellNum());

            // build list of expected column headers
            final List<String> expectedHeaderValues = new ArrayList<>();
            expectedHeaderValues.add(TabularDataExportJob.COLUMN_HEADER_SECONDS);
            expectedHeaderValues.add(TabularDataExportJob.COLUMN_HEADER_NANOS);
            expectedHeaderValues.addAll(expectedColumnNameList);

            for (int columnIndex = 0; columnIndex < fileHeaderRow.getLastCellNum(); columnIndex++) {
                final String expectedHeaderValue = expectedHeaderValues.get(columnIndex);
                final String fileHeaderValue = fileHeaderRow.getCell(columnIndex).getStringCellValue();
                assertEquals(expectedHeaderValue, fileHeaderValue);
            }
        }

        // verify data rows from file
        {
            final TimestampDataMap.DataRowIterator expectedDataRowIterator = expectedDataMap.dataRowIterator();
            int dataRowCount = 0;
            while (fileRowIterator.hasNext() && expectedDataRowIterator.hasNext()) {

                // read row from excel file
                final Row fileDataRow = fileRowIterator.next();
                assertEquals(expectedNumColumns, fileDataRow.getLastCellNum());

                // read expected row from map structure
                final TimestampDataMap.DataRow expectedDataRow = expectedDataRowIterator.next();

                // verify timestamp columns
                final long fileSeconds = Double.valueOf(fileDataRow.getCell(0).getNumericCellValue()).longValue();
                final long fileNanos = Double.valueOf(fileDataRow.getCell(1).getNumericCellValue()).longValue();
                assertEquals(expectedDataRow.seconds(), fileSeconds);
                assertEquals(expectedDataRow.nanos(), fileNanos);

                // verify data columns
                for (int fileColumnIndex = 2; fileColumnIndex < fileDataRow.getLastCellNum(); fileColumnIndex++) {
                    final int expectedColumnIndex = fileColumnIndex - 2; // adjust for seconds/nanos columns in file
                    final Cell fileCell = fileDataRow.getCell(fileColumnIndex);
                    final DataValue expectedDataValue = expectedDataRow.dataValues().get(expectedColumnIndex);
                    switch (expectedDataValue.getValueCase()) {
                        case STRINGVALUE -> {
                            assertEquals(expectedDataValue.getStringValue(), fileCell.getStringCellValue());
                        }
                        case BOOLEANVALUE -> {
                            assertEquals(expectedDataValue.getBooleanValue(), fileCell.getBooleanCellValue());
                        }
                        case UINTVALUE -> {
                            assertEquals(
                                    expectedDataValue.getUintValue(),
                                    Double.valueOf(fileCell.getNumericCellValue()).intValue());
                        }
                        case ULONGVALUE -> {
                            assertEquals(
                                    expectedDataValue.getUlongValue(),
                                    Double.valueOf(fileCell.getNumericCellValue()).longValue());
                        }
                        case INTVALUE -> {
                            assertEquals(
                                    expectedDataValue.getIntValue(),
                                    Double.valueOf(fileCell.getNumericCellValue()).intValue());
                        }
                        case LONGVALUE -> {
                            assertEquals(
                                    expectedDataValue.getLongValue(),
                                    Double.valueOf(fileCell.getNumericCellValue()).longValue());
                        }
                        case FLOATVALUE -> {
                            assertEquals(
                                    expectedDataValue.getFloatValue(),
                                    Double.valueOf(fileCell.getNumericCellValue()).floatValue());
                        }
                        case DOUBLEVALUE -> {
                            assertEquals(
                                    expectedDataValue.getDoubleValue(),
                                    Double.valueOf(fileCell.getNumericCellValue()).doubleValue(),
                                    0);
                        }
//            case BYTEARRAYVALUE -> {
//            }
//            case ARRAYVALUE -> {
//            }
//            case STRUCTUREVALUE -> {
//            }
//            case IMAGEVALUE -> {
//            }
//            case TIMESTAMPVALUE -> {
//            }
//            case VALUE_NOT_SET -> {
//            }
                        default -> {
                            assertEquals(expectedDataValue.toString(), fileCell.getStringCellValue());
                        }
                    }
                }
                dataRowCount = dataRowCount + 1;
            }

            assertFalse(fileRowIterator.hasNext());
            assertFalse(expectedDataRowIterator.hasNext());
            assertEquals(expectedDataMap.size(), dataRowCount);
        }

        // close excel file
        try {
            filePackage.close();
        } catch (IOException e) {
            fail(
                    "IOException closing package for excel file "
                            + exportResult.getFilePath() + ": "
                            + e.getMessage());;
        }
    }

}
