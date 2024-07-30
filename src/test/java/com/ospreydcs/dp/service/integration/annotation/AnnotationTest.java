package com.ospreydcs.dp.service.integration.annotation;

import com.ospreydcs.dp.grpc.v1.annotation.DataBlock;
import com.ospreydcs.dp.grpc.v1.annotation.QueryAnnotationsResponse;
import com.ospreydcs.dp.grpc.v1.common.Timestamp;
import com.ospreydcs.dp.grpc.v1.query.QueryDataResponse;
import com.ospreydcs.dp.service.annotation.AnnotationTestBase;
import com.ospreydcs.dp.service.integration.GrpcIntegrationTestBase;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.*;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class AnnotationTest extends GrpcIntegrationTestBase {

    // static variables
    private static final Logger logger = LogManager.getLogger();

    // constants
    private static final int INGESTION_PROVIDER_ID = 1;
    public static final String CFG_KEY_START_SECONDS = "IngestionBenchmark.startSeconds";
    public static final Long DEFAULT_START_SECONDS = 1698767462L;

    @BeforeClass
    public static void setUp() throws Exception {
        GrpcIntegrationTestBase.setUp();
    }

    @AfterClass
    public static void tearDown() {
        GrpcIntegrationTestBase.tearDown();
    }

    @Test
    public void annotationTest() {

        final long startSeconds = configMgr().getConfigLong(CFG_KEY_START_SECONDS, DEFAULT_START_SECONDS);
        final long startNanos = 0L;

        final int providerId = INGESTION_PROVIDER_ID;

        Map<String, IngestionStreamInfo> ingestionStreamInfoMap = null;
        {
            // run ingestion scenario

            final List<IngestionColumnInfo> ingestionColumnInfoList = new ArrayList<>();

            // create data for 10 sectors, each containing 3 gauges and 3 bpms
            for (int sectorIndex = 1; sectorIndex <= 10; ++sectorIndex) {
                final String sectorName = String.format("S%02d", sectorIndex);

                // create columns for 3 gccs in each sector
                for (int gccIndex = 1; gccIndex <= 3; ++gccIndex) {
                    final String gccName = sectorName + "-" + String.format("GCC%02d", gccIndex);
                    final String requestIdBase = gccName + "-";
                    final long interval = 100_000_000L;
                    final int numBuckets = 10;
                    final int numSecondsPerBucket = 1;
                    final IngestionColumnInfo columnInfoTenths =
                            new IngestionColumnInfo(
                                    gccName,
                                    requestIdBase,
                                    interval,
                                    numBuckets,
                                    numSecondsPerBucket, false);
                    ingestionColumnInfoList.add(columnInfoTenths);
                }

                // create columns for 3 bpms in each sector
                for (int bpmIndex = 1; bpmIndex <= 3; ++bpmIndex) {
                    final String bpmName = sectorName + "-" + String.format("BPM%02d", bpmIndex);
                    final String requestIdBase = bpmName + "-";
                    final long interval = 100_000_000L;
                    final int numBuckets = 10;
                    final int numSecondsPerBucket = 1;
                    final IngestionColumnInfo columnInfoTenths =
                            new IngestionColumnInfo(
                                    bpmName,
                                    requestIdBase,
                                    interval,
                                    numBuckets,
                                    numSecondsPerBucket, false);
                    ingestionColumnInfoList.add(columnInfoTenths);
                }
            }

            {
                // perform ingestion for specified list of columns
                ingestionStreamInfoMap =
                        ingestDataBidiStreamFromColumn(ingestionColumnInfoList, startSeconds, startNanos, providerId);
            }
        }

        {
            // createDataSet() negative test - request should be rejected because name not specified

            final List<AnnotationTestBase.AnnotationDataBlock> dataBlocks = new ArrayList<>();

            // create data block with pvNames that do exist in archive
            final List<String> pvNamesValid = List.of("S01-GCC01", "S02-GCC01");
            final AnnotationTestBase.AnnotationDataBlock dataBlockValid
                    = new AnnotationTestBase.AnnotationDataBlock(
                    startSeconds, startNanos, startSeconds+1, 0, pvNamesValid);
            dataBlocks.add(dataBlockValid);

            final String ownerId = "craigmcc";
            final String unspecifiedName = "";
            final String description = "reject test";
            final AnnotationTestBase.AnnotationDataSet dataSet =
                    new AnnotationTestBase.AnnotationDataSet(unspecifiedName, ownerId, description, dataBlocks);

            final AnnotationTestBase.CreateDataSetParams params =
                    new AnnotationTestBase.CreateDataSetParams(dataSet);

            sendAndVerifyCreateDataSet(
                    params, true, "DataSet name must be specified");
        }

        {
            // createDataSet() negative test - request should be rejected because some PVs don't exist in the archive

            final List<AnnotationTestBase.AnnotationDataBlock> dataBlocks = new ArrayList<>();

            // create data block with pvNames that don't exist in archive
            final List<String> pvNamesInvalid = List.of("pv1", "pv2");
            final AnnotationTestBase.AnnotationDataBlock dataBlockInvalid
                    = new AnnotationTestBase.AnnotationDataBlock(
                    startSeconds, startNanos, startSeconds+1, 0, pvNamesInvalid);
            dataBlocks.add(dataBlockInvalid);

            // create data block with pvNames that do exist in archive
            final List<String> pvNamesValid = List.of("S01-GCC01", "S02-GCC01");
            final AnnotationTestBase.AnnotationDataBlock dataBlockValid
                    = new AnnotationTestBase.AnnotationDataBlock(
                    startSeconds, startNanos, startSeconds+1, 0, pvNamesValid);
            dataBlocks.add(dataBlockValid);

            // create data block with both pvNames that do and do not exist in archive
            final List<String> pvNamesMixed = List.of("S01-BPM01", "pv3");
            final AnnotationTestBase.AnnotationDataBlock dataBlockMixed
                    = new AnnotationTestBase.AnnotationDataBlock(
                    startSeconds, startNanos, startSeconds+1, 0, pvNamesMixed);
            dataBlocks.add(dataBlockMixed);

            final String ownerId = "craigmcc";
            final String name = "missing PV test";
            final String description = "negative test, PVs don't exist in archive";
            final AnnotationTestBase.AnnotationDataSet dataSet =
                    new AnnotationTestBase.AnnotationDataSet(name, ownerId, description, dataBlocks);

            final AnnotationTestBase.CreateDataSetParams params =
                    new AnnotationTestBase.CreateDataSetParams(dataSet);

            sendAndVerifyCreateDataSet(
                    params, true, "no PV metadata found for names: [pv1, pv2, pv3]");
        }

        String firstHalfDataSetId = null;
        String secondHalfDataSetId = null;
        AnnotationTestBase.CreateDataSetParams firstHalfDataSetParams = null;
        AnnotationTestBase.CreateDataSetParams secondHalfDataSetParams = null;
        {
            /*
             * createDataSet() positive test using pvNames that exist in archive from ingestion scenario above.
             *
             * We are going to create two data sets including 5 seconds of data, one set with data blocks for the
             * first half-second of the 5 seconds and one with blocks for the second half-second.  These will be used
             * for testing createAnnotation() and queryAnnotations() later in the test.
             */

            final List<AnnotationTestBase.AnnotationDataBlock> firstHalfDataBlocks = new ArrayList<>();
            final List<AnnotationTestBase.AnnotationDataBlock> secondHalfDataBlocks = new ArrayList<>();

            // create 5 data blocks for same 2 PVs with one block per second from startSeconds
            for (int secondIndex = 0 ; secondIndex < 5 ; ++secondIndex) {

                final long currentSecond = startSeconds + secondIndex;

                // create first half data block for current second
                final List<String> firstHalfPvNames = List.of("S01-GCC01", "S01-BPM01");
                final AnnotationTestBase.AnnotationDataBlock firstHalfDataBlock = 
                        new AnnotationTestBase.AnnotationDataBlock(
                                currentSecond, 0L, currentSecond, 499_000_000L, firstHalfPvNames);
                firstHalfDataBlocks.add(firstHalfDataBlock);

                // create second half data block for current second
                final List<String> secondHalfPvNames = List.of("S02-GCC01", "S02-BPM01");
                final AnnotationTestBase.AnnotationDataBlock secondHalfDataBlock =
                        new AnnotationTestBase.AnnotationDataBlock(
                                currentSecond,
                                500_000_000L,
                                currentSecond,
                                999_000_000L,
                                secondHalfPvNames);
                secondHalfDataBlocks.add(secondHalfDataBlock);
            }

            final String ownerId = "craigmcc";

            // create data set with first half-second blocks
            final String firstHalfName = "first half dataset";
            final String firstHalfDescription = "first half-second data blocks";
            final AnnotationTestBase.AnnotationDataSet firstHalfDataSet = 
                    new AnnotationTestBase.AnnotationDataSet(
                            firstHalfName, ownerId, firstHalfDescription, firstHalfDataBlocks);
            firstHalfDataSetParams =
                    new AnnotationTestBase.CreateDataSetParams(firstHalfDataSet);
            firstHalfDataSetId =
                    sendAndVerifyCreateDataSet(firstHalfDataSetParams, false, "");
            System.out.println("created first half dataset with id: " + firstHalfDataSetId);

            // create data set with second half-second blocks
            final String secondHalfName = "second half dataset";
            final String secondHalfDescription = "second half-second data blocks";
            final AnnotationTestBase.AnnotationDataSet secondHalfDataSet =
                    new AnnotationTestBase.AnnotationDataSet(
                            secondHalfName, ownerId, secondHalfDescription, secondHalfDataBlocks);
            secondHalfDataSetParams =
                    new AnnotationTestBase.CreateDataSetParams(secondHalfDataSet);
            secondHalfDataSetId =
                    sendAndVerifyCreateDataSet(secondHalfDataSetParams, false, "");
            System.out.println("created second half dataset with id: " + secondHalfDataSetId);
        }

        {
            // queryDataSets() negative test - rejected because descriptionText is empty

            final String ownerId = "craigmcc";
            final String blankDescriptionText = "";
            final boolean expectReject = true;
            final String expectedRejectMessage =
                    "QueryDataSetsRequest.criteria.DescriptionCriterion descriptionText must be specified";
            sendAndVerifyQueryDataSetsOwnerDescription(
                    ownerId, blankDescriptionText, expectReject, expectedRejectMessage, new ArrayList<>());
        }

        {
            /*
             * queryDataSets() positive test
             *
             * This test scenario utilizes the annotations created above, which include 10 annotations for each of two
             * different owners, with 5 annotations for a dataset with blocks for the first half second of a 5 second
             * interval, and 5 annotations for the second half second of that interval.
             *
             * The queryAnnotations() test will retrieve annotations for one of the owners for the first half data set,
             * and confirm that only the appropriate 5 annotations are retrieved.
             */

            final String ownerId = "craigmcc";
            final String descriptionText = "first";
            final boolean expectReject = false;
            final String expectedRejectMessage ="";
            List<AnnotationTestBase.CreateDataSetParams> expectedQueryResultDataSets = List.of(firstHalfDataSetParams);
            sendAndVerifyQueryDataSetsOwnerDescription(
                    ownerId, descriptionText, expectReject, expectedRejectMessage, expectedQueryResultDataSets);
        }

        {
            // createAnnotation() negative test - request should be rejected because dataSetId not specified.

            final String ownerId = "craigmcc";
            final String emptyDataSetId = "";
            final String comment = "negative test case - unspecified dataset id";
            AnnotationTestBase.CreateCommentAnnotationParams params =
                    new AnnotationTestBase.CreateCommentAnnotationParams(ownerId, emptyDataSetId, comment);
            final String expectedRejectMessage = "CreateAnnotationRequest must specify dataSetId";
            sendAndVerifyCreateCommentAnnotation(
                    params, true, expectedRejectMessage);
        }

        {
            // createAnnotation() negative test - request should be rejected because specified dataset doesn't exist

            final String ownerId = "craigmcc";
            final String invalidDataSetId = "junk12345";
            final String comment = "negative test case - invalid dataset id";
            AnnotationTestBase.CreateCommentAnnotationParams params =
                    new AnnotationTestBase.CreateCommentAnnotationParams(ownerId, invalidDataSetId, comment);
            final String expectedRejectMessage = "no DataSetDocument found with id";
            sendAndVerifyCreateCommentAnnotation(
                    params, true, expectedRejectMessage);
        }

        List<AnnotationTestBase.CreateCommentAnnotationParams> expectedQueryResultAnnotations = new ArrayList<>();
        {
            /*
             * createAnnotation() positive test
             *
             * Create annotations for two different owners, each with two different types of annotations.
             * We'll save a list of one type of annotation for one of the owners for use in verifying
             * the queryAnnotations() positive test results.
             */

            final String firstHalfBase = "first half: ";
            final String secondHalfBase = "second half: ";
            for (String owner : List.of("craigmcc", "allenck")) {
                for (int commentNumber : List.of(1, 2, 3, 4, 5)) {
                    
                    // create annotation for first half data set
                    final String firstHalfComment = firstHalfBase + commentNumber;
                    AnnotationTestBase.CreateCommentAnnotationParams firstHalfParams =
                            new AnnotationTestBase.CreateCommentAnnotationParams(
                                    owner, firstHalfDataSetId, firstHalfComment);
                    sendAndVerifyCreateCommentAnnotation(
                            firstHalfParams, false, "");
                    if (owner.equals("craigmcc")) {
                        expectedQueryResultAnnotations.add(firstHalfParams);
                    }

                    // create annotation for second half data set
                    final String secondHalfComment = secondHalfBase + commentNumber;
                    AnnotationTestBase.CreateCommentAnnotationParams secondHalfParams =
                            new AnnotationTestBase.CreateCommentAnnotationParams(
                                    owner, secondHalfDataSetId, secondHalfComment);
                    sendAndVerifyCreateCommentAnnotation(
                            secondHalfParams, false, "");
                }
            }
        }

        {
            // queryAnnotations() negative test

            final String ownerId = "craigmcc";
            final String blankCommentText = "";
            final boolean expectReject = true;
            final String expectedRejectMessage =
                    "QueryAnnotationsRequest.criteria.CommentCriterion commentText must be specified";
            sendAndVerifyQueryAnnotationsOwnerComment(
                    ownerId, blankCommentText, expectReject, expectedRejectMessage, new ArrayList<>());
        }

        List<QueryAnnotationsResponse.AnnotationsResult.Annotation> annotationsQueryResult = null;
        {
            /*
             * queryAnnotations() positive test
             *
             * This test scenario utilizes the annotations created above, which include 10 annotations for each of two
             * different owners, with 5 annotations for a dataset with blocks for the first half second of a 5 second
             * interval, and 5 annotations for the second half second of that interval.
             *
             * The queryAnnotations() test will retrieve annotations for one of the owners for the first half data set,
             * and confirm that only the appropriate 5 annotations are retrieved.
             */

            final String ownerId = "craigmcc";
            final String commentText = "first";
            final boolean expectReject = false;
            final String expectedRejectMessage ="";
            annotationsQueryResult = sendAndVerifyQueryAnnotationsOwnerComment(
                    ownerId, commentText, expectReject, expectedRejectMessage, expectedQueryResultAnnotations);
        }

        {
            /*
             * query data test using result of queryAnnotations()
             *
             * This test scenario uses the result from queryAnnotations to send a data query for one of the datasets.
             * We iterate through each annoation from the query result, and send a queryDataStream() data query for each
             * data block in the annotation's dataset, verifying that we receive the buckets expected for the specified
             * pvNames and that each bucket has the expected begin time.
             */

            for (QueryAnnotationsResponse.AnnotationsResult.Annotation queryResultAnnotation : annotationsQueryResult) {

                for (DataBlock queryResultBlock : queryResultAnnotation.getDataSet().getDataBlocksList()) {

                    final List<String> queryPvNames = queryResultBlock.getPvNamesList();
                    final long queryBeginSeconds = queryResultBlock.getBeginTime().getEpochSeconds();
                    final long queryBeginNanos = queryResultBlock.getBeginTime().getNanoseconds();
                    final long queryEndSeconds = queryResultBlock.getEndTime().getEpochSeconds();
                    final long queryEndNanos = queryResultBlock.getEndTime().getNanoseconds();

                    final int numBucketsExpected = 2;

                    final List<QueryDataResponse.QueryData.DataBucket> queryResultBuckets =
                            queryDataStream(queryPvNames, queryBeginSeconds, queryBeginNanos, queryEndSeconds, queryEndNanos);
                    assertEquals(numBucketsExpected, queryResultBuckets.size());
                    for (String pvName : queryPvNames) {
                        boolean foundPvBucket = false;
                        QueryDataResponse.QueryData.DataBucket matchingResponseBucket = null;
                        for (QueryDataResponse.QueryData.DataBucket responseBucket : queryResultBuckets) {
                            if (Objects.equals(pvName, responseBucket.getDataColumn().getName())) {
                                foundPvBucket = true;
                                matchingResponseBucket = responseBucket;
                                break;
                            }
                        }
                        assertTrue(foundPvBucket);
                        final Timestamp matchingBucketTimestamp =
                                matchingResponseBucket.getDataTimestamps().getSamplingClock().getStartTime();
                        assertEquals(queryBeginSeconds, matchingBucketTimestamp.getEpochSeconds());
                        assertEquals(queryBeginNanos, matchingBucketTimestamp.getNanoseconds());
                    }

                }
            }
        }

    }

}
