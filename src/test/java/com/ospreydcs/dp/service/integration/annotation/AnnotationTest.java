package com.ospreydcs.dp.service.integration.annotation;

import com.ospreydcs.dp.grpc.v1.annotation.DataBlock;
import com.ospreydcs.dp.grpc.v1.annotation.ExportDataSetRequest;
import com.ospreydcs.dp.grpc.v1.annotation.ExportDataSetResponse;
import com.ospreydcs.dp.grpc.v1.annotation.QueryAnnotationsResponse;
import com.ospreydcs.dp.grpc.v1.common.Timestamp;
import com.ospreydcs.dp.grpc.v1.query.QueryDataResponse;
import com.ospreydcs.dp.service.annotation.AnnotationTestBase;
import com.ospreydcs.dp.service.common.grpc.EventMetadataUtility;
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

        // register ingestion provider
        final String providerName = String.valueOf(INGESTION_PROVIDER_ID);
        final String providerId = registerProvider(providerName, null);


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
            final String secondHalfName = "half2 second half dataset";
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
            // queryDataSets() negative test - rejected because TextCriterion is empty

            final String ownerId = "craigmcc";
            final String blankDescriptionText = "";
            final AnnotationTestBase.QueryDataSetsParams queryParams = new AnnotationTestBase.QueryDataSetsParams();
            queryParams.setOwnerCriterion(ownerId);
            queryParams.setTextCriterion(blankDescriptionText);

            final boolean expectReject = true;
            final String expectedRejectMessage =
                    "QueryDataSetsRequest.criteria.TextCriterion text must be specified";

            sendAndVerifyQueryDataSets(
                    queryParams, expectReject, expectedRejectMessage, new ArrayList<>());
        }

        {
            // queryDataSets() negative test - rejected because IdCriterion is empty

            final String blankDatasetId = "";
            final AnnotationTestBase.QueryDataSetsParams queryParams = new AnnotationTestBase.QueryDataSetsParams();
            queryParams.setIdCriterion(blankDatasetId);

            final boolean expectReject = true;
            final String expectedRejectMessage =
                    "QueryDataSetsRequest.criteria.IdCriterion id must be specified";

            sendAndVerifyQueryDataSets(
                    queryParams, expectReject, expectedRejectMessage, new ArrayList<>());
        }

        {
            /*
             * queryDataSets() positive test - query by OwnerCriterion and TextCriterion (on description field)
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
            final AnnotationTestBase.QueryDataSetsParams queryParams = new AnnotationTestBase.QueryDataSetsParams();
            queryParams.setOwnerCriterion(ownerId);
            queryParams.setTextCriterion(descriptionText);

            final boolean expectReject = false;
            final String expectedRejectMessage ="";

            List<AnnotationTestBase.CreateDataSetParams> expectedQueryResultDataSets = List.of(firstHalfDataSetParams);
            sendAndVerifyQueryDataSets(
                    queryParams, expectReject, expectedRejectMessage, expectedQueryResultDataSets);
        }

        {
            /*
             * queryDataSets() positive test - query by IdCriterion
             */

            final String datasetId = firstHalfDataSetId;
            final AnnotationTestBase.QueryDataSetsParams queryParams = new AnnotationTestBase.QueryDataSetsParams();
            queryParams.setIdCriterion(firstHalfDataSetId);

            final boolean expectReject = false;
            final String expectedRejectMessage ="";

            List<AnnotationTestBase.CreateDataSetParams> expectedQueryResultDataSets = List.of(firstHalfDataSetParams);
            sendAndVerifyQueryDataSets(
                    queryParams, expectReject, expectedRejectMessage, expectedQueryResultDataSets);
        }

        {
            /*
             * queryDataSets() positive test - query by TextCriterion (on name field)
             */

            final String datasetName = "half2";
            final AnnotationTestBase.QueryDataSetsParams queryParams = new AnnotationTestBase.QueryDataSetsParams();
            queryParams.setTextCriterion(datasetName);

            final boolean expectReject = false;
            final String expectedRejectMessage ="";

            List<AnnotationTestBase.CreateDataSetParams> expectedQueryResultDataSets = List.of(secondHalfDataSetParams);
            sendAndVerifyQueryDataSets(
                    queryParams, expectReject, expectedRejectMessage, expectedQueryResultDataSets);
        }

        {
            // createAnnotation() negative test - request should be rejected because ownerId is not specified.

            final String unspecifiedOwnerId = "";
            final String dataSetId = firstHalfDataSetId;
            final String name = "craigmcc negative test unspecified ownerId";
            AnnotationTestBase.CreateAnnotationRequestParams params =
                    new AnnotationTestBase.CreateAnnotationRequestParams(unspecifiedOwnerId, name, List.of(dataSetId));
            final String expectedRejectMessage = "CreateAnnotationRequest.AnnotationDetails.ownerId must be specified";
            sendAndVerifyCreateAnnotation(
                    params, true, expectedRejectMessage);
        }

        {
            // createAnnotation() negative test - request should be rejected because name is not specified.

            final String ownerId = "craigmcc";
            final String dataSetId = firstHalfDataSetId;
            final String unspecifiedName = "";
            AnnotationTestBase.CreateAnnotationRequestParams params =
                    new AnnotationTestBase.CreateAnnotationRequestParams(ownerId, unspecifiedName, List.of(dataSetId));
            final String expectedRejectMessage = "CreateAnnotationRequest.AnnotationDetails.name must be specified";
            sendAndVerifyCreateAnnotation(
                    params, true, expectedRejectMessage);
        }

        {
            // createAnnotation() negative test - request should be rejected because list of dataset ids is empty.

            final String ownerId = "craigmcc";
            final String emptyDataSetId = "";
            final String name = "craigmcc negative test unspecified dataset id";
            AnnotationTestBase.CreateAnnotationRequestParams params =
                    new AnnotationTestBase.CreateAnnotationRequestParams(ownerId, name, new ArrayList<>());
            final String expectedRejectMessage = "CreateAnnotationRequest.AnnotationDetails.dataSetIds must not be empty";
            sendAndVerifyCreateAnnotation(
                    params, true, expectedRejectMessage);
        }

        {
            // createAnnotation() negative test - request should be rejected because specified dataset doesn't exist

            final String ownerId = "craigmcc";
            final String invalidDataSetId = "junk12345";
            final String name = "craigmcc negative test invalid dataset id";
            AnnotationTestBase.CreateAnnotationRequestParams params =
                    new AnnotationTestBase.CreateAnnotationRequestParams(ownerId, name, List.of(invalidDataSetId));
            final String expectedRejectMessage = "no DataSetDocument found with id";
            sendAndVerifyCreateAnnotation(
                    params, true, expectedRejectMessage);
        }

        List<AnnotationTestBase.CreateAnnotationRequestParams> expectedQueryResultAnnotations = new ArrayList<>();
        List<AnnotationTestBase.CreateAnnotationRequestParams> expectedQueryByIdResultAnnotations = new ArrayList<>();
        List<AnnotationTestBase.CreateAnnotationRequestParams> expectedQueryByNameAnnotations = new ArrayList<>();
        String annotationQueryId = "";
        List<String> secondHalfAnnotationIds = new ArrayList<>();
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
                    final String firstHalfName = firstHalfComment;
                    AnnotationTestBase.CreateAnnotationRequestParams firstHalfParams =
                            new AnnotationTestBase.CreateAnnotationRequestParams(
                                    owner,
                                    firstHalfName,
                                    List.of(firstHalfDataSetId),
                                    null,
                                    firstHalfComment,
                                    null,
                                    null,
                                    null);
                    final String createdAnnotationId = sendAndVerifyCreateAnnotation(
                            firstHalfParams, false, "");
                    expectedQueryByNameAnnotations.add(firstHalfParams);
                    if (owner.equals("craigmcc")) {
                        expectedQueryResultAnnotations.add(firstHalfParams);
                    }
                    if (owner.equals("craigmcc") && (commentNumber == 1)) {
                        annotationQueryId = createdAnnotationId;
                        expectedQueryByIdResultAnnotations.add(firstHalfParams);
                    }

                    // create annotation for second half data set
                    final String secondHalfComment = secondHalfBase + commentNumber;
                    final String secondHalfName = secondHalfComment;
                    AnnotationTestBase.CreateAnnotationRequestParams secondHalfParams =
                            new AnnotationTestBase.CreateAnnotationRequestParams(
                                    owner,
                                    secondHalfName,
                                    List.of(secondHalfDataSetId),
                                    null,
                                    secondHalfComment,
                                    null,
                                    null,
                                    null);
                    secondHalfAnnotationIds.add(
                            sendAndVerifyCreateAnnotation(
                                    secondHalfParams, false, ""));
                }
            }
        }

        {
            // createAnnotation() negative test - request includes an invalid associated annotation id

            final String ownerId = "craigmcc";
            final List<String> dataSetIds = List.of(secondHalfDataSetId);
            final String name = "craigmcc negative test case with invalid annotation id";
            final List<String> annotationIds = List.of("junk12345");
            final String comment = "This negative test case covers an annotation that specifies an invalid associated annotation id.";
            final List<String> tags = List.of("beam loss", "outage");
            final Map<String, String> attributeMap = Map.of("sector", "01", "subsystem", "vacuum");
            final EventMetadataUtility.EventMetadataParams eventMetadataParams =
                    new EventMetadataUtility.EventMetadataParams(
                            "experiment 1234",
                            startSeconds,
                            0L,
                            startSeconds+60,
                            999_000_000L);

            AnnotationTestBase.CreateAnnotationRequestParams params =
                    new AnnotationTestBase.CreateAnnotationRequestParams(
                            ownerId,
                            name,
                            dataSetIds,
                            annotationIds,
                            comment,
                            tags,
                            attributeMap,
                            eventMetadataParams);

            final boolean expectReject = true;
            final String expectedRejectMessage = "no AnnotationDocument found with id: junk12345";
            sendAndVerifyCreateAnnotation(
                    params, expectReject, expectedRejectMessage);
        }

        {
            // createAnnotation() positive test - request includes all required and optional annotation fields

            final String ownerId = "craigmcc";
            final List<String> dataSetIds = List.of(secondHalfDataSetId);
            final String name = "craigmcc positive test case with all fields";
            final List<String> annotationIds = secondHalfAnnotationIds;
            final String comment = "This positive test case covers an annotation with all required and optional fields set.";
            final List<String> tags = List.of("beam loss", "outage");
            final Map<String, String> attributeMap = Map.of("sector", "01", "subsystem", "vacuum");
            final EventMetadataUtility.EventMetadataParams eventMetadataParams =
                    new EventMetadataUtility.EventMetadataParams(
                            "experiment 1234",
                            startSeconds,
                            0L,
                            startSeconds+60,
                            999_000_000L);

            AnnotationTestBase.CreateAnnotationRequestParams params =
                    new AnnotationTestBase.CreateAnnotationRequestParams(
                            ownerId,
                            name,
                            dataSetIds,
                            annotationIds,
                            comment,
                            tags,
                            attributeMap,
                            eventMetadataParams);

            final String expectedRejectMessage = null;
            sendAndVerifyCreateAnnotation(
                    params, false, expectedRejectMessage);
        }

        {
            // queryAnnotations() negative test: empty annotationId in query by IdCriterion

            final String blankAnnotationId = "";
            final AnnotationTestBase.QueryAnnotationsParams queryParams =
                    new AnnotationTestBase.QueryAnnotationsParams();
            queryParams.setIdCriterion(blankAnnotationId);

            final boolean expectReject = true;
            final String expectedRejectMessage =
                    "QueryAnnotationsRequest.criteria.IdCriterion id must be specified";

            sendAndVerifyQueryAnnotations(
                    queryParams,
                    expectReject,
                    expectedRejectMessage,
                    new ArrayList<>());
        }

        {
            // queryAnnotations() negative test: empty comment text in query by OwnerCriterion and TextCriterion

            final String ownerId = "craigmcc";
            final String blankCommentText = "";
            final AnnotationTestBase.QueryAnnotationsParams queryParams =
                    new AnnotationTestBase.QueryAnnotationsParams();
            queryParams.setOwnerCriterion(ownerId);
            queryParams.setTextCriterion(blankCommentText);

            final boolean expectReject = true;
            final String expectedRejectMessage =
                    "QueryAnnotationsRequest.criteria.TextCriterion text must be specified";

            sendAndVerifyQueryAnnotations(
                    queryParams,
                    expectReject,
                    expectedRejectMessage,
                    new ArrayList<>());
        }

        {
            // queryAnnotations() negative test: empty datasetId in query by OwnerCriterion and DataSetCriterion.

            final String ownerId = "craigmcc";
            final String blankDatasetId = "";
            final AnnotationTestBase.QueryAnnotationsParams queryParams =
                    new AnnotationTestBase.QueryAnnotationsParams();
            queryParams.setOwnerCriterion(ownerId);
            queryParams.setDatasetCriterion(blankDatasetId);

            final boolean expectReject = true;
            final String expectedRejectMessage =
                    "QueryAnnotationsRequest.criteria.DataSetCriterion dataSetId must be specified";

            sendAndVerifyQueryAnnotations(
                    queryParams,
                    expectReject,
                    expectedRejectMessage,
                    new ArrayList<>());
        }

        {
            /*
             * queryAnnotations() positive test for query by OwnerCriterion and DataSetCriterion.
             *
             * This test scenario utilizes the annotations created above, which include 10 annotations for each of two
             * different owners, with 5 annotations for a dataset with blocks for the first half second of a 5 second
             * interval, and 5 annotations for the second half second of that interval.
             *
             * The queryAnnotations() test will retrieve annotations for one of the owners for the first half data set,
             * and confirm that only the appropriate 5 annotations are retrieved.
             */

            final String ownerId = "craigmcc";
            final String datasetId = firstHalfDataSetId;
            final AnnotationTestBase.QueryAnnotationsParams queryParams =
                    new AnnotationTestBase.QueryAnnotationsParams();
            queryParams.setOwnerCriterion(ownerId);
            queryParams.setDatasetCriterion(datasetId);

            final boolean expectReject = false;
            final String expectedRejectMessage ="";

            sendAndVerifyQueryAnnotations(
                    queryParams,
                    expectReject,
                    expectedRejectMessage,
                    expectedQueryResultAnnotations);
        }

        List<QueryAnnotationsResponse.AnnotationsResult.Annotation> annotationsQueryResult = null;
        {
            /*
             * queryAnnotations() positive test for query by OwnerCriterion and TextCriterion.
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
            final AnnotationTestBase.QueryAnnotationsParams queryParams =
                    new AnnotationTestBase.QueryAnnotationsParams();
            queryParams.setOwnerCriterion(ownerId);
            queryParams.setTextCriterion(commentText);

            final boolean expectReject = false;
            final String expectedRejectMessage ="";

            annotationsQueryResult = sendAndVerifyQueryAnnotations(
                    queryParams,
                    expectReject,
                    expectedRejectMessage,
                    expectedQueryResultAnnotations);
        }

        {
            /*
             * queryAnnotations() positive test for query by IdCriterion.
             */

            final String annotationId = annotationQueryId;
            final AnnotationTestBase.QueryAnnotationsParams queryParams =
                    new AnnotationTestBase.QueryAnnotationsParams();
            queryParams.setIdCriterion(annotationQueryId);

            final boolean expectReject = false;
            final String expectedRejectMessage ="";

            sendAndVerifyQueryAnnotations(
                    queryParams,
                    expectReject,
                    expectedRejectMessage,
                    expectedQueryByIdResultAnnotations);
        }

        {
            /*
             * queryAnnotations() positive test for query by TextCriterion.
             */

            final String nameText = "first";
            final AnnotationTestBase.QueryAnnotationsParams queryParams =
                    new AnnotationTestBase.QueryAnnotationsParams();
            queryParams.setTextCriterion(nameText);

            final boolean expectReject = false;
            final String expectedRejectMessage ="";

            sendAndVerifyQueryAnnotations(
                    queryParams,
                    expectReject,
                    expectedRejectMessage,
                    expectedQueryByNameAnnotations);
        }

        // TODO: uncomment this section after adding dataset content back to annotation
//        {
//            /*
//             * query data test using result of queryAnnotations()
//             *
//             * This test scenario uses the result from queryAnnotations to send a data query for one of the datasets.
//             * We iterate through each annoation from the query result, and send a queryDataStream() data query for each
//             * data block in the annotation's dataset, verifying that we receive the buckets expected for the specified
//             * pvNames and that each bucket has the expected begin time.
//             */
//
//            for (QueryAnnotationsResponse.AnnotationsResult.Annotation queryResultAnnotation : annotationsQueryResult) {
//
//                for (DataBlock queryResultBlock : queryResultAnnotation.getDataSet().getDataBlocksList()) {
//
//                    final List<String> queryPvNames = queryResultBlock.getPvNamesList();
//                    final long queryBeginSeconds = queryResultBlock.getBeginTime().getEpochSeconds();
//                    final long queryBeginNanos = queryResultBlock.getBeginTime().getNanoseconds();
//                    final long queryEndSeconds = queryResultBlock.getEndTime().getEpochSeconds();
//                    final long queryEndNanos = queryResultBlock.getEndTime().getNanoseconds();
//
//                    final int numBucketsExpected = 2;
//
//                    final List<QueryDataResponse.QueryData.DataBucket> queryResultBuckets =
//                            queryDataStream(queryPvNames, queryBeginSeconds, queryBeginNanos, queryEndSeconds, queryEndNanos);
//                    assertEquals(numBucketsExpected, queryResultBuckets.size());
//                    for (String pvName : queryPvNames) {
//                        boolean foundPvBucket = false;
//                        QueryDataResponse.QueryData.DataBucket matchingResponseBucket = null;
//                        for (QueryDataResponse.QueryData.DataBucket responseBucket : queryResultBuckets) {
//                            if (Objects.equals(pvName, responseBucket.getDataColumn().getName())) {
//                                foundPvBucket = true;
//                                matchingResponseBucket = responseBucket;
//                                break;
//                            }
//                        }
//                        assertTrue(foundPvBucket);
//                        final Timestamp matchingBucketTimestamp =
//                                matchingResponseBucket.getDataTimestamps().getSamplingClock().getStartTime();
//                        assertEquals(queryBeginSeconds, matchingBucketTimestamp.getEpochSeconds());
//                        assertEquals(queryBeginNanos, matchingBucketTimestamp.getNanoseconds());
//                    }
//
//                }
//            }
//        }

        {
            // export to hdf5, negative test, unspecified dataset id
            ExportDataSetResponse.ExportDataSetResult exportResult =
                    sendAndVerifyExportDataSet(
                            "",
                            ExportDataSetRequest.ExportOutputFormat.EXPORT_FORMAT_HDF5,
                            10, // expect 10 buckets (2 pvs, 5 seconds, 1 bucket per second)
                            true,
                            "ExportDataSetRequest.dataSetId must be specified");
        }

        {
            // export to hdf5, negative test, invalid dataset id
            ExportDataSetResponse.ExportDataSetResult exportResult =
                    sendAndVerifyExportDataSet(
                            "1234abcd1234abcd1234abcd",
                            ExportDataSetRequest.ExportOutputFormat.EXPORT_FORMAT_HDF5,
                            10, // expect 10 buckets (2 pvs, 5 seconds, 1 bucket per second)
                            true,
                            "Dataset with id 1234abcd1234abcd1234abcd not found");
        }

        {
            // export to hdf5, negative test, unspecified output format
            ExportDataSetResponse.ExportDataSetResult exportResult =
                    sendAndVerifyExportDataSet(
                            firstHalfDataSetId,
                            ExportDataSetRequest.ExportOutputFormat.EXPORT_FORMAT_UNSPECIFIED,
                            10, // expect 10 buckets (2 pvs, 5 seconds, 1 bucket per second)
                            true,
                            "valid ExportDataSetRequest.outputFormat must be specified");
        }

        {
            // export to hdf5, positive test
            ExportDataSetResponse.ExportDataSetResult exportResult =
                    sendAndVerifyExportDataSet(
                            firstHalfDataSetId,
                            ExportDataSetRequest.ExportOutputFormat.EXPORT_FORMAT_HDF5,
                            10, // expect 10 buckets (2 pvs, 5 seconds, 1 bucket per second)
                            false,
                            "");
        }

        {
            // export to csv, positive test
            ExportDataSetResponse.ExportDataSetResult exportResult =
                    sendAndVerifyExportDataSet(
                            firstHalfDataSetId,
                            ExportDataSetRequest.ExportOutputFormat.EXPORT_FORMAT_CSV,
                            10, // expect 10 buckets (2 pvs, 5 seconds, 1 bucket per second)
                            false,
                            "");
        }

        {
            // export to xlsx, positive test
            ExportDataSetResponse.ExportDataSetResult exportResult =
                    sendAndVerifyExportDataSet(
                            firstHalfDataSetId,
                            ExportDataSetRequest.ExportOutputFormat.EXPORT_FORMAT_XLSX,
                            10, // expect 10 buckets (2 pvs, 5 seconds, 1 bucket per second)
                            false,
                            "");
        }

    }

}
