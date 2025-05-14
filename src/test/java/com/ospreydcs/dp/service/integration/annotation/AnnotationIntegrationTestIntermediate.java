package com.ospreydcs.dp.service.integration.annotation;

import com.ospreydcs.dp.service.annotation.AnnotationTestBase;
import com.ospreydcs.dp.service.common.protobuf.EventMetadataUtility;
import com.ospreydcs.dp.service.integration.GrpcIntegrationTestBase;
import org.junit.AfterClass;
import org.junit.BeforeClass;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class AnnotationIntegrationTestIntermediate extends GrpcIntegrationTestBase {

    // static variables
    protected static long startSeconds;
    protected static long startNanos;

    // constants
    private static final int INGESTION_PROVIDER_ID = 1;
    public static final String CFG_KEY_START_SECONDS = "IngestionBenchmark.startSeconds";
    public static final Long DEFAULT_START_SECONDS = 1698767462L;

    @BeforeClass
    public static void setUp() throws Exception {

        GrpcIntegrationTestBase.setUp();

        startSeconds = configMgr().getConfigLong(CFG_KEY_START_SECONDS, DEFAULT_START_SECONDS);
        startNanos = 0L;
    }

    @AfterClass
    public static void tearDown() {
        GrpcIntegrationTestBase.tearDown();
    }

    public static Map<String, IngestionStreamInfo> annotationIngestionScenario() {

        // register ingestion provider
        final String providerName = String.valueOf(INGESTION_PROVIDER_ID);
        final String providerId = registerProvider(providerName, null);

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
                                    providerId,
                                    interval,
                                    numBuckets,
                                    numSecondsPerBucket,
                                    false,
                                    true,
                                    null,
                                    null,
                                    null,
                                    null,
                                    null,
                                    null,
                                    null);
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
                                    providerId,
                                    interval,
                                    numBuckets,
                                    numSecondsPerBucket,
                                    false,
                                    true,
                                    null,
                                    null,
                                    null,
                                    null,
                                    null,
                                    null,
                                    null);
                    ingestionColumnInfoList.add(columnInfoTenths);
                }
            }

            {
                // perform ingestion for specified list of columns
                return ingestDataBidiStreamFromColumn(ingestionColumnInfoList, startSeconds, startNanos);
            }
        }
    }
    
    protected static class CreateDataSetScenarioResult {

        public final String firstHalfDataSetId;
        public final String secondHalfDataSetId;
        public final AnnotationTestBase.CreateDataSetParams firstHalfDataSetParams;
        public final AnnotationTestBase.CreateDataSetParams secondHalfDataSetParams;

        protected CreateDataSetScenarioResult(
                String firstHalfDataSetId,
                String secondHalfDataSetId,
                AnnotationTestBase.CreateDataSetParams firstHalfDataSetParams,
                AnnotationTestBase.CreateDataSetParams secondHalfDataSetParams
        ) {
            this.firstHalfDataSetId = firstHalfDataSetId;
            this.secondHalfDataSetId = secondHalfDataSetId;
            this.firstHalfDataSetParams = firstHalfDataSetParams;
            this.secondHalfDataSetParams = secondHalfDataSetParams;
        }
    }

    protected static CreateDataSetScenarioResult createDataSetScenario() {

        String firstHalfDataSetId;
        String secondHalfDataSetId;
        AnnotationTestBase.CreateDataSetParams firstHalfDataSetParams;
        AnnotationTestBase.CreateDataSetParams secondHalfDataSetParams;
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

        return new CreateDataSetScenarioResult(
                firstHalfDataSetId, secondHalfDataSetId, firstHalfDataSetParams, secondHalfDataSetParams);
    }

    protected static class CreateAnnotationScenarioResult {

        public final List<AnnotationTestBase.CreateAnnotationRequestParams> firstHalfAnnotationsOwnerCraigmcc;
        public final List<AnnotationTestBase.CreateAnnotationRequestParams> expectedQueryByIdResultAnnotations;
        public final List<AnnotationTestBase.CreateAnnotationRequestParams> expectedQueryByNameAnnotations;
        public final List<String> secondHalfAnnotationIds;
        public final String annotationIdOwnerCraigmccComment1;
        public final AnnotationTestBase.CreateAnnotationRequestParams annotationWithAllFieldsParams;

        protected CreateAnnotationScenarioResult(
                List<AnnotationTestBase.CreateAnnotationRequestParams> firstHalfAnnotationsOwnerCraigmcc,
                List<AnnotationTestBase.CreateAnnotationRequestParams> expectedQueryByIdResultAnnotations,
                List<AnnotationTestBase.CreateAnnotationRequestParams> expectedQueryByNameAnnotations,
                List<String> secondHalfAnnotationIds, String annotationIdOwnerCraigmccComment1, AnnotationTestBase.CreateAnnotationRequestParams annotationWithAllFieldsParams
        ) {
            this.firstHalfAnnotationsOwnerCraigmcc = firstHalfAnnotationsOwnerCraigmcc;
            this.expectedQueryByIdResultAnnotations = expectedQueryByIdResultAnnotations;
            this.expectedQueryByNameAnnotations = expectedQueryByNameAnnotations;
            this.secondHalfAnnotationIds = secondHalfAnnotationIds;
            this.annotationIdOwnerCraigmccComment1 = annotationIdOwnerCraigmccComment1;
            this.annotationWithAllFieldsParams = annotationWithAllFieldsParams;
        }
    }

    protected static CreateAnnotationScenarioResult createAnnotationScenario(
            String firstHalfDataSetId,
            String secondHalfDataSetId
    ) {

        final List<AnnotationTestBase.CreateAnnotationRequestParams> firstHalfAnnotationsOwnerCraigmcc = new ArrayList<>();
        final List<AnnotationTestBase.CreateAnnotationRequestParams> expectedQueryByIdResultAnnotations = new ArrayList<>();
        final List<AnnotationTestBase.CreateAnnotationRequestParams> expectedQueryByNameAnnotations = new ArrayList<>();
        final List<String> secondHalfAnnotationIds = new ArrayList<>();
        String annotationIdOwnerCraigmccComment1 = "";
        AnnotationTestBase.CreateAnnotationRequestParams annotationWithAllFieldsParams = null;
        {
            /*
             * createAnnotation() positive test
             *
             * Create annotations for two different owners, each with two different types of annotations.
             * We'll save a list of one type of annotation for one of the owners for use in verifying
             * the queryAnnotations() positive test results.
             *
             * The first set of annotations includes values for tags, attributes, and event metadata.  The second
             * set of annotations does not include those descriptive fields so that we can test both cases.
             */

            final List<String> tags = List.of("unit tests", "positive");
            final Map<String, String> attributeMap = Map.of("service", "annotation", "feature", "annotation");
            final EventMetadataUtility.EventMetadataParams eventMetadataParams =
                    new EventMetadataUtility.EventMetadataParams(
                            "unit test event",
                            startSeconds,
                            0L,
                            null,
                            null);

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
                                    tags,
                                    attributeMap,
                                    eventMetadataParams,
                                    null);
                    final String createdAnnotationId = sendAndVerifyCreateAnnotation(
                            firstHalfParams, false, "");
                    expectedQueryByNameAnnotations.add(firstHalfParams);
                    if (owner.equals("craigmcc")) {
                        firstHalfAnnotationsOwnerCraigmcc.add(firstHalfParams);
                    }
                    if (owner.equals("craigmcc") && (commentNumber == 1)) {
                        annotationIdOwnerCraigmccComment1 = createdAnnotationId;
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
                                    null,
                                    null);
                    secondHalfAnnotationIds.add(
                            sendAndVerifyCreateAnnotation(
                                    secondHalfParams, false, ""));
                }
            }
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

            final AnnotationTestBase.CreateAnnotationRequestParams params =
                    new AnnotationTestBase.CreateAnnotationRequestParams(
                            ownerId,
                            name,
                            dataSetIds,
                            annotationIds,
                            comment,
                            tags,
                            attributeMap,
                            eventMetadataParams, null);
            annotationWithAllFieldsParams = params;

            final String expectedRejectMessage = null;
            sendAndVerifyCreateAnnotation(params, false, expectedRejectMessage);
        }

        return new CreateAnnotationScenarioResult(
                firstHalfAnnotationsOwnerCraigmcc,
                expectedQueryByIdResultAnnotations,
                expectedQueryByNameAnnotations,
                secondHalfAnnotationIds,
                annotationIdOwnerCraigmccComment1,
                annotationWithAllFieldsParams);
    }

}
