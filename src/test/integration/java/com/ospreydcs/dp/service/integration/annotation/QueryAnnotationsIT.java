package com.ospreydcs.dp.service.integration.annotation;

import com.ospreydcs.dp.grpc.v1.annotation.DataBlock;
import com.ospreydcs.dp.grpc.v1.annotation.DataSet;
import com.ospreydcs.dp.grpc.v1.annotation.Annotation;
import com.ospreydcs.dp.grpc.v1.annotation.DpAnnotationServiceGrpc;
import com.ospreydcs.dp.grpc.v1.annotation.QueryAnnotationsRequest;
import com.ospreydcs.dp.grpc.v1.annotation.QueryAnnotationsResponse;
import com.ospreydcs.dp.grpc.v1.common.DataBucket;
import com.ospreydcs.dp.grpc.v1.common.Timestamp;
import com.ospreydcs.dp.service.annotation.AnnotationTestBase;
import com.ospreydcs.dp.service.annotation.handler.model.AnnotationQueryPageToken;
import com.ospreydcs.dp.service.query.QueryTestBase;
import org.junit.*;

import java.time.Instant;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class QueryAnnotationsIT extends AnnotationIntegrationTestIntermediate {

    @Before
    public void setUp() throws Exception {
        super.setUp();
    }

    @After
    public void tearDown() {
        super.tearDown();
    }

    @Test
    public void testQueryAnnotationsNegative() {

        // queryAnnotations() negative test: IdCriterion with an empty ids list.  Built as a raw
        // request: the params builder emits one entry per supplied value, so it cannot produce an
        // empty criterion.
        {
            final QueryAnnotationsRequest request = QueryAnnotationsRequest.newBuilder()
                    .addCriteria(QueryAnnotationsRequest.QueryAnnotationsCriterion.newBuilder()
                            .setIdCriterion(
                                    QueryAnnotationsRequest.QueryAnnotationsCriterion.IdCriterion.newBuilder()))
                    .build();

            annotationServiceWrapper.sendQueryAnnotations(
                    request,
                    true,
                    "QueryAnnotationsRequest.criteria.IdCriterion must specify at least one id");
        }

        // queryAnnotations() negative test: empty comment text in query by OwnerCriterion and TextCriterion
        {
            final String ownerId = "craigmcc";
            final String blankCommentText = "";
            final AnnotationTestBase.QueryAnnotationsParams queryParams =
                    new AnnotationTestBase.QueryAnnotationsParams();
            queryParams.setOwnerCriterion(ownerId);
            queryParams.setTextCriterion(blankCommentText);

            final boolean expectReject = true;
            final String expectedRejectMessage =
                    "QueryAnnotationsRequest.criteria.TextCriterion text must be specified";

            annotationServiceWrapper.sendAndVerifyQueryAnnotations(
                    queryParams,
                    expectReject,
                    expectedRejectMessage,
                    new ArrayList<>());
        }

        // queryAnnotations() negative test: DataSetsCriterion with an empty dataSetIds list, built
        // as a raw request for the same reason as above.
        {
            final QueryAnnotationsRequest request = QueryAnnotationsRequest.newBuilder()
                    .addCriteria(QueryAnnotationsRequest.QueryAnnotationsCriterion.newBuilder()
                            .setDataSetsCriterion(
                                    QueryAnnotationsRequest.QueryAnnotationsCriterion.DataSetsCriterion.newBuilder()))
                    .build();

            annotationServiceWrapper.sendQueryAnnotations(
                    request,
                    true,
                    "QueryAnnotationsRequest.criteria.DataSetsCriterion must specify at least one dataSetId");
        }

    }

    @Test
    public void testQueryAnnotationsPositive() {

        final long startSeconds = Instant.now().getEpochSecond();
        final long startNanos = 0L;

        // run ingestion, create datasets and annotations needed for tests
        annotationIngestionScenario(startSeconds);
        CreateDataSetScenarioResult createDataSetScenarioResult = createDataSetScenario(startSeconds);
        CreateAnnotationScenarioResult createAnnotationScenarioResult = createAnnotationScenario(
                startSeconds,
                createDataSetScenarioResult.firstHalfDataSetId(),
                createDataSetScenarioResult.secondHalfDataSetId());

        // queryAnnotations() negative test: empty query result.
        {
            final String unknownText = "JUNK";
            final AnnotationTestBase.QueryAnnotationsParams queryParams =
                    new AnnotationTestBase.QueryAnnotationsParams();
            queryParams.setTextCriterion(unknownText);

            final boolean expectReject = false;
            final String expectedRejectMessage ="";

            annotationServiceWrapper.sendAndVerifyQueryAnnotations(
                    queryParams,
                    expectReject,
                    expectedRejectMessage,
                    new ArrayList<>());
        }

        // queryAnnotations() positive test for query by OwnerCriterion and DataSetCriterion.
        {
            /*
             * This test scenario utilizes the annotations created above, which include 10 annotations for each of two
             * different owners, with 5 annotations for a dataset with blocks for the first half second of a 5 second
             * interval, and 5 annotations for the second half second of that interval.
             *
             * The queryAnnotations() test will retrieve annotations for one of the owners for the first half data set,
             * and confirm that only the appropriate 5 annotations are retrieved.
             */

            final String ownerId = "craigmcc";
            final String datasetId = createDataSetScenarioResult.firstHalfDataSetId();
            final AnnotationTestBase.QueryAnnotationsParams queryParams =
                    new AnnotationTestBase.QueryAnnotationsParams();
            queryParams.setOwnerCriterion(ownerId);
            queryParams.setDatasetsCriterion(datasetId);

            final boolean expectReject = false;
            final String expectedRejectMessage ="";

            annotationServiceWrapper.sendAndVerifyQueryAnnotations(
                    queryParams,
                    expectReject,
                    expectedRejectMessage,
                    createAnnotationScenarioResult.firstHalfAnnotationsOwnerCraigmcc());
        }

        // queryAnnotations() positive test for query by OwnerCriterion and TextCriterion (over comment field).
        List<Annotation> annotationsQueryResult = null;
        {
            /*
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

            annotationsQueryResult = annotationServiceWrapper.sendAndVerifyQueryAnnotations(
                    queryParams,
                    expectReject,
                    expectedRejectMessage,
                    createAnnotationScenarioResult.firstHalfAnnotationsOwnerCraigmcc());

            // query data test using result of queryAnnotations()
            {
                /*
                 * This test scenario uses the result from queryAnnotations to send a data query for one of the datasets.
                 * We iterate through each annoation from the query result, and send a queryDataStream() data query for each
                 * data block in the annotation's dataset, verifying that we receive the buckets expected for the specified
                 * pvNames and that each bucket has the expected begin time.
                 */

                for (Annotation resultAnnotation : annotationsQueryResult) {

                    // the annotation carries dataset references (dp-grpc #132); fetch each
                    // referenced dataset's content from the database to drive the data query
                    for (String resultDataSetId : resultAnnotation.getDataSetIdsList()) {

                        final DataSet resultDataSet = mongoClient.findDataSet(resultDataSetId).toDataSet();

                        for (DataBlock queryResultBlock : resultDataSet.getDataBlocksList()) {

                            final List<String> queryPvNames = queryResultBlock.getPvNamesList();
                            final long queryBeginSeconds = queryResultBlock.getBeginTime().getEpochSeconds();
                            final long queryBeginNanos = queryResultBlock.getBeginTime().getNanoseconds();
                            final long queryEndSeconds = queryResultBlock.getEndTime().getEpochSeconds();
                            final long queryEndNanos = queryResultBlock.getEndTime().getNanoseconds();

                            final int numBucketsExpected = 2;

                            final QueryTestBase.QueryDataRequestParams params =
                                    new QueryTestBase.QueryDataRequestParams(
                                            queryPvNames,
                                            queryBeginSeconds,
                                            queryBeginNanos,
                                            queryEndSeconds,
                                            queryEndNanos
                                    );

                            final List<DataBucket> queryResultBuckets =
                                    queryServiceWrapper.queryDataStream(params, false, "");
                            assertEquals(numBucketsExpected, queryResultBuckets.size());
                            for (String pvName : queryPvNames) {
                                boolean foundPvBucket = false;
                                DataBucket matchingResponseBucket = null;
                                for (DataBucket responseBucket : queryResultBuckets) {
                                    if (Objects.equals(pvName, responseBucket.getDataValues().getDataColumn().getName())) {
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

        // queryAnnotations() positive test for query by IdCriterion.
        {
            final AnnotationTestBase.QueryAnnotationsParams queryParams =
                    new AnnotationTestBase.QueryAnnotationsParams();
            queryParams.setIdCriterion(createAnnotationScenarioResult.annotationIdOwnerCraigmccComment1());

            final boolean expectReject = false;
            final String expectedRejectMessage ="";

            annotationServiceWrapper.sendAndVerifyQueryAnnotations(
                    queryParams,
                    expectReject,
                    expectedRejectMessage,
                    createAnnotationScenarioResult.expectedQueryByIdResultAnnotations());
        }

        // queryAnnotations() positive test for query by TextCriterion (over name field).
        {
            final String nameText = "first";
            final AnnotationTestBase.QueryAnnotationsParams queryParams =
                    new AnnotationTestBase.QueryAnnotationsParams();
            queryParams.setTextCriterion(nameText);

            final boolean expectReject = false;
            final String expectedRejectMessage ="";

            annotationServiceWrapper.sendAndVerifyQueryAnnotations(
                    queryParams,
                    expectReject,
                    expectedRejectMessage,
                    createAnnotationScenarioResult.expectedQueryByNameAnnotations());
        }

        // queryAnnotations() positive test for query by AnnotationCriterion (by id of related annotation).
        {
            final String relatedAnnotationId = createAnnotationScenarioResult.secondHalfAnnotationIds().get(0);
            final AnnotationTestBase.QueryAnnotationsParams queryParams =
                    new AnnotationTestBase.QueryAnnotationsParams();
            queryParams.setAnnotationsCriterion(relatedAnnotationId);

            final boolean expectReject = false;
            final String expectedRejectMessage ="";

            annotationServiceWrapper.sendAndVerifyQueryAnnotations(
                    queryParams,
                    expectReject,
                    expectedRejectMessage,
                    List.of(createAnnotationScenarioResult.annotationWithAllFieldsParams()));
        }

        // queryAnnotations() positive test for query by Tags (tag value).
        {
            final String tagValue = "beam loss";
            final AnnotationTestBase.QueryAnnotationsParams queryParams =
                    new AnnotationTestBase.QueryAnnotationsParams();
            queryParams.setTagsCriterion(tagValue);

            final boolean expectReject = false;
            final String expectedRejectMessage ="";

            annotationServiceWrapper.sendAndVerifyQueryAnnotations(
                    queryParams,
                    expectReject,
                    expectedRejectMessage,
                    List.of(createAnnotationScenarioResult.annotationWithAllFieldsParams()));
        }

        // queryAnnotations() positive test for query by Attributes (attribute key and value).
        {
            final String attributeKey = "sector";
            final String attributeValue = "01";
            final AnnotationTestBase.QueryAnnotationsParams queryParams =
                    new AnnotationTestBase.QueryAnnotationsParams();
            queryParams.setAttributesCriterion(attributeKey, attributeValue);

            final boolean expectReject = false;
            final String expectedRejectMessage ="";

            List<Annotation> matchingAnnotations =
                    annotationServiceWrapper.sendAndVerifyQueryAnnotations(
                            queryParams,
                            expectReject,
                            expectedRejectMessage,
                            List.of(createAnnotationScenarioResult.annotationWithAllFieldsParams()));

            // positive test for updating an annotation received in the query result
            final Annotation annotation = matchingAnnotations.get(0);
            final AnnotationTestBase.SaveAnnotationRequestParams createParams =
                    createAnnotationScenarioResult.annotationWithAllFieldsParams();
            final AnnotationTestBase.SaveAnnotationRequestParams updateParams =
                    new AnnotationTestBase.SaveAnnotationRequestParams(
                            annotation.getId(),
                            createParams.ownerId,
                            createParams.name,
                            createParams.dataSetIds,
                            createParams.annotationIds,
                            "updated annotation",
                            createParams.tags,
                            createParams.attributeMap,
                            createParams.calculations
                    );
            annotationServiceWrapper.sendAndVerifySaveAnnotation(
                    updateParams, true, false, "");
        }

    }

    /**
     * Blank or malformed criterion entries are rejected rather than silently dropped: a blank entry
     * that vanishes from a filter would turn the criterion into a match-all (#243), and a malformed
     * ObjectId would otherwise throw in the worker thread with no response ever sent.  All built as
     * raw requests: the params builder cannot produce blank entries.
     */
    @Test
    public void testQueryAnnotationsRejectBlankCriterionEntries() {

        // IdCriterion: entry that is not a parseable ObjectId (a blank entry fails the same check)
        {
            final QueryAnnotationsRequest request = QueryAnnotationsRequest.newBuilder()
                    .addCriteria(QueryAnnotationsRequest.QueryAnnotationsCriterion.newBuilder()
                            .setIdCriterion(
                                    QueryAnnotationsRequest.QueryAnnotationsCriterion.IdCriterion.newBuilder()
                                            .addIds("not-an-objectid")))
                    .build();
            annotationServiceWrapper.sendQueryAnnotations(
                    request,
                    true,
                    "QueryAnnotationsRequest.criteria.IdCriterion ids must be valid ObjectId hex strings");
        }

        // OwnerCriterion: blank entry
        {
            final QueryAnnotationsRequest request = QueryAnnotationsRequest.newBuilder()
                    .addCriteria(QueryAnnotationsRequest.QueryAnnotationsCriterion.newBuilder()
                            .setOwnerCriterion(
                                    QueryAnnotationsRequest.QueryAnnotationsCriterion.OwnerCriterion.newBuilder()
                                            .addOwnerIds("")))
                    .build();
            annotationServiceWrapper.sendQueryAnnotations(
                    request,
                    true,
                    "QueryAnnotationsRequest.criteria.OwnerCriterion ownerIds must not contain blank entries");
        }

        // DataSetsCriterion: blank entry
        {
            final QueryAnnotationsRequest request = QueryAnnotationsRequest.newBuilder()
                    .addCriteria(QueryAnnotationsRequest.QueryAnnotationsCriterion.newBuilder()
                            .setDataSetsCriterion(
                                    QueryAnnotationsRequest.QueryAnnotationsCriterion.DataSetsCriterion.newBuilder()
                                            .addDataSetIds("")))
                    .build();
            annotationServiceWrapper.sendQueryAnnotations(
                    request,
                    true,
                    "QueryAnnotationsRequest.criteria.DataSetsCriterion dataSetIds must not contain blank entries");
        }

        // AnnotationsCriterion: blank entry
        {
            final QueryAnnotationsRequest request = QueryAnnotationsRequest.newBuilder()
                    .addCriteria(QueryAnnotationsRequest.QueryAnnotationsCriterion.newBuilder()
                            .setAnnotationsCriterion(
                                    QueryAnnotationsRequest.QueryAnnotationsCriterion.AnnotationsCriterion.newBuilder()
                                            .addAnnotationIds(" ")))
                    .build();
            annotationServiceWrapper.sendQueryAnnotations(
                    request,
                    true,
                    "QueryAnnotationsRequest.criteria.AnnotationsCriterion annotationIds must not contain blank entries");
        }

        // NameCriterion: blank contains entry (would build a match-everything regex)
        {
            final QueryAnnotationsRequest request = QueryAnnotationsRequest.newBuilder()
                    .addCriteria(QueryAnnotationsRequest.QueryAnnotationsCriterion.newBuilder()
                            .setNameCriterion(
                                    QueryAnnotationsRequest.QueryAnnotationsCriterion.NameCriterion.newBuilder()
                                            .addContains("")))
                    .build();
            annotationServiceWrapper.sendQueryAnnotations(
                    request,
                    true,
                    "QueryAnnotationsRequest.criteria.NameCriterion entries must not be blank");
        }

        // TagsCriterion: blank entry
        {
            final QueryAnnotationsRequest request = QueryAnnotationsRequest.newBuilder()
                    .addCriteria(QueryAnnotationsRequest.QueryAnnotationsCriterion.newBuilder()
                            .setTagsCriterion(
                                    QueryAnnotationsRequest.QueryAnnotationsCriterion.TagsCriterion.newBuilder()
                                            .addValues("")))
                    .build();
            annotationServiceWrapper.sendQueryAnnotations(
                    request,
                    true,
                    "QueryAnnotationsRequest.criteria.TagsCriterion values must not contain blank entries");
        }

        // AttributesCriterion: blank value entry (an empty values list is a key-existence search
        // and stays legal)
        {
            final QueryAnnotationsRequest request = QueryAnnotationsRequest.newBuilder()
                    .addCriteria(QueryAnnotationsRequest.QueryAnnotationsCriterion.newBuilder()
                            .setAttributesCriterion(
                                    QueryAnnotationsRequest.QueryAnnotationsCriterion.AttributesCriterion.newBuilder()
                                            .setKey("sector")
                                            .addValues("")))
                    .build();
            annotationServiceWrapper.sendQueryAnnotations(
                    request,
                    true,
                    "QueryAnnotationsRequest.criteria.AttributesCriterion values must not contain blank entries");
        }
    }

    /**
     * An empty criteria list is match-all, not a rejection -- the #245 contract extended to
     * queryAnnotations by #248 Phase 1.
     */
    @Test
    public void testQueryAnnotationsEmptyCriteriaMatchesAll() {

        final long startSeconds = Instant.now().getEpochSecond();
        annotationIngestionScenario(startSeconds);
        final CreateDataSetScenarioResult dataSetScenario = createDataSetScenario(startSeconds);

        // save 3 annotations against the scenario's first dataset
        for (final String name : List.of("match all A", "match all B", "match all C")) {
            annotationServiceWrapper.sendAndVerifySaveAnnotation(
                    new AnnotationTestBase.SaveAnnotationRequestParams(
                            "craigmcc", name, List.of(dataSetScenario.firstHalfDataSetId())),
                    false, false, "");
        }

        final List<Annotation> resultAnnotations = annotationServiceWrapper.sendQueryAnnotations(
                QueryAnnotationsRequest.newBuilder().build(), false, null);
        assertEquals(3, resultAnnotations.size());
    }

    /**
     * Skip-token paging: limit-sized pages, a non-blank nextPageToken while more pages exist, a
     * blank one on the last page, and no document repeated or dropped across pages.
     */
    @Test
    public void testQueryAnnotationsPagination() {

        final long startSeconds = Instant.now().getEpochSecond();
        annotationIngestionScenario(startSeconds);
        final CreateDataSetScenarioResult dataSetScenario = createDataSetScenario(startSeconds);

        for (final String name : List.of("page annotation A", "page annotation B", "page annotation C")) {
            annotationServiceWrapper.sendAndVerifySaveAnnotation(
                    new AnnotationTestBase.SaveAnnotationRequestParams(
                            "craigmcc", name, List.of(dataSetScenario.firstHalfDataSetId())),
                    false, false, "");
        }

        final Set<String> seenIds = new HashSet<>();

        // page 1: limit=2, expect 2 results and a non-blank nextPageToken
        final AnnotationTestBase.QueryAnnotationsResponseObserver page1Observer =
                new AnnotationTestBase.QueryAnnotationsResponseObserver();
        final QueryAnnotationsRequest page1Request = QueryAnnotationsRequest.newBuilder()
                .setLimit(2)
                .build();
        new Thread(() -> DpAnnotationServiceGrpc
                .newStub(annotationServiceWrapper.getChannel())
                .queryAnnotations(page1Request, page1Observer)).start();
        page1Observer.await();
        assertFalse(page1Observer.getErrorMessage(), page1Observer.isError());
        assertEquals(2, page1Observer.getAnnotationsList().size());
        page1Observer.getAnnotationsList().forEach(annotation -> seenIds.add(annotation.getId()));
        final String nextPageToken = page1Observer.getNextPageToken();
        assertNotNull(nextPageToken);
        assertFalse(nextPageToken.isBlank());

        // page 2: use nextPageToken, expect 1 result and a blank nextPageToken (last page)
        final AnnotationTestBase.QueryAnnotationsResponseObserver page2Observer =
                new AnnotationTestBase.QueryAnnotationsResponseObserver();
        final QueryAnnotationsRequest page2Request = QueryAnnotationsRequest.newBuilder()
                .setLimit(2)
                .setPageToken(nextPageToken)
                .build();
        new Thread(() -> DpAnnotationServiceGrpc
                .newStub(annotationServiceWrapper.getChannel())
                .queryAnnotations(page2Request, page2Observer)).start();
        page2Observer.await();
        assertFalse(page2Observer.getErrorMessage(), page2Observer.isError());
        assertEquals(1, page2Observer.getAnnotationsList().size());
        page2Observer.getAnnotationsList().forEach(annotation -> seenIds.add(annotation.getId()));
        assertTrue("expected blank nextPageToken on last page", page2Observer.getNextPageToken().isBlank());

        // the two pages together cover all three annotations with no repeats
        assertEquals(3, seenIds.size());
    }

    @Test
    public void testQueryAnnotationsEmitsAuditFields() {

        final long startSeconds = Instant.now().getEpochSecond();

        // ingest some data, create a dataset, and save an annotation carrying modifiedBy
        annotationIngestionScenario(startSeconds);
        final CreateDataSetScenarioResult scenarioResult = createDataSetScenario(startSeconds);

        final AnnotationTestBase.SaveAnnotationRequestParams params =
                new AnnotationTestBase.SaveAnnotationRequestParams(
                        "craigmcc", "audit emission annotation",
                        List.of(scenarioResult.firstHalfDataSetId()))
                        .withModifiedBy("operator-1");
        final String annotationId =
                annotationServiceWrapper.sendAndVerifySaveAnnotation(params, false, false, "");

        // query it back by id and verify the audit fields are emitted in query results
        final AnnotationTestBase.QueryAnnotationsParams queryParams = new AnnotationTestBase.QueryAnnotationsParams();
        queryParams.setIdCriterion(annotationId);
        final QueryAnnotationsRequest request = AnnotationTestBase.buildQueryAnnotationsRequest(queryParams);
        final List<Annotation> resultAnnotations =
                annotationServiceWrapper.sendQueryAnnotations(request, false, null);

        assertEquals(1, resultAnnotations.size());
        final Annotation resultAnnotation = resultAnnotations.get(0);
        assertEquals("operator-1", resultAnnotation.getModifiedBy());
        assertTrue(resultAnnotation.hasCreatedTime());
        assertFalse(resultAnnotation.hasUpdatedTime());

        // queryAnnotations() returns references only: calculations content stays empty
        assertFalse(resultAnnotation.hasCalculations());
    }

    /**
     * Criteria list entries combine with AND (#248 Phase 3, plan D4): a TagsCriterion and an
     * AttributesCriterion intersect.  Under the legacy two-bucket semantics these two criterion
     * types ORed, which would return every annotation matching either.
     */
    @Test
    public void testQueryAnnotationsCriteriaCombineWithAnd() {

        final long startSeconds = Instant.now().getEpochSecond();
        annotationIngestionScenario(startSeconds);
        final List<AnnotationTestBase.AnnotationDataBlock> dataBlocks = List.of(
                new AnnotationTestBase.AnnotationDataBlock(
                        startSeconds, 0L, startSeconds + 1, 0L, List.of("S01-GCC01")));
        final AnnotationTestBase.AnnotationDataSet dataSet =
                new AnnotationTestBase.AnnotationDataSet(
                        null, "and semantics dataset", "craigmcc", "for AND semantics test", dataBlocks);
        final String dataSetId = annotationServiceWrapper.sendAndVerifySaveDataSet(
                new AnnotationTestBase.SaveDataSetParams(dataSet), false, false, "");

        final String matchingAnnotationId = annotationServiceWrapper.sendAndVerifySaveAnnotation(
                new AnnotationTestBase.SaveAnnotationRequestParams(
                        null, "craigmcc", "and-both", List.of(dataSetId), null, "tag and attribute",
                        List.of("alpha"), java.util.Map.of("env", "prod"), null),
                false, false, "");
        annotationServiceWrapper.sendAndVerifySaveAnnotation(
                new AnnotationTestBase.SaveAnnotationRequestParams(
                        null, "craigmcc", "and-tag-only", List.of(dataSetId), null, "tag only",
                        List.of("alpha"), java.util.Map.of("env", "dev"), null),
                false, false, "");
        annotationServiceWrapper.sendAndVerifySaveAnnotation(
                new AnnotationTestBase.SaveAnnotationRequestParams(
                        null, "craigmcc", "and-attribute-only", List.of(dataSetId), null, "attribute only",
                        List.of("beta"), java.util.Map.of("env", "prod"), null),
                false, false, "");

        // sanity check that each criterion alone matches two annotations, so the intersection
        // below is doing the narrowing
        {
            final QueryAnnotationsRequest request = QueryAnnotationsRequest.newBuilder()
                    .addCriteria(QueryAnnotationsRequest.QueryAnnotationsCriterion.newBuilder()
                            .setTagsCriterion(
                                    QueryAnnotationsRequest.QueryAnnotationsCriterion.TagsCriterion.newBuilder()
                                            .addValues("alpha")))
                    .build();
            assertEquals(2, annotationServiceWrapper.sendQueryAnnotations(request, false, null).size());
        }
        {
            final QueryAnnotationsRequest request = QueryAnnotationsRequest.newBuilder()
                    .addCriteria(QueryAnnotationsRequest.QueryAnnotationsCriterion.newBuilder()
                            .setAttributesCriterion(
                                    QueryAnnotationsRequest.QueryAnnotationsCriterion.AttributesCriterion.newBuilder()
                                            .setKey("env")
                                            .addValues("prod")))
                    .build();
            assertEquals(2, annotationServiceWrapper.sendQueryAnnotations(request, false, null).size());
        }

        // tags AND attributes: only the annotation matching both remains
        {
            final QueryAnnotationsRequest request = QueryAnnotationsRequest.newBuilder()
                    .addCriteria(QueryAnnotationsRequest.QueryAnnotationsCriterion.newBuilder()
                            .setTagsCriterion(
                                    QueryAnnotationsRequest.QueryAnnotationsCriterion.TagsCriterion.newBuilder()
                                            .addValues("alpha")))
                    .addCriteria(QueryAnnotationsRequest.QueryAnnotationsCriterion.newBuilder()
                            .setAttributesCriterion(
                                    QueryAnnotationsRequest.QueryAnnotationsCriterion.AttributesCriterion.newBuilder()
                                            .setKey("env")
                                            .addValues("prod")))
                    .build();
            final List<Annotation> resultAnnotations =
                    annotationServiceWrapper.sendQueryAnnotations(request, false, null);
            assertEquals(1, resultAnnotations.size());
            assertEquals(matchingAnnotationId, resultAnnotations.get(0).getId());
        }
    }

    /**
     * Values within one TagsCriterion OR; separate TagsCriterion entries AND (#248 Phase 3, plan
     * D4).  The same two tag values produce different results depending on which side of that
     * line they sit.
     */
    @Test
    public void testQueryAnnotationsTagsCriteriaIntersect() {

        final long startSeconds = Instant.now().getEpochSecond();
        annotationIngestionScenario(startSeconds);
        final List<AnnotationTestBase.AnnotationDataBlock> dataBlocks = List.of(
                new AnnotationTestBase.AnnotationDataBlock(
                        startSeconds, 0L, startSeconds + 1, 0L, List.of("S01-GCC01")));
        final AnnotationTestBase.AnnotationDataSet dataSet =
                new AnnotationTestBase.AnnotationDataSet(
                        null, "tags intersect dataset", "craigmcc", "for tags intersection test", dataBlocks);
        final String dataSetId = annotationServiceWrapper.sendAndVerifySaveDataSet(
                new AnnotationTestBase.SaveDataSetParams(dataSet), false, false, "");

        final String bothTagsAnnotationId = annotationServiceWrapper.sendAndVerifySaveAnnotation(
                new AnnotationTestBase.SaveAnnotationRequestParams(
                        null, "craigmcc", "tags-both", List.of(dataSetId), null, "carries alpha and beta",
                        List.of("alpha", "beta"), null, null),
                false, false, "");
        annotationServiceWrapper.sendAndVerifySaveAnnotation(
                new AnnotationTestBase.SaveAnnotationRequestParams(
                        null, "craigmcc", "tags-alpha", List.of(dataSetId), null, "carries alpha only",
                        List.of("alpha"), null, null),
                false, false, "");
        annotationServiceWrapper.sendAndVerifySaveAnnotation(
                new AnnotationTestBase.SaveAnnotationRequestParams(
                        null, "craigmcc", "tags-beta", List.of(dataSetId), null, "carries beta only",
                        List.of("beta"), null, null),
                false, false, "");

        // one TagsCriterion listing both values ORs them: all three annotations match
        {
            final QueryAnnotationsRequest request = QueryAnnotationsRequest.newBuilder()
                    .addCriteria(QueryAnnotationsRequest.QueryAnnotationsCriterion.newBuilder()
                            .setTagsCriterion(
                                    QueryAnnotationsRequest.QueryAnnotationsCriterion.TagsCriterion.newBuilder()
                                            .addValues("alpha")
                                            .addValues("beta")))
                    .build();
            assertEquals(3, annotationServiceWrapper.sendQueryAnnotations(request, false, null).size());
        }

        // two TagsCriterion entries AND: only the annotation carrying both tags matches
        {
            final QueryAnnotationsRequest request = QueryAnnotationsRequest.newBuilder()
                    .addCriteria(QueryAnnotationsRequest.QueryAnnotationsCriterion.newBuilder()
                            .setTagsCriterion(
                                    QueryAnnotationsRequest.QueryAnnotationsCriterion.TagsCriterion.newBuilder()
                                            .addValues("alpha")))
                    .addCriteria(QueryAnnotationsRequest.QueryAnnotationsCriterion.newBuilder()
                            .setTagsCriterion(
                                    QueryAnnotationsRequest.QueryAnnotationsCriterion.TagsCriterion.newBuilder()
                                            .addValues("beta")))
                    .build();
            final List<Annotation> resultAnnotations =
                    annotationServiceWrapper.sendQueryAnnotations(request, false, null);
            assertEquals(1, resultAnnotations.size());
            assertEquals(bothTagsAnnotationId, resultAnnotations.get(0).getId());
        }
    }

    /**
     * Criteria combine with AND and Mongo permits a single $text expression per query, so a
     * second TextCriterion is a validation REJECT rather than a server error ("Too many text
     * expressions") surfacing as RESULT_STATUS_ERROR.
     */
    @Test
    public void testQueryAnnotationsRejectMultipleTextCriteria() {

        final QueryAnnotationsRequest request = QueryAnnotationsRequest.newBuilder()
                .addCriteria(QueryAnnotationsRequest.QueryAnnotationsCriterion.newBuilder()
                        .setTextCriterion(
                                QueryAnnotationsRequest.QueryAnnotationsCriterion.TextCriterion.newBuilder()
                                        .setText("first")))
                .addCriteria(QueryAnnotationsRequest.QueryAnnotationsCriterion.newBuilder()
                        .setTextCriterion(
                                QueryAnnotationsRequest.QueryAnnotationsCriterion.TextCriterion.newBuilder()
                                        .setText("second")))
                .build();
        annotationServiceWrapper.sendQueryAnnotations(
                request,
                true,
                "QueryAnnotationsRequest.criteria may contain at most one TextCriterion");
    }


    /**
     * pageToken is an opaque keyset token and a malformed one is rejected per the proto contract
     * (#248 Phase 3, plan D18/D19) — including a legacy Base64 skip offset and a token issued for
     * the other query, which would otherwise decode cleanly and silently skip results.
     */
    @Test
    public void testQueryAnnotationsRejectMalformedPageToken() {

        final String expectedRejectMessage = "QueryAnnotationsRequest.pageToken is not a valid page token";

        // not a token at all
        annotationServiceWrapper.sendQueryAnnotations(
                QueryAnnotationsRequest.newBuilder().setPageToken("not-a-valid-token").build(),
                true, expectedRejectMessage);

        // a legacy Base64 skip-offset token (the pre-Phase-3 format, still used by the metadata
        // queries) is not a valid keyset token
        final String skipOffsetToken = java.util.Base64.getEncoder()
                .encodeToString("100".getBytes(java.nio.charset.StandardCharsets.UTF_8));
        annotationServiceWrapper.sendQueryAnnotations(
                QueryAnnotationsRequest.newBuilder().setPageToken(skipOffsetToken).build(),
                true, expectedRejectMessage);

        // a queryDataSets token is rejected by the query discriminator
        final String wrongQueryToken = new AnnotationQueryPageToken(
                AnnotationQueryPageToken.QUERY_DATA_SETS, "65f0123456789abcdef01234").encode();
        annotationServiceWrapper.sendQueryAnnotations(
                QueryAnnotationsRequest.newBuilder().setPageToken(wrongQueryToken).build(),
                true, expectedRejectMessage);
    }

}
