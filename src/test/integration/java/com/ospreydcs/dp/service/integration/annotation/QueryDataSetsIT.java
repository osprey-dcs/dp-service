package com.ospreydcs.dp.service.integration.annotation;

import com.ospreydcs.dp.grpc.v1.annotation.DataSet;
import com.ospreydcs.dp.grpc.v1.annotation.DpAnnotationServiceGrpc;
import com.ospreydcs.dp.grpc.v1.annotation.QueryDataSetsRequest;
import com.ospreydcs.dp.service.annotation.AnnotationTestBase;
import org.junit.*;

import java.time.Instant;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class QueryDataSetsIT extends AnnotationIntegrationTestIntermediate {

    @Before
    public void setUp() throws Exception {
        super.setUp();
    }

    @After
    public void tearDown() {
        super.tearDown();
    }

    @Test
    public void testQueryDataSetsNegative() {

        // queryDataSets() negative test - rejected because TextCriterion is empty
        {
            final String ownerId = "craigmcc";
            final String blankDescriptionText = "";
            final AnnotationTestBase.QueryDataSetsParams queryParams = new AnnotationTestBase.QueryDataSetsParams();
            queryParams.setOwnerCriterion(ownerId);
            queryParams.setTextCriterion(blankDescriptionText);

            final boolean expectReject = true;
            final String expectedRejectMessage =
                    "QueryDataSetsRequest.criteria.TextCriterion text must be specified";

            annotationServiceWrapper.sendAndVerifyQueryDataSets(
                    queryParams, expectReject, expectedRejectMessage, new ArrayList<>());
        }

        // queryDataSets() negative test - rejected because the IdCriterion ids list is empty.
        // Built as a raw request: the params builder emits one entry per supplied value, so it
        // cannot produce an empty criterion.
        {
            final QueryDataSetsRequest request = QueryDataSetsRequest.newBuilder()
                    .addCriteria(QueryDataSetsRequest.QueryDataSetsCriterion.newBuilder()
                            .setIdCriterion(
                                    QueryDataSetsRequest.QueryDataSetsCriterion.IdCriterion.newBuilder()))
                    .build();

            annotationServiceWrapper.sendQueryDataSets(
                    request,
                    true,
                    "QueryDataSetsRequest.criteria.IdCriterion must specify at least one id");
        }

        // queryDataSets() negative test - rejected because the PvNameCriterion names list is empty,
        // built as a raw request for the same reason as above.
        {
            final QueryDataSetsRequest request = QueryDataSetsRequest.newBuilder()
                    .addCriteria(QueryDataSetsRequest.QueryDataSetsCriterion.newBuilder()
                            .setPvNameCriterion(
                                    QueryDataSetsRequest.QueryDataSetsCriterion.PvNameCriterion.newBuilder()))
                    .build();

            annotationServiceWrapper.sendQueryDataSets(
                    request,
                    true,
                    "QueryDataSetsRequest.criteria.PvNameCriterion must specify at least one name");
        }

    }

    @Test
    public void testQueryDataSetsPositive() {

        final long startSeconds = Instant.now().getEpochSecond();
        final long startNanos = 0L;

        // ingest some data
        annotationIngestionScenario(startSeconds);

        // create some datasets
        CreateDataSetScenarioResult createDataSetScenarioResult = createDataSetScenario(startSeconds);

        // queryDataSets() positive test - query by OwnerCriterion and TextCriterion (on description field)
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
            final String descriptionText = "first";
            final AnnotationTestBase.QueryDataSetsParams queryParams = new AnnotationTestBase.QueryDataSetsParams();
            queryParams.setOwnerCriterion(ownerId);
            queryParams.setTextCriterion(descriptionText);

            final boolean expectReject = false;
            final String expectedRejectMessage ="";

            List<AnnotationTestBase.SaveDataSetParams> expectedQueryResultDataSets =
                    List.of(createDataSetScenarioResult.firstHalfDataSetParams());
            annotationServiceWrapper.sendAndVerifyQueryDataSets(
                    queryParams, expectReject, expectedRejectMessage, expectedQueryResultDataSets);
        }

        // queryDataSets() positive test - empty query result
        {
            final String unknownPvName = "JUNK";
            final AnnotationTestBase.QueryDataSetsParams queryParams = new AnnotationTestBase.QueryDataSetsParams();
            queryParams.setPvNameCriterion(unknownPvName);

            final boolean expectReject = false;
            final String expectedRejectMessage ="";

            annotationServiceWrapper.sendAndVerifyQueryDataSets(
                    queryParams, expectReject, expectedRejectMessage, new ArrayList<>());
        }

        // queryDataSets() positive test - query by IdCriterion
        {
            final String datasetId = createDataSetScenarioResult.firstHalfDataSetId();
            final AnnotationTestBase.QueryDataSetsParams queryParams = new AnnotationTestBase.QueryDataSetsParams();
            queryParams.setIdCriterion(createDataSetScenarioResult.firstHalfDataSetId());

            final boolean expectReject = false;
            final String expectedRejectMessage ="";

            List<AnnotationTestBase.SaveDataSetParams> expectedQueryResultDataSets =
                    List.of(createDataSetScenarioResult.firstHalfDataSetParams());
            annotationServiceWrapper.sendAndVerifyQueryDataSets(
                    queryParams, expectReject, expectedRejectMessage, expectedQueryResultDataSets);
        }

        // queryDataSets() positive test - query by TextCriterion (on name field)
        {
            final String datasetName = "half2";
            final AnnotationTestBase.QueryDataSetsParams queryParams = new AnnotationTestBase.QueryDataSetsParams();
            queryParams.setTextCriterion(datasetName);

            final boolean expectReject = false;
            final String expectedRejectMessage ="";

            List<AnnotationTestBase.SaveDataSetParams> expectedQueryResultDataSets =
                    List.of(createDataSetScenarioResult.secondHalfDataSetParams());
            annotationServiceWrapper.sendAndVerifyQueryDataSets(
                    queryParams, expectReject, expectedRejectMessage, expectedQueryResultDataSets);
        }

        // queryDataSets() positive test - query by PvNameCriterion (on data block pvNames field)
        {
            final String pvName = "S01-GCC01";
            final AnnotationTestBase.QueryDataSetsParams queryParams = new AnnotationTestBase.QueryDataSetsParams();
            queryParams.setPvNameCriterion(pvName);

            final boolean expectReject = false;
            final String expectedRejectMessage ="";

            List<AnnotationTestBase.SaveDataSetParams> expectedQueryResultDataSets =
                    List.of(createDataSetScenarioResult.firstHalfDataSetParams());
            final List<DataSet> matchingDatasets = annotationServiceWrapper.sendAndVerifyQueryDataSets(
                    queryParams, expectReject, expectedRejectMessage, expectedQueryResultDataSets);

            // positive test for updating dataset returned by query
            final DataSet dataset = matchingDatasets.get(0);
            final AnnotationTestBase.SaveDataSetParams createParams =
                    createDataSetScenarioResult.firstHalfDataSetParams();
            final AnnotationTestBase.AnnotationDataSet createDataset = createParams.dataSet();
            final AnnotationTestBase.AnnotationDataSet updateDataset =
                    new AnnotationTestBase.AnnotationDataSet(
                            dataset.getId(),
                            createDataset.name(),
                            createDataset.ownerId(),
                            "updated description",
                            createDataset.dataBlocks());
            final AnnotationTestBase.SaveDataSetParams updateParams =
                    new AnnotationTestBase.SaveDataSetParams(updateDataset);
            annotationServiceWrapper.sendAndVerifySaveDataSet(
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
    public void testQueryDataSetsRejectBlankCriterionEntries() {

        // IdCriterion: entry that is not a parseable ObjectId (a blank entry fails the same check)
        {
            final QueryDataSetsRequest request = QueryDataSetsRequest.newBuilder()
                    .addCriteria(QueryDataSetsRequest.QueryDataSetsCriterion.newBuilder()
                            .setIdCriterion(
                                    QueryDataSetsRequest.QueryDataSetsCriterion.IdCriterion.newBuilder()
                                            .addIds("not-an-objectid")))
                    .build();
            annotationServiceWrapper.sendQueryDataSets(
                    request,
                    true,
                    "QueryDataSetsRequest.criteria.IdCriterion ids must be valid ObjectId hex strings");
        }

        // OwnerCriterion: blank entry
        {
            final QueryDataSetsRequest request = QueryDataSetsRequest.newBuilder()
                    .addCriteria(QueryDataSetsRequest.QueryDataSetsCriterion.newBuilder()
                            .setOwnerCriterion(
                                    QueryDataSetsRequest.QueryDataSetsCriterion.OwnerCriterion.newBuilder()
                                            .addOwnerIds("")))
                    .build();
            annotationServiceWrapper.sendQueryDataSets(
                    request,
                    true,
                    "QueryDataSetsRequest.criteria.OwnerCriterion ownerIds must not contain blank entries");
        }

        // NameCriterion: blank prefix entry (would build a match-everything regex)
        {
            final QueryDataSetsRequest request = QueryDataSetsRequest.newBuilder()
                    .addCriteria(QueryDataSetsRequest.QueryDataSetsCriterion.newBuilder()
                            .setNameCriterion(
                                    QueryDataSetsRequest.QueryDataSetsCriterion.NameCriterion.newBuilder()
                                            .addPrefix("")))
                    .build();
            annotationServiceWrapper.sendQueryDataSets(
                    request,
                    true,
                    "QueryDataSetsRequest.criteria.NameCriterion entries must not be blank");
        }

        // PvNameCriterion: whitespace-only entry
        {
            final QueryDataSetsRequest request = QueryDataSetsRequest.newBuilder()
                    .addCriteria(QueryDataSetsRequest.QueryDataSetsCriterion.newBuilder()
                            .setPvNameCriterion(
                                    QueryDataSetsRequest.QueryDataSetsCriterion.PvNameCriterion.newBuilder()
                                            .addNames(" ")))
                    .build();
            annotationServiceWrapper.sendQueryDataSets(
                    request,
                    true,
                    "QueryDataSetsRequest.criteria.PvNameCriterion names must not contain blank entries");
        }

        // TagsCriterion: blank entry
        {
            final QueryDataSetsRequest request = QueryDataSetsRequest.newBuilder()
                    .addCriteria(QueryDataSetsRequest.QueryDataSetsCriterion.newBuilder()
                            .setTagsCriterion(
                                    QueryDataSetsRequest.QueryDataSetsCriterion.TagsCriterion.newBuilder()
                                            .addValues("")))
                    .build();
            annotationServiceWrapper.sendQueryDataSets(
                    request,
                    true,
                    "QueryDataSetsRequest.criteria.TagsCriterion values must not contain blank entries");
        }

        // AttributesCriterion: blank value entry (an empty values list is a key-existence search
        // and stays legal)
        {
            final QueryDataSetsRequest request = QueryDataSetsRequest.newBuilder()
                    .addCriteria(QueryDataSetsRequest.QueryDataSetsCriterion.newBuilder()
                            .setAttributesCriterion(
                                    QueryDataSetsRequest.QueryDataSetsCriterion.AttributesCriterion.newBuilder()
                                            .setKey("sector")
                                            .addValues("")))
                    .build();
            annotationServiceWrapper.sendQueryDataSets(
                    request,
                    true,
                    "QueryDataSetsRequest.criteria.AttributesCriterion values must not contain blank entries");
        }
    }

    /**
     * An empty criteria list is match-all, not a rejection -- the #245 contract extended to
     * queryDataSets by #248 Phase 1.
     */
    @Test
    public void testQueryDataSetsEmptyCriteriaMatchesAll() {

        final long startSeconds = Instant.now().getEpochSecond();
        annotationIngestionScenario(startSeconds);
        createDataSetScenario(startSeconds);

        final List<DataSet> resultDataSets = annotationServiceWrapper.sendQueryDataSets(
                QueryDataSetsRequest.newBuilder().build(), false, null);
        assertEquals(2, resultDataSets.size());
    }

    /**
     * Skip-token paging: limit-sized pages, a non-blank nextPageToken while more pages exist, a
     * blank one on the last page, and no document repeated or dropped across pages.
     */
    @Test
    public void testQueryDataSetsPagination() {

        final long startSeconds = Instant.now().getEpochSecond();
        annotationIngestionScenario(startSeconds);
        createDataSetScenario(startSeconds);

        // add a third dataset so limit=2 produces two pages
        final AnnotationTestBase.AnnotationDataBlock thirdBlock =
                new AnnotationTestBase.AnnotationDataBlock(
                        startSeconds, 0L, startSeconds, 499_000_000L, List.of("S03-GCC01"));
        final AnnotationTestBase.AnnotationDataSet thirdDataSet =
                new AnnotationTestBase.AnnotationDataSet(
                        null, "page test dataset", "craigmcc", "third dataset for paging",
                        List.of(thirdBlock));
        annotationServiceWrapper.sendAndVerifySaveDataSet(
                new AnnotationTestBase.SaveDataSetParams(thirdDataSet), false, false, "");

        final Set<String> seenIds = new HashSet<>();

        // page 1: limit=2, expect 2 results and a non-blank nextPageToken
        final AnnotationTestBase.QueryDataSetsResponseObserver page1Observer =
                new AnnotationTestBase.QueryDataSetsResponseObserver();
        final QueryDataSetsRequest page1Request = QueryDataSetsRequest.newBuilder()
                .setLimit(2)
                .build();
        new Thread(() -> DpAnnotationServiceGrpc
                .newStub(annotationServiceWrapper.getChannel())
                .queryDataSets(page1Request, page1Observer)).start();
        page1Observer.await();
        assertFalse(page1Observer.getErrorMessage(), page1Observer.isError());
        assertEquals(2, page1Observer.getDataSetsList().size());
        page1Observer.getDataSetsList().forEach(dataSet -> seenIds.add(dataSet.getId()));
        final String nextPageToken = page1Observer.getNextPageToken();
        assertNotNull(nextPageToken);
        assertFalse(nextPageToken.isBlank());

        // page 2: use nextPageToken, expect 1 result and a blank nextPageToken (last page)
        final AnnotationTestBase.QueryDataSetsResponseObserver page2Observer =
                new AnnotationTestBase.QueryDataSetsResponseObserver();
        final QueryDataSetsRequest page2Request = QueryDataSetsRequest.newBuilder()
                .setLimit(2)
                .setPageToken(nextPageToken)
                .build();
        new Thread(() -> DpAnnotationServiceGrpc
                .newStub(annotationServiceWrapper.getChannel())
                .queryDataSets(page2Request, page2Observer)).start();
        page2Observer.await();
        assertFalse(page2Observer.getErrorMessage(), page2Observer.isError());
        assertEquals(1, page2Observer.getDataSetsList().size());
        page2Observer.getDataSetsList().forEach(dataSet -> seenIds.add(dataSet.getId()));
        assertTrue("expected blank nextPageToken on last page", page2Observer.getNextPageToken().isBlank());

        // the two pages together cover all three datasets with no repeats
        assertEquals(3, seenIds.size());
    }

}
