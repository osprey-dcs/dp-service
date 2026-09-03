package com.ospreydcs.dp.service.integration.annotation;

import com.ospreydcs.dp.grpc.v1.annotation.DataSet;
import com.ospreydcs.dp.grpc.v1.annotation.QueryDataSetsRequest;
import com.ospreydcs.dp.service.annotation.AnnotationTestBase;
import org.junit.*;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

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

}
