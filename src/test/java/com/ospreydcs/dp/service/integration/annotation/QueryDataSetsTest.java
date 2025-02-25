package com.ospreydcs.dp.service.integration.annotation;

import com.ospreydcs.dp.service.annotation.AnnotationTestBase;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

public class QueryDataSetsTest extends AnnotationIntegrationTestIntermediate {

    @BeforeClass
    public static void setUp() throws Exception {
        AnnotationIntegrationTestIntermediate.setUp();
    }

    @AfterClass
    public static void tearDown() {
        AnnotationIntegrationTestIntermediate.tearDown();
    }

    @Test
    public void testQueryDataSetsReject() {

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

        // queryDataSets() negative test - rejected because PvNameCriterion is empty
        {
            final String blankPvName = "";
            final AnnotationTestBase.QueryDataSetsParams queryParams = new AnnotationTestBase.QueryDataSetsParams();
            queryParams.setPvNameCriterion(blankPvName);

            final boolean expectReject = true;
            final String expectedRejectMessage ="QueryDataSetsRequest.criteria.PvNameCriterion name must be specified";

            sendAndVerifyQueryDataSets(
                    queryParams, expectReject, expectedRejectMessage, new ArrayList<>());
        }


    }

    @Test
    public void testQueryDataSetsPositive() {

        // ingest some data
        AnnotationIntegrationTestIntermediate.annotationIngestionScenario();

        // create some datasets
        CreateDataSetScenarioResult createDataSetScenarioResult =
                AnnotationIntegrationTestIntermediate.createDataSetScenario();

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

            List<AnnotationTestBase.CreateDataSetParams> expectedQueryResultDataSets =
                    List.of(createDataSetScenarioResult.firstHalfDataSetParams);
            sendAndVerifyQueryDataSets(
                    queryParams, expectReject, expectedRejectMessage, expectedQueryResultDataSets);
        }

        // queryDataSets() positive test - query by IdCriterion
        {
            final String datasetId = createDataSetScenarioResult.firstHalfDataSetId;
            final AnnotationTestBase.QueryDataSetsParams queryParams = new AnnotationTestBase.QueryDataSetsParams();
            queryParams.setIdCriterion(createDataSetScenarioResult.firstHalfDataSetId);

            final boolean expectReject = false;
            final String expectedRejectMessage ="";

            List<AnnotationTestBase.CreateDataSetParams> expectedQueryResultDataSets =
                    List.of(createDataSetScenarioResult.firstHalfDataSetParams);
            sendAndVerifyQueryDataSets(
                    queryParams, expectReject, expectedRejectMessage, expectedQueryResultDataSets);
        }

        // queryDataSets() positive test - query by TextCriterion (on name field)
        {
            final String datasetName = "half2";
            final AnnotationTestBase.QueryDataSetsParams queryParams = new AnnotationTestBase.QueryDataSetsParams();
            queryParams.setTextCriterion(datasetName);

            final boolean expectReject = false;
            final String expectedRejectMessage ="";

            List<AnnotationTestBase.CreateDataSetParams> expectedQueryResultDataSets =
                    List.of(createDataSetScenarioResult.secondHalfDataSetParams);
            sendAndVerifyQueryDataSets(
                    queryParams, expectReject, expectedRejectMessage, expectedQueryResultDataSets);
        }

        // queryDataSets() positive test - query by PvNameCriterion (on data block pvNames field)
        {
            final String pvName = "S01-GCC01";
            final AnnotationTestBase.QueryDataSetsParams queryParams = new AnnotationTestBase.QueryDataSetsParams();
            queryParams.setPvNameCriterion(pvName);

            final boolean expectReject = false;
            final String expectedRejectMessage ="";

            List<AnnotationTestBase.CreateDataSetParams> expectedQueryResultDataSets =
                    List.of(createDataSetScenarioResult.firstHalfDataSetParams);
            sendAndVerifyQueryDataSets(
                    queryParams, expectReject, expectedRejectMessage, expectedQueryResultDataSets);
        }

    }

}
