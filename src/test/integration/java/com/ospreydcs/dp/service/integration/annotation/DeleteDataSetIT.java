package com.ospreydcs.dp.service.integration.annotation;

import com.ospreydcs.dp.service.annotation.AnnotationTestBase;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.time.Instant;
import java.util.List;

public class DeleteDataSetIT extends AnnotationIntegrationTestIntermediate {

    @Before
    public void setUp() throws Exception {
        super.setUp();
    }

    @After
    public void tearDown() {
        super.tearDown();
    }

    @Test
    public void testDeleteDataSet() {

        final long startSeconds = Instant.now().getEpochSecond();

        // ingest some data and create datasets over it
        annotationIngestionScenario(startSeconds);
        final CreateDataSetScenarioResult scenarioResult = createDataSetScenario(startSeconds);
        final String referencedDataSetId = scenarioResult.firstHalfDataSetId();
        final String unreferencedDataSetId = scenarioResult.secondHalfDataSetId();

        // create an annotation referencing the first dataset
        final AnnotationTestBase.SaveAnnotationRequestParams annotationParams =
                new AnnotationTestBase.SaveAnnotationRequestParams(
                        "craigmcc", "references first half dataset", List.of(referencedDataSetId));
        final String annotationId =
                annotationServiceWrapper.sendAndVerifySaveAnnotation(annotationParams, false, false, "");

        {
            // deleteDataSet() negative test - rejected while an annotation references the dataset,
            // naming one referencing annotation id and the total count (#248 plan D7)
            annotationServiceWrapper.sendAndVerifyDeleteDataSet(
                    referencedDataSetId, true,
                    "cannot delete dataSetId '" + referencedDataSetId
                            + "': referenced by 1 annotation(s) including id: " + annotationId);
        }

        {
            // deleteDataSet() positive test - an unreferenced dataset deletes cleanly
            annotationServiceWrapper.sendAndVerifyDeleteDataSet(unreferencedDataSetId, false, null);

            // and is no longer retrievable
            annotationServiceWrapper.sendAndVerifyGetDataSet(
                    unreferencedDataSetId, true,
                    "no DataSet record found for id: " + unreferencedDataSetId);
        }

        {
            // deleteDataSet() positive test - deleting the referencing annotation unblocks the delete
            annotationServiceWrapper.sendAndVerifyDeleteAnnotation(annotationId, false, null);
            annotationServiceWrapper.sendAndVerifyDeleteDataSet(referencedDataSetId, false, null);
        }

        {
            // deleteDataSet() negative test - id that matches no record (including one just deleted)
            annotationServiceWrapper.sendAndVerifyDeleteDataSet(
                    unreferencedDataSetId, true,
                    "no DataSet record found for id: " + unreferencedDataSetId);
        }

        {
            // deleteDataSet() negative test - blank and malformed ids (#248 plan D11)
            annotationServiceWrapper.sendAndVerifyDeleteDataSet(
                    "", true, "DeleteDataSetRequest.dataSetId must be specified");
            annotationServiceWrapper.sendAndVerifyDeleteDataSet(
                    "not-an-object-id", true,
                    "DeleteDataSetRequest.dataSetId is not a valid id: not-an-object-id");
        }
    }
}
