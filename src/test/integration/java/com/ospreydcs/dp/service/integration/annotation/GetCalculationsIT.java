package com.ospreydcs.dp.service.integration.annotation;

import com.ospreydcs.dp.grpc.v1.annotation.Calculations;
import com.ospreydcs.dp.service.annotation.AnnotationTestBase;
import org.bson.types.ObjectId;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.time.Instant;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class GetCalculationsIT extends AnnotationIntegrationTestIntermediate {

    @Before
    public void setUp() throws Exception {
        super.setUp();
    }

    @After
    public void tearDown() {
        super.tearDown();
    }

    @Test
    public void testGetCalculations() {

        final long startSeconds = Instant.now().getEpochSecond();

        // ingest some data and create datasets over it
        annotationIngestionScenario(startSeconds);
        final CreateDataSetScenarioResult scenarioResult = createDataSetScenario(startSeconds);

        // calculations are written through saveAnnotation(); the save response returns the
        // calculationsId addressing key used here
        final AnnotationTestBase.SaveAnnotationRequestParams params =
                new AnnotationTestBase.SaveAnnotationRequestParams(
                        null, "craigmcc", "annotation with calculations",
                        List.of(scenarioResult.firstHalfDataSetId()),
                        null, null, null, null,
                        GetAnnotationIT.buildCalculations(startSeconds));
        annotationServiceWrapper.sendAndVerifySaveAnnotation(params, false, false, "");
        final String calculationsId = annotationServiceWrapper.lastSaveAnnotationCalculationsId;
        assertNotNull(calculationsId);

        {
            // getCalculations() positive test
            final Calculations calculations =
                    annotationServiceWrapper.sendAndVerifyGetCalculations(calculationsId, false, null);
            assertEquals(1, calculations.getCalculationDataFramesCount());
            assertEquals("frame-1", calculations.getCalculationDataFrames(0).getName());
            assertEquals(2, calculations.getCalculationDataFrames(0).getFrame().getDataColumnsCount());
        }

        {
            // getCalculations() negative test - id that matches no record
            final String missingId = new ObjectId().toHexString();
            annotationServiceWrapper.sendAndVerifyGetCalculations(
                    missingId, true, "no Calculations record found for id: " + missingId);
        }

        {
            // getCalculations() negative test - blank and malformed ids (#248 plan D11)
            annotationServiceWrapper.sendAndVerifyGetCalculations(
                    "", true, "GetCalculationsRequest.calculationsId must be specified");
            annotationServiceWrapper.sendAndVerifyGetCalculations(
                    "not-an-object-id", true,
                    "GetCalculationsRequest.calculationsId is not a valid id: not-an-object-id");
        }
    }
}
