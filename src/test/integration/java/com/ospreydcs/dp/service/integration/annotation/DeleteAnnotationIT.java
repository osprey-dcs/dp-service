package com.ospreydcs.dp.service.integration.annotation;

import com.ospreydcs.dp.service.annotation.AnnotationTestBase;
import com.ospreydcs.dp.service.common.bson.annotation.AnnotationDocument;
import org.bson.types.ObjectId;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.time.Instant;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class DeleteAnnotationIT extends AnnotationIntegrationTestIntermediate {

    @Before
    public void setUp() throws Exception {
        super.setUp();
    }

    @After
    public void tearDown() {
        super.tearDown();
    }

    @Test
    public void testDeleteAnnotation() {

        final long startSeconds = Instant.now().getEpochSecond();

        // ingest some data and create datasets over it
        annotationIngestionScenario(startSeconds);
        final CreateDataSetScenarioResult scenarioResult = createDataSetScenario(startSeconds);
        final String dataSetId = scenarioResult.firstHalfDataSetId();

        {
            // deleteAnnotation() positive test - annotation without calculations
            final AnnotationTestBase.SaveAnnotationRequestParams params =
                    new AnnotationTestBase.SaveAnnotationRequestParams(
                            "craigmcc", "annotation without calculations", List.of(dataSetId));
            final String annotationId =
                    annotationServiceWrapper.sendAndVerifySaveAnnotation(params, false, false, "");

            annotationServiceWrapper.sendAndVerifyDeleteAnnotation(annotationId, false, null);
        }

        {
            // deleteAnnotation() positive test - the annotation's calculations document is deleted
            // with it (lifecycle belongs to the owning annotation); cascade verified by the wrapper
            final AnnotationTestBase.SaveAnnotationRequestParams params =
                    new AnnotationTestBase.SaveAnnotationRequestParams(
                            null, "craigmcc", "annotation with calculations", List.of(dataSetId),
                            null, null, null, null,
                            GetAnnotationIT.buildCalculations(startSeconds));
            final String annotationId =
                    annotationServiceWrapper.sendAndVerifySaveAnnotation(params, false, false, "");

            annotationServiceWrapper.sendAndVerifyDeleteAnnotation(annotationId, false, null);
        }

        {
            // deleteAnnotation() positive test - incoming soft references do not block the delete
            // and are permitted to dangle afterward
            final AnnotationTestBase.SaveAnnotationRequestParams targetParams =
                    new AnnotationTestBase.SaveAnnotationRequestParams(
                            "craigmcc", "soft-reference target", List.of(dataSetId));
            final String targetId =
                    annotationServiceWrapper.sendAndVerifySaveAnnotation(targetParams, false, false, "");

            final AnnotationTestBase.SaveAnnotationRequestParams referrerParams =
                    new AnnotationTestBase.SaveAnnotationRequestParams(
                            null, "craigmcc", "soft-reference referrer", List.of(dataSetId),
                            List.of(targetId), null, null, null, null);
            final String referrerId =
                    annotationServiceWrapper.sendAndVerifySaveAnnotation(referrerParams, false, false, "");

            annotationServiceWrapper.sendAndVerifyDeleteAnnotation(targetId, false, null);

            // the referrer survives, its annotationIds link now dangling
            final AnnotationDocument referrerDocument = mongoClient.findAnnotationNoRetry(referrerId);
            assertNotNull(referrerDocument);
            assertEquals(List.of(targetId), referrerDocument.getAnnotationIds());
        }

        {
            // deleteAnnotation() negative test - id that matches no record
            final String missingId = new ObjectId().toHexString();
            annotationServiceWrapper.sendAndVerifyDeleteAnnotation(
                    missingId, true, "no Annotation record found for id: " + missingId);
        }

        {
            // deleteAnnotation() negative test - blank and malformed ids (#248 plan D11)
            annotationServiceWrapper.sendAndVerifyDeleteAnnotation(
                    "", true, "DeleteAnnotationRequest.annotationId must be specified");
            annotationServiceWrapper.sendAndVerifyDeleteAnnotation(
                    "not-an-object-id", true,
                    "DeleteAnnotationRequest.annotationId is not a valid id: not-an-object-id");
        }
    }
}
