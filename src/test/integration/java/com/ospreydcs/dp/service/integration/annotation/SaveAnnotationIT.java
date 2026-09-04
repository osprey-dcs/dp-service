package com.ospreydcs.dp.service.integration.annotation;

import com.ospreydcs.dp.service.annotation.AnnotationTestBase;
import com.ospreydcs.dp.service.common.bson.annotation.AnnotationDocument;
import org.bson.types.ObjectId;
import org.junit.*;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.*;

public class SaveAnnotationIT extends AnnotationIntegrationTestIntermediate {

    @Before
    public void setUp() throws Exception {
        super.setUp();
    }

    @After
    public void tearDown() {
        super.tearDown();
    }

    @Test
    public void testSaveAnnotationReject() {

        {
            // saveAnnotatio() negative test - request should be rejected because ownerId is not specified.

            final String unspecifiedOwnerId = "";
            final String dataSetId = "abcd1234";
            final String name = "negative test";
            AnnotationTestBase.SaveAnnotationRequestParams params =
                    new AnnotationTestBase.SaveAnnotationRequestParams(unspecifiedOwnerId, name, List.of(dataSetId));
            final String expectedRejectMessage = "SaveAnnotationRequest.ownerId must be specified";
            annotationServiceWrapper.sendAndVerifySaveAnnotation(
                    params, false, true, expectedRejectMessage);
        }

        {
            // saveAnnotatio() negative test - request should be rejected because name is not specified.

            final String ownerId = "craigmcc";
            final String dataSetId = "abcd1234";
            final String unspecifiedName = "";
            AnnotationTestBase.SaveAnnotationRequestParams params =
                    new AnnotationTestBase.SaveAnnotationRequestParams(ownerId, unspecifiedName, List.of(dataSetId));
            final String expectedRejectMessage = "SaveAnnotationRequest.name must be specified";
            annotationServiceWrapper.sendAndVerifySaveAnnotation(
                    params, false, true, expectedRejectMessage);
        }

        {
            // saveAnnotatio() negative test - request should be rejected because list of dataset ids is empty.

            final String ownerId = "craigmcc";
            final String emptyDataSetId = "";
            final String name = "negative test";
            AnnotationTestBase.SaveAnnotationRequestParams params =
                    new AnnotationTestBase.SaveAnnotationRequestParams(ownerId, name, new ArrayList<>());
            final String expectedRejectMessage = "SaveAnnotationRequest.dataSetIds must not be empty";
            annotationServiceWrapper.sendAndVerifySaveAnnotation(
                    params, false, true, expectedRejectMessage);
        }

        {
            // saveAnnotatio() negative test - request should be rejected because dataset id is malformed

            final String ownerId = "craigmcc";
            final String invalidDataSetId = "junk12345";
            final String name = "negative test";
            AnnotationTestBase.SaveAnnotationRequestParams params =
                    new AnnotationTestBase.SaveAnnotationRequestParams(ownerId, name, List.of(invalidDataSetId));
            final String expectedRejectMessage = "SaveAnnotationRequest.dataSetIds contains invalid id: junk12345";
            annotationServiceWrapper.sendAndVerifySaveAnnotation(
                    params, false, true, expectedRejectMessage);
        }

        {
            // saveAnnotatio() negative test - request should be rejected because specified dataset doesn't exist

            final String ownerId = "craigmcc";
            final String missingDataSetId = new ObjectId().toHexString();
            final String name = "negative test";
            AnnotationTestBase.SaveAnnotationRequestParams params =
                    new AnnotationTestBase.SaveAnnotationRequestParams(ownerId, name, List.of(missingDataSetId));
            final String expectedRejectMessage = "no DataSetDocument found with id: " + missingDataSetId;
            annotationServiceWrapper.sendAndVerifySaveAnnotation(
                    params, false, true, expectedRejectMessage);
        }

    }

    @Test
    public void testSaveAnnotatioPositive() {

        final long startSeconds = Instant.now().getEpochSecond();
        final long startNanos = 0L;

        // ingest some data
        annotationIngestionScenario(startSeconds);

        // create some datasets
        CreateDataSetScenarioResult createDataSetScenarioResult = createDataSetScenario(startSeconds);

        // positive test case defined in superclass so it can be used to generate annotations for query and export tests
        CreateAnnotationScenarioResult createAnnotationScenarioResult = createAnnotationScenario(
                startSeconds,
                createDataSetScenarioResult.firstHalfDataSetId(),
                createDataSetScenarioResult.secondHalfDataSetId());

        {
            // saveAnnotatio() negative test - request includes an invalid associated annotation id

            final String ownerId = "craigmcc";
            final List<String> dataSetIds = List.of(createDataSetScenarioResult.secondHalfDataSetId());
            final String name = "negative test";
            final List<String> annotationIds = List.of("junk12345");
            final String comment = "This negative test case covers an annotation that specifies an invalid associated annotation id.";
            final List<String> tags = List.of("beam loss", "outage");
            final Map<String, String> attributeMap = Map.of("sector", "01", "subsystem", "vacuum");

            AnnotationTestBase.SaveAnnotationRequestParams params =
                    new AnnotationTestBase.SaveAnnotationRequestParams(
                            null, ownerId,
                            name,
                            dataSetIds,
                            annotationIds,
                            comment,
                            tags,
                            attributeMap,
                            null);

            final boolean expectReject = true;
            final String expectedRejectMessage = "SaveAnnotationRequest.annotationIds contains invalid id: junk12345";
            annotationServiceWrapper.sendAndVerifySaveAnnotation(
                    params, false, expectReject, expectedRejectMessage);
        }

    }

    @Test
    public void testSaveAnnotationAuditFieldsAndCalculationsLifecycle() {

        final long startSeconds = Instant.now().getEpochSecond();

        // ingest some data and create datasets over it
        annotationIngestionScenario(startSeconds);
        final CreateDataSetScenarioResult scenarioResult = createDataSetScenario(startSeconds);
        final String dataSetId = scenarioResult.firstHalfDataSetId();

        String annotationId;
        String firstCalculationsId;
        {
            // saveAnnotation() positive test - create with modifiedBy and calculations; the response
            // carries calculationsId (asserted by the wrapper), and updatedTime stays unset on
            // create (#248 plan D13)

            final AnnotationTestBase.SaveAnnotationRequestParams params =
                    new AnnotationTestBase.SaveAnnotationRequestParams(
                            null, "craigmcc", "calculations lifecycle", List.of(dataSetId),
                            null, null, List.of("Beam Loss", "OUTAGE", "beam loss"), null,
                            GetAnnotationIT.buildCalculations(startSeconds))
                            .withModifiedBy("operator-3");
            annotationId = annotationServiceWrapper.sendAndVerifySaveAnnotation(params, false, false, "");
            firstCalculationsId = annotationServiceWrapper.lastSaveAnnotationCalculationsId;

            final AnnotationDocument document = mongoClient.findAnnotation(annotationId);
            assertEquals("operator-3", document.getModifiedBy());
            assertEquals(List.of("beam loss", "outage"), document.getTags());
            assertNotNull(document.getCreatedAt());
            assertNull(document.getUpdatedAt());
        }

        {
            // saveAnnotation() positive test - full replace with new calculations deletes the
            // replaced calculations document rather than orphaning it (#248 plan D14)

            final AnnotationTestBase.SaveAnnotationRequestParams params =
                    new AnnotationTestBase.SaveAnnotationRequestParams(
                            annotationId, "craigmcc", "calculations lifecycle", List.of(dataSetId),
                            null, null, null, null,
                            GetAnnotationIT.buildCalculations(startSeconds + 100));
            annotationServiceWrapper.sendAndVerifySaveAnnotation(params, true, false, "");
            final String secondCalculationsId = annotationServiceWrapper.lastSaveAnnotationCalculationsId;

            assertNotEquals(firstCalculationsId, secondCalculationsId);
            assertNull("replaced calculations document must be deleted, not orphaned",
                    mongoClient.findCalculationsNoRetry(firstCalculationsId));
            assertNotNull(mongoClient.findCalculationsNoRetry(secondCalculationsId));

            // full-replace update: createdAt preserved, updatedAt now set
            final AnnotationDocument document = mongoClient.findAnnotation(annotationId);
            assertNotNull(document.getCreatedAt());
            assertNotNull(document.getUpdatedAt());

            // full replace applies to calculations like every other field: an update that omits
            // them clears the reference and deletes the stored document
            final AnnotationTestBase.SaveAnnotationRequestParams clearingParams =
                    new AnnotationTestBase.SaveAnnotationRequestParams(
                            annotationId, "craigmcc", "calculations lifecycle", List.of(dataSetId),
                            null, null, null, null, null);
            annotationServiceWrapper.sendAndVerifySaveAnnotation(clearingParams, true, false, "");

            assertNull(mongoClient.findAnnotation(annotationId).getCalculationsId());
            assertNull("cleared calculations document must be deleted, not orphaned",
                    mongoClient.findCalculationsNoRetry(secondCalculationsId));
        }
    }

}
