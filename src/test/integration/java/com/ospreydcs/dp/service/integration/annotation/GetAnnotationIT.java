package com.ospreydcs.dp.service.integration.annotation;

import com.ospreydcs.dp.grpc.v1.annotation.Annotation;
import com.ospreydcs.dp.grpc.v1.annotation.Calculations;
import com.ospreydcs.dp.grpc.v1.annotation.DpAnnotationServiceGrpc;
import com.ospreydcs.dp.grpc.v1.annotation.GetAnnotationRequest;
import com.ospreydcs.dp.grpc.v1.common.DataColumn;
import com.ospreydcs.dp.grpc.v1.common.DataFrame;
import com.ospreydcs.dp.grpc.v1.common.DataTimestamps;
import com.ospreydcs.dp.grpc.v1.common.ExceptionalResult;
import com.ospreydcs.dp.service.annotation.AnnotationTestBase;
import com.ospreydcs.dp.service.common.protobuf.DataColumnUtility;
import com.ospreydcs.dp.service.common.protobuf.DataTimestampsUtility;
import org.bson.types.ObjectId;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.time.Instant;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class GetAnnotationIT extends AnnotationIntegrationTestIntermediate {

    @Before
    public void setUp() throws Exception {
        super.setUp();
    }

    @After
    public void tearDown() {
        super.tearDown();
    }

    static Calculations buildCalculations(long startSeconds) {
        // one frame with a sampling clock and two double columns
        final DataTimestamps dataTimestamps =
                DataTimestampsUtility.dataTimestampsWithSamplingClock(
                        startSeconds, 500_000_000L, 250_000_000L, 2);
        final DataColumn column1 = DataColumnUtility.dataColumnWithDoubleValues("calc-1", List.of(0.0, 1.1));
        final DataColumn column2 = DataColumnUtility.dataColumnWithDoubleValues("calc-2", List.of(2.2, 3.3));
        return Calculations.newBuilder()
                .addCalculationDataFrames(Calculations.CalculationsDataFrame.newBuilder()
                        .setName("frame-1")
                        .setFrame(DataFrame.newBuilder()
                                .setDataTimestamps(dataTimestamps)
                                .addAllDataColumns(List.of(column1, column2))))
                .build();
    }

    @Test
    public void testGetAnnotation() {

        final long startSeconds = Instant.now().getEpochSecond();

        // ingest some data and create datasets over it
        annotationIngestionScenario(startSeconds);
        final CreateDataSetScenarioResult scenarioResult = createDataSetScenario(startSeconds);
        final String dataSetId = scenarioResult.firstHalfDataSetId();

        {
            // getAnnotation() positive test - annotation without calculations

            final AnnotationTestBase.SaveAnnotationRequestParams params =
                    new AnnotationTestBase.SaveAnnotationRequestParams(
                            "craigmcc", "annotation without calculations", List.of(dataSetId))
                            .withModifiedBy("operator-2");
            final String annotationId =
                    annotationServiceWrapper.sendAndVerifySaveAnnotation(params, false, false, "");

            final Annotation annotation =
                    annotationServiceWrapper.sendAndVerifyGetAnnotation(annotationId, false, null);

            assertEquals("craigmcc", annotation.getOwnerId());
            assertEquals("annotation without calculations", annotation.getName());
            assertEquals(List.of(dataSetId), annotation.getDataSetIdsList());
            assertTrue(annotation.getCalculationsId().isEmpty());
            assertFalse(annotation.hasCalculations());
            assertEquals("operator-2", annotation.getModifiedBy());

            // audit fields: createdTime is server-set; updatedTime stays unset until the first
            // update (#248 plan D13, matching the other entity types in this service)
            assertTrue(annotation.hasCreatedTime());
            assertFalse(annotation.hasUpdatedTime());
        }

        {
            // getAnnotation() positive test - calculations content is populated inline, the one
            // method that returns it within an Annotation

            final AnnotationTestBase.SaveAnnotationRequestParams params =
                    new AnnotationTestBase.SaveAnnotationRequestParams(
                            null, "craigmcc", "annotation with calculations", List.of(dataSetId),
                            null, null, null, null, buildCalculations(startSeconds));
            final String annotationId =
                    annotationServiceWrapper.sendAndVerifySaveAnnotation(params, false, false, "");

            final Annotation annotation =
                    annotationServiceWrapper.sendAndVerifyGetAnnotation(annotationId, false, null);

            assertFalse(annotation.getCalculationsId().isEmpty());
            assertTrue(annotation.hasCalculations());
            assertEquals(annotation.getCalculationsId(), annotation.getCalculations().getId());
            assertEquals(1, annotation.getCalculations().getCalculationDataFramesCount());
            assertEquals("frame-1", annotation.getCalculations().getCalculationDataFrames(0).getName());
        }

        {
            // getAnnotation() negative test - a calculationsId that resolves to nothing is
            // corruption, reported as an ERROR rather than as silently-empty content (#248 plan D16)

            final AnnotationTestBase.SaveAnnotationRequestParams params =
                    new AnnotationTestBase.SaveAnnotationRequestParams(
                            null, "craigmcc", "annotation with dangling calculations", List.of(dataSetId),
                            null, null, null, null, buildCalculations(startSeconds));
            final String annotationId =
                    annotationServiceWrapper.sendAndVerifySaveAnnotation(params, false, false, "");

            // manufacture the corrupt state: remove the calculations document out from under the
            // annotation
            final String calculationsId = mongoClient.findAnnotation(annotationId).getCalculationsId();
            mongoClient.deleteCalculationsDocument(calculationsId);

            final GetAnnotationRequest request = AnnotationTestBase.buildGetAnnotationRequest(annotationId);
            final DpAnnotationServiceGrpc.DpAnnotationServiceStub asyncStub =
                    DpAnnotationServiceGrpc.newStub(annotationServiceWrapper.getAnnotationChannel());
            final AnnotationTestBase.GetAnnotationResponseObserver responseObserver =
                    new AnnotationTestBase.GetAnnotationResponseObserver();
            new Thread(() -> asyncStub.getAnnotation(request, responseObserver)).start();
            responseObserver.await();

            assertTrue(responseObserver.isError());
            assertEquals(
                    "a dangling calculations reference is corruption, not a rejection",
                    ExceptionalResult.ExceptionalResultStatus.RESULT_STATUS_ERROR,
                    responseObserver.getExceptionalResultStatus());
            assertTrue(responseObserver.getErrorMessage().contains(calculationsId));
        }
    }

    @Test
    public void testGetAnnotationRejects() {

        {
            // getAnnotation() negative test - id that matches no record
            final String missingId = new ObjectId().toHexString();
            annotationServiceWrapper.sendAndVerifyGetAnnotation(
                    missingId, true, "no Annotation record found for id: " + missingId);
        }

        {
            // getAnnotation() negative test - blank id
            annotationServiceWrapper.sendAndVerifyGetAnnotation(
                    "", true, "GetAnnotationRequest.annotationId must be specified");
        }

        {
            // getAnnotation() negative test - malformed id (#248 plan D11)
            annotationServiceWrapper.sendAndVerifyGetAnnotation(
                    "not-an-object-id", true,
                    "GetAnnotationRequest.annotationId is not a valid id: not-an-object-id");
        }
    }
}
