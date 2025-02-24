package com.ospreydcs.dp.service.integration.annotation;

import com.ospreydcs.dp.grpc.v1.annotation.Calculations;
import com.ospreydcs.dp.grpc.v1.common.DataColumn;
import com.ospreydcs.dp.grpc.v1.common.DataTimestamps;
import com.ospreydcs.dp.grpc.v1.common.Timestamp;
import com.ospreydcs.dp.service.annotation.AnnotationTestBase;
import com.ospreydcs.dp.service.common.protobuf.DataColumnUtility;
import com.ospreydcs.dp.service.common.protobuf.DataTimestampsUtility;
import com.ospreydcs.dp.service.common.protobuf.EventMetadataUtility;
import com.ospreydcs.dp.service.common.protobuf.TimestampUtility;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class CreateAnnotationTest extends AnnotationIntegrationTestIntermediate {

    @BeforeClass
    public static void setUp() throws Exception {
        AnnotationIntegrationTestIntermediate.setUp();
    }

    @AfterClass
    public static void tearDown() {
        AnnotationIntegrationTestIntermediate.tearDown();
    }

    @Test
    public void testCreateAnnotationReject() {

        {
            // createAnnotation() negative test - request should be rejected because ownerId is not specified.

            final String unspecifiedOwnerId = "";
            final String dataSetId = "abcd1234";
            final String name = "negative test";
            AnnotationTestBase.CreateAnnotationRequestParams params =
                    new AnnotationTestBase.CreateAnnotationRequestParams(unspecifiedOwnerId, name, List.of(dataSetId));
            final String expectedRejectMessage = "CreateAnnotationRequest.ownerId must be specified";
            sendAndVerifyCreateAnnotation(
                    params, true, expectedRejectMessage);
        }

        {
            // createAnnotation() negative test - request should be rejected because name is not specified.

            final String ownerId = "craigmcc";
            final String dataSetId = "abcd1234";
            final String unspecifiedName = "";
            AnnotationTestBase.CreateAnnotationRequestParams params =
                    new AnnotationTestBase.CreateAnnotationRequestParams(ownerId, unspecifiedName, List.of(dataSetId));
            final String expectedRejectMessage = "CreateAnnotationRequest.name must be specified";
            sendAndVerifyCreateAnnotation(
                    params, true, expectedRejectMessage);
        }

        {
            // createAnnotation() negative test - request should be rejected because list of dataset ids is empty.

            final String ownerId = "craigmcc";
            final String emptyDataSetId = "";
            final String name = "negative test";
            AnnotationTestBase.CreateAnnotationRequestParams params =
                    new AnnotationTestBase.CreateAnnotationRequestParams(ownerId, name, new ArrayList<>());
            final String expectedRejectMessage = "CreateAnnotationRequest.dataSetIds must not be empty";
            sendAndVerifyCreateAnnotation(
                    params, true, expectedRejectMessage);
        }

        {
            // createAnnotation() negative test - request should be rejected because specified dataset doesn't exist

            final String ownerId = "craigmcc";
            final String invalidDataSetId = "junk12345";
            final String name = "negative test";
            AnnotationTestBase.CreateAnnotationRequestParams params =
                    new AnnotationTestBase.CreateAnnotationRequestParams(ownerId, name, List.of(invalidDataSetId));
            final String expectedRejectMessage = "no DataSetDocument found with id";
            sendAndVerifyCreateAnnotation(
                    params, true, expectedRejectMessage);
        }

    }

    @Test
    public void testCreateAnnotationPositive() {

        // ingest some data
        AnnotationIntegrationTestIntermediate.annotationIngestionScenario();

        // create some datasets
        CreateDataSetScenarioResult createDataSetScenarioResult =
                AnnotationIntegrationTestIntermediate.createDataSetScenario();

        // positive test case defined in superclass so it can be used to generate annotations for query and export tests
        CreateAnnotationScenarioResult createAnnotationScenarioResult =
                AnnotationIntegrationTestIntermediate.createAnnotationScenario(
                        createDataSetScenarioResult.firstHalfDataSetId, createDataSetScenarioResult.secondHalfDataSetId);

        {
            // createAnnotation() negative test - request includes an invalid associated annotation id

            final String ownerId = "craigmcc";
            final List<String> dataSetIds = List.of(createDataSetScenarioResult.secondHalfDataSetId);
            final String name = "negative test";
            final List<String> annotationIds = List.of("junk12345");
            final String comment = "This negative test case covers an annotation that specifies an invalid associated annotation id.";
            final List<String> tags = List.of("beam loss", "outage");
            final Map<String, String> attributeMap = Map.of("sector", "01", "subsystem", "vacuum");
            final EventMetadataUtility.EventMetadataParams eventMetadataParams =
                    new EventMetadataUtility.EventMetadataParams(
                            "experiment 1234",
                            startSeconds,
                            0L,
                            startSeconds+60,
                            999_000_000L);

            AnnotationTestBase.CreateAnnotationRequestParams params =
                    new AnnotationTestBase.CreateAnnotationRequestParams(
                            ownerId,
                            name,
                            dataSetIds,
                            annotationIds,
                            comment,
                            tags,
                            attributeMap,
                            eventMetadataParams, null);

            final boolean expectReject = true;
            final String expectedRejectMessage = "no AnnotationDocument found with id: junk12345";
            sendAndVerifyCreateAnnotation(
                    params, expectReject, expectedRejectMessage);
        }

    }

}
