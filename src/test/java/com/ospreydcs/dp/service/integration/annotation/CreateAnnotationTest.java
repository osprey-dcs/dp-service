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

        {
            // createAnnotation() with calculations negative test using DataTimestamps.TimestampList
            // rejected because TimestampList is empty

            final String ownerId = "craigmcc";
            final List<String> dataSetIds = List.of(createDataSetScenarioResult.secondHalfDataSetId);
            final String name = "negative test";

            // create calculations for request, with 2 data frames, each with 2 columns
            final Calculations.Builder calculationsBuilder = Calculations.newBuilder();
            for (int i = 0 ; i < 2 ; i++) {

                // create sampling clock with TimestampList
                final List<Timestamp> emptyTimestampList = new ArrayList<>();
                final DataTimestamps dataTimestamps =
                        DataTimestampsUtility.dataTimestampsWithTimestampList(emptyTimestampList);

                // create data columns
                final List<DataColumn> dataColumns = new ArrayList<>();
                for (int j = 0 ; j < 2 ; j++) {
                    final String columnName = "calc-" + i + "-" + j;
                    final DataColumn dataColumn =
                            DataColumnUtility.dataColumnWithDoubleValues(columnName, List.of(0.0, 1.1));
                    dataColumns.add(dataColumn);
                }

                // create data frame
                final Calculations.CalculationsDataFrame dataFrame = Calculations.CalculationsDataFrame.newBuilder()
                        .setDataTimestamps(dataTimestamps)
                        .addAllDataColumns(dataColumns)
                        .build();
                calculationsBuilder.addCalculationDataFrames(dataFrame);
            }
            final Calculations calculations = calculationsBuilder.build();

            final AnnotationTestBase.CreateAnnotationRequestParams params =
                    new AnnotationTestBase.CreateAnnotationRequestParams(
                            ownerId,
                            name,
                            dataSetIds,
                            null,
                            null,
                            null,
                            null,
                            null,
                            calculations);

            final boolean expectReject = true;
            final String expectedRejectMessage = "CalculationDataFrame.dataTimestamps.timestampList must not be empty";
            sendAndVerifyCreateAnnotation(params, expectReject, expectedRejectMessage);
        }

        {
            // createAnnotation() with calculations positive test using DataTimestamps.TimestampList

            final String ownerId = "craigmcc";
            final List<String> dataSetIds = List.of(createDataSetScenarioResult.secondHalfDataSetId);
            final String name = "negative test empty list of data frames";

            // create calculations for request, with 2 data frames, each with 2 columns
            final Calculations.Builder calculationsBuilder = Calculations.newBuilder();
            for (int i = 0 ; i < 2 ; i++) {

                // create sampling clock with TimestampList
                final List<Timestamp> timestampList = new ArrayList<>();
                final Timestamp timestamp1 =
                        TimestampUtility.timestampFromSeconds(startSeconds+i, 500_000_000L);
                timestampList.add(timestamp1);
                final Timestamp timestamp2 =
                        TimestampUtility.timestampFromSeconds(startSeconds+i, 750_000_000L);
                timestampList.add(timestamp2);
                final DataTimestamps dataTimestamps =
                        DataTimestampsUtility.dataTimestampsWithTimestampList(timestampList);

                // create data columns
                final List<DataColumn> dataColumns = new ArrayList<>();
                for (int j = 0 ; j < 2 ; j++) {
                    final String columnName = "calc-" + i + "-" + j;
                    final DataColumn dataColumn =
                            DataColumnUtility.dataColumnWithDoubleValues(columnName, List.of(0.0, 1.1));
                    dataColumns.add(dataColumn);
                }

                // create data frame
                final Calculations.CalculationsDataFrame dataFrame = Calculations.CalculationsDataFrame.newBuilder()
                        .setDataTimestamps(dataTimestamps)
                        .addAllDataColumns(dataColumns)
                        .build();
                calculationsBuilder.addCalculationDataFrames(dataFrame);
            }
            final Calculations calculations = calculationsBuilder.build();

            final AnnotationTestBase.CreateAnnotationRequestParams params =
                    new AnnotationTestBase.CreateAnnotationRequestParams(
                            ownerId,
                            name,
                            dataSetIds,
                            null,
                            null,
                            null,
                            null,
                            null,
                            calculations);

            final boolean expectReject = false;
            final String expectedRejectMessage = "";
            sendAndVerifyCreateAnnotation(params, expectReject, expectedRejectMessage);
        }

    }
}
