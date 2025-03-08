package com.ospreydcs.dp.service.integration.annotation;

import com.ospreydcs.dp.grpc.v1.annotation.Calculations;
import com.ospreydcs.dp.grpc.v1.common.DataColumn;
import com.ospreydcs.dp.grpc.v1.common.DataTimestamps;
import com.ospreydcs.dp.grpc.v1.common.Timestamp;
import com.ospreydcs.dp.service.annotation.AnnotationTestBase;
import com.ospreydcs.dp.service.common.protobuf.DataColumnUtility;
import com.ospreydcs.dp.service.common.protobuf.DataTimestampsUtility;
import com.ospreydcs.dp.service.common.protobuf.TimestampUtility;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

public class CreateAnnotationCalculationsTest extends AnnotationIntegrationTestIntermediate {

    @BeforeClass
    public static void setUp() throws Exception {
        AnnotationIntegrationTestIntermediate.setUp();
    }

    @AfterClass
    public static void tearDown() {
        AnnotationIntegrationTestIntermediate.tearDown();
    }

    @Test
    public void testCreateAnnotation() {

        // ingest some data
        AnnotationIntegrationTestIntermediate.annotationIngestionScenario();

        // create some datasets
        CreateDataSetScenarioResult createDataSetScenarioResult =
                AnnotationIntegrationTestIntermediate.createDataSetScenario();

        // createAnnotation() with calculations negative test -
        // request should be rejected because: list of data frames is empty
        {
            final String ownerId = "craigmcc";
            final List<String> dataSetIds = List.of(createDataSetScenarioResult.secondHalfDataSetId);
            final String name = "negative test";

            // create calculations for request, with 2 data frames, each with 2 columns
            final Calculations.Builder calculationsBuilder = Calculations.newBuilder();
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
            final String expectedRejectMessage =
                    "CreateAnnotationRequest.calculations.calculationDataFrames must not be empty";
            sendAndVerifyCreateAnnotation(params, expectReject, expectedRejectMessage);
        }

        // createAnnotation() with calculations negative test -
        // request should be rejected because: DataTimestamps doesn't include SamplingClock or TimestampList
        {
            final String ownerId = "craigmcc";
            final List<String> dataSetIds = List.of(createDataSetScenarioResult.secondHalfDataSetId);
            final String name = "negative test";

            // create calculations for request, with 2 data frames, each with 2 columns
            final Calculations.Builder calculationsBuilder = Calculations.newBuilder();
            for (int i = 0; i < 2; i++) {

                // create data timestamps
                final DataTimestamps invalidDatatimestamps = DataTimestamps.newBuilder().build();

                // create data columns
                final List<DataColumn> dataColumns = new ArrayList<>();
                for (int j = 0; j < 2; j++) {
                    final String columnName = "calc-" + i + "-" + j;
                    final DataColumn dataColumn =
                            DataColumnUtility.dataColumnWithDoubleValues(columnName, List.of(0.0, 1.1));
                    dataColumns.add(dataColumn);
                }

                // create data frame
                final Calculations.CalculationsDataFrame dataFrame = Calculations.CalculationsDataFrame.newBuilder()
                        .setDataTimestamps(invalidDatatimestamps)
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
            final String expectedRejectMessage =
                    "CalculationDataFrame.dataTimestamps must contain either SamplingClock or TimestampList";
            sendAndVerifyCreateAnnotation(params, expectReject, expectedRejectMessage);
        }

        // createAnnotation() with calculations negative test -
        // request should be rejected because: DataColumns list is empty
        {
            final String ownerId = "craigmcc";
            final List<String> dataSetIds = List.of(createDataSetScenarioResult.secondHalfDataSetId);
            final String name = "negative test";

            // create calculations for request, with 2 data frames, each with 2 columns
            final Calculations.Builder calculationsBuilder = Calculations.newBuilder();
            for (int i = 0; i < 2; i++) {

                // create sampling clock
                final DataTimestamps dataTimestamps =
                        DataTimestampsUtility.dataTimestampsWithSamplingClock(
                                startSeconds + i, 500_000_000L, 250_000_000L, 2);

                // create data columns
                final List<DataColumn> emptyDataColumns = new ArrayList<>();

                // create data frame
                final Calculations.CalculationsDataFrame dataFrame = Calculations.CalculationsDataFrame.newBuilder()
                        .setDataTimestamps(dataTimestamps)
                        .addAllDataColumns(emptyDataColumns)
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
            final String expectedRejectMessage =
                    "CalculationDataFrame.dataColumns must not be empty";
            sendAndVerifyCreateAnnotation(params, expectReject, expectedRejectMessage);
        }

        // createAnnotation() with calculations negative test: rejected because startTime is invalid
        {
            final String ownerId = "craigmcc";
            final List<String> dataSetIds = List.of(createDataSetScenarioResult.secondHalfDataSetId);
            final String name = "negative test";

            // create calculations for request, with 2 data frames, each with 2 columns
            final Calculations.Builder calculationsBuilder = Calculations.newBuilder();
            for (int i = 0; i < 2; i++) {

                // create sampling clock
                final long invalidStartSeconds = 0L;
                final DataTimestamps dataTimestamps =
                        DataTimestampsUtility.dataTimestampsWithSamplingClock(
                                invalidStartSeconds, 500_000_000L, 250_000_000L, 2);

                // create data columns
                final List<DataColumn> dataColumns = new ArrayList<>();
                for (int j = 0; j < 2; j++) {
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
            final String expectedRejectMessage =
                    "CalculationDataFrame.dataTimestamps.samplingClock must specify startTime, periodNanos, and count";
            sendAndVerifyCreateAnnotation(params, expectReject, expectedRejectMessage);
        }

        // createAnnotation() with calculations negative test: rejected because periodNanos is invalid
        {
            final String ownerId = "craigmcc";
            final List<String> dataSetIds = List.of(createDataSetScenarioResult.secondHalfDataSetId);
            final String name = "negative test";

            // create calculations for request, with 2 data frames, each with 2 columns
            final Calculations.Builder calculationsBuilder = Calculations.newBuilder();
            for (int i = 0; i < 2; i++) {

                // create sampling clock
                final long invalidPeriodNanos = 0L;
                final DataTimestamps dataTimestamps =
                        DataTimestampsUtility.dataTimestampsWithSamplingClock(
                                startSeconds + i, 500_000_000L, invalidPeriodNanos, 2);

                // create data columns
                final List<DataColumn> dataColumns = new ArrayList<>();
                for (int j = 0; j < 2; j++) {
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
            final String expectedRejectMessage =
                    "CalculationDataFrame.dataTimestamps.samplingClock must specify startTime, periodNanos, and count";
            sendAndVerifyCreateAnnotation(params, expectReject, expectedRejectMessage);
        }

        // createAnnotation() with calculations negative test: rejected because count is invalid
        {
            final String ownerId = "craigmcc";
            final List<String> dataSetIds = List.of(createDataSetScenarioResult.secondHalfDataSetId);
            final String name = "negative test";

            // create calculations for request, with 2 data frames, each with 2 columns
            final Calculations.Builder calculationsBuilder = Calculations.newBuilder();
            for (int i = 0; i < 2; i++) {

                // create sampling clock
                final int invalidCount = 0;
                final DataTimestamps dataTimestamps =
                        DataTimestampsUtility.dataTimestampsWithSamplingClock(
                                startSeconds + i, 500_000_000L, 250_000_000L, invalidCount);

                // create data columns
                final List<DataColumn> dataColumns = new ArrayList<>();
                for (int j = 0; j < 2; j++) {
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
            final String expectedRejectMessage =
                    "CalculationDataFrame.dataTimestamps.samplingClock must specify startTime, periodNanos, and count";
            sendAndVerifyCreateAnnotation(params, expectReject, expectedRejectMessage);
        }

        // createAnnotation() with calculations negative test: rejected because DataColumn name not specified
        {
            final String ownerId = "craigmcc";
            final List<String> dataSetIds = List.of(createDataSetScenarioResult.secondHalfDataSetId);
            final String name = "negative test";

            // create calculations for request, with 2 data frames, each with 2 columns
            final Calculations.Builder calculationsBuilder = Calculations.newBuilder();
            for (int i = 0; i < 2; i++) {

                // create sampling clock
                final int count = 2;
                final DataTimestamps dataTimestamps =
                        DataTimestampsUtility.dataTimestampsWithSamplingClock(
                                startSeconds + i, 500_000_000L, 250_000_000L, count);

                // create data columns
                final List<DataColumn> dataColumns = new ArrayList<>();
                for (int j = 0; j < 2; j++) {
                    final String unspecifiedName = "";
                    final DataColumn dataColumn =
                            DataColumnUtility.dataColumnWithDoubleValues(unspecifiedName, List.of(0.0, 1.1));
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
            final String expectedRejectMessage =
                    "CalculationDataFrame.dataColumns name must be specified for each DataColumn";
            sendAndVerifyCreateAnnotation(params, expectReject, expectedRejectMessage);
        }

        // createAnnotation() with calculations negative test: rejected because DataColumn is empty
        {
            final String ownerId = "craigmcc";
            final List<String> dataSetIds = List.of(createDataSetScenarioResult.secondHalfDataSetId);
            final String name = "negative test";

            // create calculations for request, with 2 data frames, each with 2 columns
            final Calculations.Builder calculationsBuilder = Calculations.newBuilder();
            for (int i = 0; i < 2; i++) {

                // create sampling clock
                final int count = 2;
                final DataTimestamps dataTimestamps =
                        DataTimestampsUtility.dataTimestampsWithSamplingClock(
                                startSeconds + i, 500_000_000L, 250_000_000L, count);

                // create data columns
                final List<DataColumn> dataColumns = new ArrayList<>();
                for (int j = 0; j < 2; j++) {
                    final String columnName = "calc-" + i + "-" + j;
                    final DataColumn emptyColumn =
                            DataColumnUtility.dataColumnWithDoubleValues(columnName, new ArrayList<>());
                    dataColumns.add(emptyColumn);
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
            final String expectedRejectMessage =
                    "CalculationDataFrame.dataColumns contains a DataColumn with no values";
            sendAndVerifyCreateAnnotation(params, expectReject, expectedRejectMessage);
        }

        // createAnnotation() with calculations positive test using DataTimestamps.SamplingClock
        {
            final String ownerId = "craigmcc";
            final List<String> dataSetIds = List.of(createDataSetScenarioResult.secondHalfDataSetId);
            final String name = "positive test: SamplingClock";

            // create calculations for request, with 2 data frames, each with 2 columns
            final Calculations.Builder calculationsBuilder = Calculations.newBuilder();
            for (int i = 0; i < 2; i++) {

                // create sampling clock
                final DataTimestamps dataTimestamps =
                        DataTimestampsUtility.dataTimestampsWithSamplingClock(
                                startSeconds + i, 500_000_000L, 250_000_000L, 2);

                // create data columns
                final List<DataColumn> dataColumns = new ArrayList<>();
                for (int j = 0; j < 2; j++) {
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

            // queryAnnotations() positive test to verify calculations in query result annotation
            // uses calculation created above
            {
                final String nameText = "SamplingClock";
                final AnnotationTestBase.QueryAnnotationsParams queryParams =
                        new AnnotationTestBase.QueryAnnotationsParams();
                queryParams.setTextCriterion(nameText);

                sendAndVerifyQueryAnnotations(
                        queryParams,
                        expectReject,
                        expectedRejectMessage,
                        List.of(params));
            }

        }

        // createAnnotation() with calculations negative test using DataTimestamps.TimestampList
        // rejected because TimestampList is empty
        {
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

        // createAnnotation() with calculations positive test using DataTimestamps.TimestampList
        {
            final String ownerId = "craigmcc";
            final List<String> dataSetIds = List.of(createDataSetScenarioResult.secondHalfDataSetId);
            final String name = "positive test: TimestampList";

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

            // queryAnnotations() positive test to verify calculations in query result annotation
            // uses calculation created above
            {
                final String nameText = "TimestampList";
                final AnnotationTestBase.QueryAnnotationsParams queryParams =
                        new AnnotationTestBase.QueryAnnotationsParams();
                queryParams.setTextCriterion(nameText);

                sendAndVerifyQueryAnnotations(
                        queryParams,
                        expectReject,
                        expectedRejectMessage,
                        List.of(params));
            }
        }

    }

}
