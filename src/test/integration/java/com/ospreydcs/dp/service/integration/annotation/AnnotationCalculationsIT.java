package com.ospreydcs.dp.service.integration.annotation;

import ch.systemsx.cisd.hdf5.HDF5Factory;
import ch.systemsx.cisd.hdf5.IHDF5Reader;
import com.google.protobuf.ByteString;
import com.ospreydcs.dp.grpc.v1.annotation.Calculations;
import com.ospreydcs.dp.grpc.v1.annotation.ExportDataRequest;
import com.ospreydcs.dp.grpc.v1.annotation.ExportDataResponse;
import com.ospreydcs.dp.grpc.v1.annotation.Annotation;
import com.ospreydcs.dp.grpc.v1.annotation.QueryAnnotationsResponse;
import com.ospreydcs.dp.grpc.v1.common.CalculationsSpec;
import com.ospreydcs.dp.grpc.v1.common.DataColumn;
import com.ospreydcs.dp.grpc.v1.common.DataFrame;
import com.ospreydcs.dp.grpc.v1.common.DataTimestamps;
import com.ospreydcs.dp.grpc.v1.common.ArrayDimensions;
import com.ospreydcs.dp.grpc.v1.common.BoolArrayColumn;
import com.ospreydcs.dp.grpc.v1.common.BoolColumn;
import com.ospreydcs.dp.grpc.v1.common.DoubleArrayColumn;
import com.ospreydcs.dp.grpc.v1.common.DoubleColumn;
import com.ospreydcs.dp.grpc.v1.common.EnumColumn;
import com.ospreydcs.dp.grpc.v1.common.FloatArrayColumn;
import com.ospreydcs.dp.grpc.v1.common.FloatColumn;
import com.ospreydcs.dp.grpc.v1.common.ImageColumn;
import com.ospreydcs.dp.grpc.v1.common.ImageDescriptor;
import com.ospreydcs.dp.grpc.v1.common.Int32ArrayColumn;
import com.ospreydcs.dp.grpc.v1.common.Int32Column;
import com.ospreydcs.dp.grpc.v1.common.Int64ArrayColumn;
import com.ospreydcs.dp.grpc.v1.common.Int64Column;
import com.ospreydcs.dp.grpc.v1.common.SerializedDataColumn;
import com.ospreydcs.dp.grpc.v1.common.StringColumn;
import com.ospreydcs.dp.grpc.v1.common.StructColumn;
import com.ospreydcs.dp.grpc.v1.common.Timestamp;
import com.ospreydcs.dp.service.annotation.AnnotationTestBase;
import com.ospreydcs.dp.service.common.bson.calculations.CalculationsDocument;
import com.ospreydcs.dp.service.common.protobuf.DataColumnUtility;
import com.ospreydcs.dp.service.common.protobuf.DataTimestampsUtility;
import com.ospreydcs.dp.service.common.protobuf.TimestampUtility;
import com.ospreydcs.dp.service.integration.ingest.GrpcIntegrationIngestionServiceWrapper;
import org.junit.*;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.fail;

public class AnnotationCalculationsIT extends AnnotationIntegrationTestIntermediate {

    @Before
    public void setUp() throws Exception {
        super.setUp();
    }

    @After
    public void tearDown() {
        super.tearDown();
    }

    @Test
    public void testAnnotationCalculations() {

        final long startSeconds = Instant.now().getEpochSecond();
        final long startNanos = 0L;

        // ingest some data
        Map<String, GrpcIntegrationIngestionServiceWrapper.IngestionStreamInfo> validationMap =
                annotationIngestionScenario(startSeconds);

        // create some datasets
        CreateDataSetScenarioResult createDataSetScenarioResult = createDataSetScenario(startSeconds);

        // createAnnotation() with calculations negative test -
        // request should be rejected because: list of data frames is empty
        {
            final String ownerId = "craigmcc";
            final List<String> dataSetIds = List.of(createDataSetScenarioResult.secondHalfDataSetId());
            final String name = "negative test";

            // create calculations for request, with 2 data frames, each with 2 columns
            final Calculations.Builder calculationsBuilder = Calculations.newBuilder();
            final Calculations calculations = calculationsBuilder.build();

            final AnnotationTestBase.SaveAnnotationRequestParams params =
                    new AnnotationTestBase.SaveAnnotationRequestParams(
                            null, ownerId,
                            name,
                            dataSetIds,
                            null,
                            null,
                            null,
                            null,
                            calculations);

            final boolean expectReject = true;
            final String expectedRejectMessage =
                    "SaveAnnotationRequest.calculations.calculationDataFrames must not be empty";
            annotationServiceWrapper.sendAndVerifySaveAnnotation(params, false, expectReject, expectedRejectMessage);
        }

        // createAnnotation() with calculations negative test -
        // request should be rejected because: data frame name not specified
        {
            final String ownerId = "craigmcc";
            final List<String> dataSetIds = List.of(createDataSetScenarioResult.secondHalfDataSetId());
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
                        .setFrame(DataFrame.newBuilder()
                                .setDataTimestamps(dataTimestamps)
                                .addAllDataColumns(dataColumns))
                        .build();
                calculationsBuilder.addCalculationDataFrames(dataFrame);
            }
            final Calculations calculations = calculationsBuilder.build();

            final AnnotationTestBase.SaveAnnotationRequestParams params =
                    new AnnotationTestBase.SaveAnnotationRequestParams(
                            null, ownerId,
                            name,
                            dataSetIds,
                            null,
                            null,
                            null,
                            null,
                            calculations);

            final boolean expectReject = true;
            final String expectedRejectMessage = "CalculationDataFrame.name must be specified";
            annotationServiceWrapper.sendAndVerifySaveAnnotation(params, false, expectReject, expectedRejectMessage);
        }

        // createAnnotation() with calculations negative test -
        // request should be rejected because: DataTimestamps doesn't include SamplingClock or TimestampList
        {
            final String ownerId = "craigmcc";
            final List<String> dataSetIds = List.of(createDataSetScenarioResult.secondHalfDataSetId());
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
                        .setName("frame-" + i)
                        .setFrame(DataFrame.newBuilder()
                                .setDataTimestamps(invalidDatatimestamps)
                                .addAllDataColumns(dataColumns))
                        .build();
                calculationsBuilder.addCalculationDataFrames(dataFrame);
            }
            final Calculations calculations = calculationsBuilder.build();

            final AnnotationTestBase.SaveAnnotationRequestParams params =
                    new AnnotationTestBase.SaveAnnotationRequestParams(
                            null, ownerId,
                            name,
                            dataSetIds,
                            null,
                            null,
                            null,
                            null,
                            calculations);

            final boolean expectReject = true;
            final String expectedRejectMessage =
                    "CalculationDataFrame.dataTimestamps must contain either SamplingClock or TimestampList";
            annotationServiceWrapper.sendAndVerifySaveAnnotation(params, false, expectReject, expectedRejectMessage);
        }

        // createAnnotation() with calculations negative test -
        // request should be rejected because: DataTimestamps doesn't include SamplingClock or TimestampList
        {
            final String ownerId = "craigmcc";
            final List<String> dataSetIds = List.of(createDataSetScenarioResult.secondHalfDataSetId());
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
                        .setName("frame-" + i)
                        .setFrame(DataFrame.newBuilder()
                                .setDataTimestamps(invalidDatatimestamps)
                                .addAllDataColumns(dataColumns))
                        .build();
                calculationsBuilder.addCalculationDataFrames(dataFrame);
            }
            final Calculations calculations = calculationsBuilder.build();

            final AnnotationTestBase.SaveAnnotationRequestParams params =
                    new AnnotationTestBase.SaveAnnotationRequestParams(
                            null, ownerId,
                            name,
                            dataSetIds,
                            null,
                            null,
                            null,
                            null,
                            calculations);

            final boolean expectReject = true;
            final String expectedRejectMessage =
                    "CalculationDataFrame.dataTimestamps must contain either SamplingClock or TimestampList";
            annotationServiceWrapper.sendAndVerifySaveAnnotation(params, false, expectReject, expectedRejectMessage);
        }

        // createAnnotation() with calculations negative test -
        // request should be rejected because: DataColumns list is empty
        {
            final String ownerId = "craigmcc";
            final List<String> dataSetIds = List.of(createDataSetScenarioResult.secondHalfDataSetId());
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
                        .setName("frame-" + i)
                        .setFrame(DataFrame.newBuilder()
                                .setDataTimestamps(dataTimestamps)
                                .addAllDataColumns(emptyDataColumns))
                        .build();
                calculationsBuilder.addCalculationDataFrames(dataFrame);
            }
            final Calculations calculations = calculationsBuilder.build();

            final AnnotationTestBase.SaveAnnotationRequestParams params =
                    new AnnotationTestBase.SaveAnnotationRequestParams(
                            null, ownerId,
                            name,
                            dataSetIds,
                            null,
                            null,
                            null,
                            null,
                            calculations);

            final boolean expectReject = true;
            final String expectedRejectMessage =
                    "CalculationDataFrame must include at least one column of any type: frame-0";
            annotationServiceWrapper.sendAndVerifySaveAnnotation(params, false, expectReject, expectedRejectMessage);
        }

        // createAnnotation() with calculations negative test: rejected because startTime is invalid
        {
            final String ownerId = "craigmcc";
            final List<String> dataSetIds = List.of(createDataSetScenarioResult.secondHalfDataSetId());
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
                        .setName("frame-" + i)
                        .setFrame(DataFrame.newBuilder()
                                .setDataTimestamps(dataTimestamps)
                                .addAllDataColumns(dataColumns))
                        .build();
                calculationsBuilder.addCalculationDataFrames(dataFrame);
            }
            final Calculations calculations = calculationsBuilder.build();

            final AnnotationTestBase.SaveAnnotationRequestParams params =
                    new AnnotationTestBase.SaveAnnotationRequestParams(
                            null, ownerId,
                            name,
                            dataSetIds,
                            null,
                            null,
                            null,
                            null,
                            calculations);

            final boolean expectReject = true;
            final String expectedRejectMessage =
                    "CalculationDataFrame.dataTimestamps.samplingClock must specify startTime, periodNanos, and count";
            annotationServiceWrapper.sendAndVerifySaveAnnotation(params, false, expectReject, expectedRejectMessage);
        }

        // createAnnotation() with calculations negative test: rejected because periodNanos is invalid
        {
            final String ownerId = "craigmcc";
            final List<String> dataSetIds = List.of(createDataSetScenarioResult.secondHalfDataSetId());
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
                        .setName("frame-" + i)
                        .setFrame(DataFrame.newBuilder()
                                .setDataTimestamps(dataTimestamps)
                                .addAllDataColumns(dataColumns))
                        .build();
                calculationsBuilder.addCalculationDataFrames(dataFrame);
            }
            final Calculations calculations = calculationsBuilder.build();

            final AnnotationTestBase.SaveAnnotationRequestParams params =
                    new AnnotationTestBase.SaveAnnotationRequestParams(
                            null, ownerId,
                            name,
                            dataSetIds,
                            null,
                            null,
                            null,
                            null,
                            calculations);

            final boolean expectReject = true;
            final String expectedRejectMessage =
                    "CalculationDataFrame.dataTimestamps.samplingClock must specify startTime, periodNanos, and count";
            annotationServiceWrapper.sendAndVerifySaveAnnotation(params, false, expectReject, expectedRejectMessage);
        }

        // createAnnotation() with calculations negative test: rejected because count is invalid
        {
            final String ownerId = "craigmcc";
            final List<String> dataSetIds = List.of(createDataSetScenarioResult.secondHalfDataSetId());
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
                        .setName("frame-" + i)
                        .setFrame(DataFrame.newBuilder()
                                .setDataTimestamps(dataTimestamps)
                                .addAllDataColumns(dataColumns))
                        .build();
                calculationsBuilder.addCalculationDataFrames(dataFrame);
            }
            final Calculations calculations = calculationsBuilder.build();

            final AnnotationTestBase.SaveAnnotationRequestParams params =
                    new AnnotationTestBase.SaveAnnotationRequestParams(
                            null, ownerId,
                            name,
                            dataSetIds,
                            null,
                            null,
                            null,
                            null,
                            calculations);

            final boolean expectReject = true;
            final String expectedRejectMessage =
                    "CalculationDataFrame.dataTimestamps.samplingClock must specify startTime, periodNanos, and count";
            annotationServiceWrapper.sendAndVerifySaveAnnotation(params, false, expectReject, expectedRejectMessage);
        }

        // createAnnotation() with calculations negative test: rejected because DataColumn name not specified
        {
            final String ownerId = "craigmcc";
            final List<String> dataSetIds = List.of(createDataSetScenarioResult.secondHalfDataSetId());
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
                        .setName("frame-" + i)
                        .setFrame(DataFrame.newBuilder()
                                .setDataTimestamps(dataTimestamps)
                                .addAllDataColumns(dataColumns))
                        .build();
                calculationsBuilder.addCalculationDataFrames(dataFrame);
            }
            final Calculations calculations = calculationsBuilder.build();

            final AnnotationTestBase.SaveAnnotationRequestParams params =
                    new AnnotationTestBase.SaveAnnotationRequestParams(
                            null, ownerId,
                            name,
                            dataSetIds,
                            null,
                            null,
                            null,
                            null,
                            calculations);

            final boolean expectReject = true;
            final String expectedRejectMessage =
                    "CalculationDataFrame.dataColumns name must be specified for each DataColumn";
            annotationServiceWrapper.sendAndVerifySaveAnnotation(params, false, expectReject, expectedRejectMessage);
        }

        // createAnnotation() with calculations negative test: rejected because DataColumn is empty
        {
            final String ownerId = "craigmcc";
            final List<String> dataSetIds = List.of(createDataSetScenarioResult.secondHalfDataSetId());
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
                        .setName("frame-" + i)
                        .setFrame(DataFrame.newBuilder()
                                .setDataTimestamps(dataTimestamps)
                                .addAllDataColumns(dataColumns))
                        .build();
                calculationsBuilder.addCalculationDataFrames(dataFrame);
            }
            final Calculations calculations = calculationsBuilder.build();

            final AnnotationTestBase.SaveAnnotationRequestParams params =
                    new AnnotationTestBase.SaveAnnotationRequestParams(
                            null, ownerId,
                            name,
                            dataSetIds,
                            null,
                            null,
                            null,
                            null,
                            calculations);

            final boolean expectReject = true;
            final String expectedRejectMessage =
                    "CalculationDataFrame.dataColumns contains a DataColumn with no values";
            annotationServiceWrapper.sendAndVerifySaveAnnotation(params, false, expectReject, expectedRejectMessage);
        }

        // createAnnotation() with calculations negative test -
        // request should be rejected because: column value count doesn't match frame timestamp count (#248 D28)
        {
            final String ownerId = "craigmcc";
            final List<String> dataSetIds = List.of(createDataSetScenarioResult.secondHalfDataSetId());
            final String name = "negative test";

            // sampling clock specifies 2 samples, but the column carries 3 values
            final DataTimestamps dataTimestamps =
                    DataTimestampsUtility.dataTimestampsWithSamplingClock(
                            startSeconds, 500_000_000L, 250_000_000L, 2);
            final DataColumn shortColumn =
                    DataColumnUtility.dataColumnWithDoubleValues("calc-0-0", List.of(0.0, 1.1, 2.2));
            final Calculations calculations = Calculations.newBuilder()
                    .addCalculationDataFrames(Calculations.CalculationsDataFrame.newBuilder()
                            .setName("frame-0")
                            .setFrame(DataFrame.newBuilder()
                                    .setDataTimestamps(dataTimestamps)
                                    .addDataColumns(shortColumn)))
                    .build();

            final AnnotationTestBase.SaveAnnotationRequestParams params =
                    new AnnotationTestBase.SaveAnnotationRequestParams(
                            null, ownerId,
                            name,
                            dataSetIds,
                            null,
                            null,
                            null,
                            null,
                            calculations);

            final boolean expectReject = true;
            final String expectedRejectMessage =
                    "CalculationDataFrame.dataColumns values count mismatch: expected 2, got: 3 for column: calc-0-0";
            annotationServiceWrapper.sendAndVerifySaveAnnotation(params, false, expectReject, expectedRejectMessage);
        }

        // createAnnotation() with calculations negative test -
        // request should be rejected because: duplicate frame names (#248 D28)
        {
            final String ownerId = "craigmcc";
            final List<String> dataSetIds = List.of(createDataSetScenarioResult.secondHalfDataSetId());
            final String name = "negative test";

            final Calculations.Builder calculationsBuilder = Calculations.newBuilder();
            for (int i = 0; i < 2; i++) {
                final DataTimestamps dataTimestamps =
                        DataTimestampsUtility.dataTimestampsWithSamplingClock(
                                startSeconds + i, 500_000_000L, 250_000_000L, 2);
                final DataColumn dataColumn =
                        DataColumnUtility.dataColumnWithDoubleValues("calc-" + i, List.of(0.0, 1.1));
                calculationsBuilder.addCalculationDataFrames(Calculations.CalculationsDataFrame.newBuilder()
                        .setName("duplicate-frame")
                        .setFrame(DataFrame.newBuilder()
                                .setDataTimestamps(dataTimestamps)
                                .addDataColumns(dataColumn)));
            }
            final Calculations calculations = calculationsBuilder.build();

            final AnnotationTestBase.SaveAnnotationRequestParams params =
                    new AnnotationTestBase.SaveAnnotationRequestParams(
                            null, ownerId,
                            name,
                            dataSetIds,
                            null,
                            null,
                            null,
                            null,
                            calculations);

            final boolean expectReject = true;
            final String expectedRejectMessage =
                    "SaveAnnotationRequest.calculations.calculationDataFrames contains duplicate frame name: duplicate-frame";
            annotationServiceWrapper.sendAndVerifySaveAnnotation(params, false, expectReject, expectedRejectMessage);
        }

        // createAnnotation() with calculations negative test -
        // request should be rejected because: duplicate column names within a frame (#248 D28)
        {
            final String ownerId = "craigmcc";
            final List<String> dataSetIds = List.of(createDataSetScenarioResult.secondHalfDataSetId());
            final String name = "negative test";

            // duplicate name across column types: a legacy DataColumn and a DoubleColumn
            final DataTimestamps dataTimestamps =
                    DataTimestampsUtility.dataTimestampsWithSamplingClock(
                            startSeconds, 500_000_000L, 250_000_000L, 2);
            final DataColumn dataColumn =
                    DataColumnUtility.dataColumnWithDoubleValues("calc-dup", List.of(0.0, 1.1));
            final DoubleColumn doubleColumn = DoubleColumn.newBuilder()
                    .setName("calc-dup")
                    .addAllValues(List.of(2.2, 3.3))
                    .build();
            final Calculations calculations = Calculations.newBuilder()
                    .addCalculationDataFrames(Calculations.CalculationsDataFrame.newBuilder()
                            .setName("frame-0")
                            .setFrame(DataFrame.newBuilder()
                                    .setDataTimestamps(dataTimestamps)
                                    .addDataColumns(dataColumn)
                                    .addDoubleColumns(doubleColumn)))
                    .build();

            final AnnotationTestBase.SaveAnnotationRequestParams params =
                    new AnnotationTestBase.SaveAnnotationRequestParams(
                            null, ownerId,
                            name,
                            dataSetIds,
                            null,
                            null,
                            null,
                            null,
                            calculations);

            final boolean expectReject = true;
            final String expectedRejectMessage =
                    "CalculationDataFrame contains duplicate column name: calc-dup in frame: frame-0";
            annotationServiceWrapper.sendAndVerifySaveAnnotation(params, false, expectReject, expectedRejectMessage);
        }

        // createAnnotation() with calculations positive test using DataTimestamps.SamplingClock.
        // Also provides positive and negative coverage for exporting calculations.
        {
            final String ownerId = "craigmcc";
            final List<String> dataSetIds = List.of(createDataSetScenarioResult.secondHalfDataSetId());
            final String name = "positive test: SamplingClock";

            // Create calculations for request, with 6 data frames, each with 2 columns.
            // Each data frame includes data values for one second of data.
            final Calculations.Builder calculationsBuilder = Calculations.newBuilder();
            for (int i = 0; i < 6; i++) {

                // create sampling clock
                // specifying 10 values per second (in the upper half second, every 20th of a second)
                final DataTimestamps dataTimestamps =
                        DataTimestampsUtility.dataTimestampsWithSamplingClock(
                                startSeconds + i, 500_000_000L, 50_000_000L, 10);

                // create data columns, each with 10 values
                final List<DataColumn> dataColumns = new ArrayList<>();
                for (int j = 0; j < 2; j++) {
                    final String columnName = "calc-" + i + "-" + j;
                    final DataColumn dataColumn =
                            DataColumnUtility.dataColumnWithDoubleValues(
                                    columnName,
                                    List.of(.50, .55, .60, .65, .70, .75, .80, .85, .90, .95));
                    dataColumns.add(dataColumn);
                }

                // create data frame
                final Calculations.CalculationsDataFrame dataFrame = Calculations.CalculationsDataFrame.newBuilder()
                        .setName("frame-" + i)
                        .setFrame(DataFrame.newBuilder()
                                .setDataTimestamps(dataTimestamps)
                                .addAllDataColumns(dataColumns))
                        .build();
                calculationsBuilder.addCalculationDataFrames(dataFrame);
            }
            final Calculations calculations = calculationsBuilder.build();

            final AnnotationTestBase.SaveAnnotationRequestParams createAnnotationRequestParams =
                    new AnnotationTestBase.SaveAnnotationRequestParams(
                            null, ownerId,
                            name,
                            dataSetIds,
                            null,
                            null,
                            null,
                            null,
                            calculations);

            final boolean expectReject = false;
            final String expectedRejectMessage = "";
            annotationServiceWrapper.sendAndVerifySaveAnnotation(createAnnotationRequestParams, false, expectReject, expectedRejectMessage);

            // queryAnnotations() positive test to verify calculations in query result annotation.
            // Result includes calculations id for annotation created above.
            String calculationsId;
            {
                final String nameText = "SamplingClock";
                final AnnotationTestBase.QueryAnnotationsParams queryAnnotationsParams =
                        new AnnotationTestBase.QueryAnnotationsParams();
                queryAnnotationsParams.setTextCriterion(nameText);

                List<Annotation> queryResultAnnotations =
                        annotationServiceWrapper.sendAndVerifyQueryAnnotations(
                                queryAnnotationsParams,
                                expectReject,
                                expectedRejectMessage,
                                List.of(createAnnotationRequestParams));
                assertEquals(1, queryResultAnnotations.size());
                calculationsId = queryResultAnnotations.get(0).getCalculationsId();
            }

            // positive export test: export of dataset with calculations to csv.
            {
                // create CalculationsSpec with calculations id from query result
                CalculationsSpec calculationsSpec = CalculationsSpec.newBuilder()
                        .setCalculationsId(calculationsId)
                        .build();

                ExportDataResponse.ExportDataResult exportResult =
                        annotationServiceWrapper.sendAndVerifyExportData(
                                createDataSetScenarioResult.secondHalfDataSetId(),
                                createDataSetScenarioResult.secondHalfDataSetParams(),
                                calculationsSpec,
                                calculations,
                                ExportDataRequest.ExportOutputFormat.EXPORT_FORMAT_CSV,
                                10, // expect 10 buckets (2 pvs, 5 seconds, 1 bucket per second)
                                50, // 10 rows per second * 5 seconds (5 rows pv and calculations, 5 rows calculations)
                                validationMap,
                                false,
                                "");
            }

            // positive export test: export of only calculations (without dataset) to csv.
            {
                // create CalculationsSpec with calculations id from query result
                CalculationsSpec calculationsSpec = CalculationsSpec.newBuilder()
                        .setCalculationsId(calculationsId)
                        .build();

                ExportDataResponse.ExportDataResult exportResult =
                        annotationServiceWrapper.sendAndVerifyExportData(
                                null,
                                null,
                                calculationsSpec,
                                calculations,
                                ExportDataRequest.ExportOutputFormat.EXPORT_FORMAT_CSV,
                                0,
                                60, // 10 rows per second, 6 seconds
                                null,
                                false,
                                "");
            }

            // positive export test: export to csv filtering calculations columns using CalculationsSpec column map.
            {

                // create frame column map for filtering
                final Map<String, CalculationsSpec.ColumnNameList> dataFrameColumnsMap = new HashMap<>();
                final String frame1Name = "frame-2";
                final List<String> frame1Columns = List.of("calc-2-0", "calc-2-1");
                final CalculationsSpec.ColumnNameList frame1ColumnNameList = CalculationsSpec.ColumnNameList.newBuilder()
                        .addAllColumnNames(frame1Columns)
                        .build();
                dataFrameColumnsMap.put(frame1Name, frame1ColumnNameList);
                final String frame2Name = "frame-3";
                final List<String> frame2Columns = List.of("calc-3-1");
                final CalculationsSpec.ColumnNameList frame2ColumnNameList = CalculationsSpec.ColumnNameList.newBuilder()
                        .addAllColumnNames(frame2Columns)
                        .build();
                dataFrameColumnsMap.put(frame2Name, frame2ColumnNameList);

                // create CalculationsSpec with calculations id from query result
                CalculationsSpec calculationsSpec = CalculationsSpec.newBuilder()
                        .setCalculationsId(calculationsId)
                        .putAllDataFrameColumns(dataFrameColumnsMap)
                        .build();

                ExportDataResponse.ExportDataResult exportResult =
                        annotationServiceWrapper.sendAndVerifyExportData(
                                null,
                                null,
                                calculationsSpec,
                                calculations,
                                ExportDataRequest.ExportOutputFormat.EXPORT_FORMAT_CSV,
                                0,
                                20, // 10 rows per second, 2 seconds
                                null,
                                false,
                                "");
            }

            // positive export test: export of dataset with calculations to xlsx.
            {
                // create CalculationsSpec with calculations id from query result
                CalculationsSpec calculationsSpec = CalculationsSpec.newBuilder()
                        .setCalculationsId(calculationsId)
                        .build();

                ExportDataResponse.ExportDataResult exportResult =
                        annotationServiceWrapper.sendAndVerifyExportData(
                                createDataSetScenarioResult.secondHalfDataSetId(),
                                createDataSetScenarioResult.secondHalfDataSetParams(),
                                calculationsSpec,
                                calculations,
                                ExportDataRequest.ExportOutputFormat.EXPORT_FORMAT_XLSX,
                                10, // expect 10 buckets (2 pvs, 5 seconds, 1 bucket per second)
                                50, // 10 rows per second * 5 seconds (5 rows pv and calculations, 5 rows calculations)
                                validationMap,
                                false,
                                "");
            }

            // positive export test: export of only calculations (without dataset) to xlsx.
            {
                // create CalculationsSpec with calculations id from query result
                CalculationsSpec calculationsSpec = CalculationsSpec.newBuilder()
                        .setCalculationsId(calculationsId)
                        .build();

                ExportDataResponse.ExportDataResult exportResult =
                        annotationServiceWrapper.sendAndVerifyExportData(
                                null,
                                null,
                                calculationsSpec,
                                calculations,
                                ExportDataRequest.ExportOutputFormat.EXPORT_FORMAT_XLSX,
                                0,
                                60, // 10 rows per second, 6 seconds
                                null,
                                false,
                                "");
            }

            // positive export test: export to xlsx filtering calculations columns using CalculationsSpec column map.
            {

                // create frame column map for filtering
                final Map<String, CalculationsSpec.ColumnNameList> dataFrameColumnsMap = new HashMap<>();
                final String frame1Name = "frame-2";
                final List<String> frame1Columns = List.of("calc-2-0", "calc-2-1");
                final CalculationsSpec.ColumnNameList frame1ColumnNameList = CalculationsSpec.ColumnNameList.newBuilder()
                        .addAllColumnNames(frame1Columns)
                        .build();
                dataFrameColumnsMap.put(frame1Name, frame1ColumnNameList);
                final String frame2Name = "frame-3";
                final List<String> frame2Columns = List.of("calc-3-1");
                final CalculationsSpec.ColumnNameList frame2ColumnNameList = CalculationsSpec.ColumnNameList.newBuilder()
                        .addAllColumnNames(frame2Columns)
                        .build();
                dataFrameColumnsMap.put(frame2Name, frame2ColumnNameList);

                // create CalculationsSpec with calculations id from query result
                CalculationsSpec calculationsSpec = CalculationsSpec.newBuilder()
                        .setCalculationsId(calculationsId)
                        .putAllDataFrameColumns(dataFrameColumnsMap)
                        .build();

                ExportDataResponse.ExportDataResult exportResult =
                        annotationServiceWrapper.sendAndVerifyExportData(
                                null,
                                null,
                                calculationsSpec,
                                calculations,
                                ExportDataRequest.ExportOutputFormat.EXPORT_FORMAT_XLSX,
                                0,
                                20, // 10 rows per second, 2 seconds
                                null,
                                false,
                                "");
            }

            // positive export test: export of dataset with calculations to hdf5.
            {
                // create CalculationsSpec with calculations id from query result
                CalculationsSpec calculationsSpec = CalculationsSpec.newBuilder()
                        .setCalculationsId(calculationsId)
                        .build();

                ExportDataResponse.ExportDataResult exportResult =
                        annotationServiceWrapper.sendAndVerifyExportData(
                                createDataSetScenarioResult.secondHalfDataSetId(),
                                createDataSetScenarioResult.secondHalfDataSetParams(),
                                calculationsSpec,
                                calculations,
                                ExportDataRequest.ExportOutputFormat.EXPORT_FORMAT_HDF5,
                                10, // expect 10 buckets (2 pvs, 5 seconds, 1 bucket per second)
                                0,
                                validationMap,
                                false,
                                "");
            }

            // positive export test: export of only calculations (without dataset) to hdf5.
            {
                // create CalculationsSpec with calculations id from query result
                CalculationsSpec calculationsSpec = CalculationsSpec.newBuilder()
                        .setCalculationsId(calculationsId)
                        .build();

                ExportDataResponse.ExportDataResult exportResult =
                        annotationServiceWrapper.sendAndVerifyExportData(
                                null,
                                null,
                                calculationsSpec,
                                calculations,
                                ExportDataRequest.ExportOutputFormat.EXPORT_FORMAT_HDF5,
                                10,
                                0, // 10 rows per second, 6 seconds
                                null,
                                false,
                                "");
            }

            // positive export test: export to hdf5 filtering calculations columns using CalculationsSpec column map.
            {

                // create frame column map for filtering
                final Map<String, CalculationsSpec.ColumnNameList> dataFrameColumnsMap = new HashMap<>();
                final String frame1Name = "frame-2";
                final List<String> frame1Columns = List.of("calc-2-0", "calc-2-1");
                final CalculationsSpec.ColumnNameList frame1ColumnNameList = CalculationsSpec.ColumnNameList.newBuilder()
                        .addAllColumnNames(frame1Columns)
                        .build();
                dataFrameColumnsMap.put(frame1Name, frame1ColumnNameList);
                final String frame2Name = "frame-3";
                final List<String> frame2Columns = List.of("calc-3-1");
                final CalculationsSpec.ColumnNameList frame2ColumnNameList = CalculationsSpec.ColumnNameList.newBuilder()
                        .addAllColumnNames(frame2Columns)
                        .build();
                dataFrameColumnsMap.put(frame2Name, frame2ColumnNameList);

                // create CalculationsSpec with calculations id from query result
                CalculationsSpec calculationsSpec = CalculationsSpec.newBuilder()
                        .setCalculationsId(calculationsId)
                        .putAllDataFrameColumns(dataFrameColumnsMap)
                        .build();

                ExportDataResponse.ExportDataResult exportResult =
                        annotationServiceWrapper.sendAndVerifyExportData(
                                null,
                                null,
                                calculationsSpec,
                                calculations,
                                ExportDataRequest.ExportOutputFormat.EXPORT_FORMAT_HDF5,
                                10,
                                0,
                                null,
                                false,
                                "");
            }

            // negative export test: blank calculations id
            {
                // create CalculationsSpec with calculations id from query result
                CalculationsSpec calculationsSpec = CalculationsSpec.newBuilder()
                        .setCalculationsId("")
                        .build();

                ExportDataResponse.ExportDataResult exportResult =
                        annotationServiceWrapper.sendAndVerifyExportData(
                                createDataSetScenarioResult.secondHalfDataSetId(),
                                createDataSetScenarioResult.secondHalfDataSetParams(),
                                calculationsSpec,
                                null,
                                ExportDataRequest.ExportOutputFormat.EXPORT_FORMAT_CSV,
                                0,
                                0,
                                validationMap,
                                true,
                                "ExportDataRequest.calculationsSpec.calculationsId must be specified");
            }

            // negative export test: invalid calculations id
            {
                // create CalculationsSpec with calculations id from query result
                CalculationsSpec calculationsSpec = CalculationsSpec.newBuilder()
                        .setCalculationsId("abcde12345")
                        .build();

                ExportDataResponse.ExportDataResult exportResult =
                        annotationServiceWrapper.sendAndVerifyExportData(
                                createDataSetScenarioResult.secondHalfDataSetId(),
                                createDataSetScenarioResult.secondHalfDataSetParams(),
                                calculationsSpec,
                                null,
                                ExportDataRequest.ExportOutputFormat.EXPORT_FORMAT_CSV,
                                0,
                                0,
                                validationMap,
                                true,
                                "ExportDataRequest.calculationsSpec.calculationsId is not a valid id: abcde12345");
            }

            // negative export test: empty column name list in CalculationsSpec column map for
            // filtering calculations columns in export
            {

                // create frame column map for filtering
                final Map<String, CalculationsSpec.ColumnNameList> dataFrameColumnsMap = new HashMap<>();
                final String frame1Name = "frame-2";
                final List<String> frame1Columns = List.of();
                final CalculationsSpec.ColumnNameList frame1ColumnNameList = CalculationsSpec.ColumnNameList.newBuilder()
                        .addAllColumnNames(frame1Columns)
                        .build();
                dataFrameColumnsMap.put(frame1Name, frame1ColumnNameList);

                // create CalculationsSpec with calculations id from query result
                CalculationsSpec calculationsSpec = CalculationsSpec.newBuilder()
                        .setCalculationsId(calculationsId)
                        .putAllDataFrameColumns(dataFrameColumnsMap)
                        .build();

                ExportDataResponse.ExportDataResult exportResult =
                        annotationServiceWrapper.sendAndVerifyExportData(
                                null,
                                null,
                                calculationsSpec,
                                calculations,
                                ExportDataRequest.ExportOutputFormat.EXPORT_FORMAT_CSV,
                                0,
                                0, // 10 rows per second, 2 seconds
                                null,
                                true,
                                "ExportDataRequest.calculationsSpec.dataFrameColumns list must not be empty");
            }

            // negative export test: blank column name in CalculationsSpec column map for
            // filtering calculations columns in export
            {

                // create frame column map for filtering
                final Map<String, CalculationsSpec.ColumnNameList> dataFrameColumnsMap = new HashMap<>();
                final String frame1Name = "frame-2";
                final List<String> frame1Columns = List.of("");
                final CalculationsSpec.ColumnNameList frame1ColumnNameList = CalculationsSpec.ColumnNameList.newBuilder()
                        .addAllColumnNames(frame1Columns)
                        .build();
                dataFrameColumnsMap.put(frame1Name, frame1ColumnNameList);

                // create CalculationsSpec with calculations id from query result
                CalculationsSpec calculationsSpec = CalculationsSpec.newBuilder()
                        .setCalculationsId(calculationsId)
                        .putAllDataFrameColumns(dataFrameColumnsMap)
                        .build();

                ExportDataResponse.ExportDataResult exportResult =
                        annotationServiceWrapper.sendAndVerifyExportData(
                                null,
                                null,
                                calculationsSpec,
                                calculations,
                                ExportDataRequest.ExportOutputFormat.EXPORT_FORMAT_CSV,
                                0,
                                0, // 10 rows per second, 2 seconds
                                null,
                                true,
                                "ExportDataRequest.calculationsSpec.dataFrameColumns includes blank column name");
            }

            // negative export test: CalculationsSpec frameColumnNames map uses invalid frame name.
            {

                // create frame column map for filtering
                final Map<String, CalculationsSpec.ColumnNameList> dataFrameColumnsMap = new HashMap<>();
                final String frame1Name = "junk";
                final List<String> frame1Columns = List.of("calc-2-0", "calc-2-1");
                final CalculationsSpec.ColumnNameList frame1ColumnNameList = CalculationsSpec.ColumnNameList.newBuilder()
                        .addAllColumnNames(frame1Columns)
                        .build();
                dataFrameColumnsMap.put(frame1Name, frame1ColumnNameList);
                final String frame2Name = "frame-3";
                final List<String> frame2Columns = List.of("calc-3-1");
                final CalculationsSpec.ColumnNameList frame2ColumnNameList = CalculationsSpec.ColumnNameList.newBuilder()
                        .addAllColumnNames(frame2Columns)
                        .build();
                dataFrameColumnsMap.put(frame2Name, frame2ColumnNameList);

                // create CalculationsSpec with calculations id from query result
                CalculationsSpec calculationsSpec = CalculationsSpec.newBuilder()
                        .setCalculationsId(calculationsId)
                        .putAllDataFrameColumns(dataFrameColumnsMap)
                        .build();

                ExportDataResponse.ExportDataResult exportResult =
                        annotationServiceWrapper.sendAndVerifyExportData(
                                null,
                                null,
                                calculationsSpec,
                                calculations,
                                ExportDataRequest.ExportOutputFormat.EXPORT_FORMAT_CSV,
                                0,
                                20, // 10 rows per second, 2 seconds
                                null,
                                true,
                                "ExportDataRequest.CalculationsSpec.dataFrameColumns includes invalid frame name: junk");
            }

            // negative export test: CalculationsSpec frameColumnNames map uses invalid column name for frame.
            {

                // create frame column map for filtering
                final Map<String, CalculationsSpec.ColumnNameList> dataFrameColumnsMap = new HashMap<>();
                final String frame1Name = "frame-2";
                final List<String> frame1Columns = List.of("calc-2-0", "junk");
                final CalculationsSpec.ColumnNameList frame1ColumnNameList = CalculationsSpec.ColumnNameList.newBuilder()
                        .addAllColumnNames(frame1Columns)
                        .build();
                dataFrameColumnsMap.put(frame1Name, frame1ColumnNameList);
                final String frame2Name = "frame-3";
                final List<String> frame2Columns = List.of("calc-3-1");
                final CalculationsSpec.ColumnNameList frame2ColumnNameList = CalculationsSpec.ColumnNameList.newBuilder()
                        .addAllColumnNames(frame2Columns)
                        .build();
                dataFrameColumnsMap.put(frame2Name, frame2ColumnNameList);

                // create CalculationsSpec with calculations id from query result
                CalculationsSpec calculationsSpec = CalculationsSpec.newBuilder()
                        .setCalculationsId(calculationsId)
                        .putAllDataFrameColumns(dataFrameColumnsMap)
                        .build();

                ExportDataResponse.ExportDataResult exportResult =
                        annotationServiceWrapper.sendAndVerifyExportData(
                                null,
                                null,
                                calculationsSpec,
                                calculations,
                                ExportDataRequest.ExportOutputFormat.EXPORT_FORMAT_CSV,
                                0,
                                20, // 10 rows per second, 2 seconds
                                null,
                                true,
                                "ExportDataRequest.CalculationsSpec.dataFrameColumns includes invalid column name: junk for frame: frame-2");
            }

        }

        // createAnnotation() with calculations positive test where calculations time range doesn't overlap
        // dataset time range
        {
            final String ownerId = "craigmcc";
            final List<String> dataSetIds = List.of(createDataSetScenarioResult.secondHalfDataSetId());
            final String name = "calculations time range doesn't overlap dataset time range";

            // Create calculations for request, with 6 data frames, each with 2 columns.
            // Each data frame includes data values for one second of data.
            final Calculations.Builder calculationsBuilder = Calculations.newBuilder();
            for (int i = 0; i < 6; i++) {

                // create sampling clock
                // specifying 10 values per second (in the upper half second, every 20th of a second)
                final DataTimestamps dataTimestamps =
                        DataTimestampsUtility.dataTimestampsWithSamplingClock(
                                startSeconds + i+10,
                                500_000_000L,
                                50_000_000L,
                                10);

                // create data columns, each with 10 values
                final List<DataColumn> dataColumns = new ArrayList<>();
                for (int j = 0; j < 2; j++) {
                    final String columnName = "calc-" + i + "-" + j;
                    final DataColumn dataColumn =
                            DataColumnUtility.dataColumnWithDoubleValues(
                                    columnName,
                                    List.of(.50, .55, .60, .65, .70, .75, .80, .85, .90, .95));
                    dataColumns.add(dataColumn);
                }

                // create data frame
                final Calculations.CalculationsDataFrame dataFrame = Calculations.CalculationsDataFrame.newBuilder()
                        .setName("frame-" + i)
                        .setFrame(DataFrame.newBuilder()
                                .setDataTimestamps(dataTimestamps)
                                .addAllDataColumns(dataColumns))
                        .build();
                calculationsBuilder.addCalculationDataFrames(dataFrame);
            }
            final Calculations calculations = calculationsBuilder.build();

            final AnnotationTestBase.SaveAnnotationRequestParams createAnnotationRequestParams =
                    new AnnotationTestBase.SaveAnnotationRequestParams(
                            null, ownerId,
                            name,
                            dataSetIds,
                            null,
                            null,
                            null,
                            null,
                            calculations);

            final boolean expectReject = false;
            final String expectedRejectMessage = "";
            annotationServiceWrapper.sendAndVerifySaveAnnotation(createAnnotationRequestParams, false, expectReject, expectedRejectMessage);

            // queryAnnotations() positive test to verify calculations in query result annotation.
            // Result includes calculations id for annotation created above.
            String calculationsId;
            {
                final String nameText = "time range";
                final AnnotationTestBase.QueryAnnotationsParams queryAnnotationsParams =
                        new AnnotationTestBase.QueryAnnotationsParams();
                queryAnnotationsParams.setTextCriterion(nameText);

                List<Annotation> queryResultAnnotations =
                        annotationServiceWrapper.sendAndVerifyQueryAnnotations(
                                queryAnnotationsParams,
                                expectReject,
                                expectedRejectMessage,
                                List.of(createAnnotationRequestParams));
                assertEquals(1, queryResultAnnotations.size());
                calculationsId = queryResultAnnotations.get(0).getCalculationsId();
            }

            // Positive export test: export where time range of calculations doesn't overlap time range of dataset.
            // Export output file contains data for the dataset with empty columns for the calculations.
            {
                // create CalculationsSpec with calculations id from query result
                CalculationsSpec calculationsSpec = CalculationsSpec.newBuilder()
                        .setCalculationsId(calculationsId)
                        .build();

                ExportDataResponse.ExportDataResult exportResult =
                        annotationServiceWrapper.sendAndVerifyExportData(
                                createDataSetScenarioResult.secondHalfDataSetId(),
                                createDataSetScenarioResult.secondHalfDataSetParams(),
                                calculationsSpec,
                                calculations,
                                ExportDataRequest.ExportOutputFormat.EXPORT_FORMAT_CSV,
                                10, // expect 10 buckets (2 pvs, 5 seconds, 1 bucket per second)
                                25, // 5 rows per second for specified dataset, no calculations rows
                                validationMap,
                                false,
                                "");

                System.err.println("dataset id for non-overlapping time range: " + createDataSetScenarioResult.secondHalfDataSetId());
            }

        }

        // createAnnotation() with calculations negative test using DataTimestamps.TimestampList
        // rejected because TimestampList is empty
        {
            final String ownerId = "craigmcc";
            final List<String> dataSetIds = List.of(createDataSetScenarioResult.secondHalfDataSetId());
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
                        .setName("frame-" + i)
                        .setFrame(DataFrame.newBuilder()
                                .setDataTimestamps(dataTimestamps)
                                .addAllDataColumns(dataColumns))
                        .build();
                calculationsBuilder.addCalculationDataFrames(dataFrame);
            }
            final Calculations calculations = calculationsBuilder.build();

            final AnnotationTestBase.SaveAnnotationRequestParams params =
                    new AnnotationTestBase.SaveAnnotationRequestParams(
                            null, ownerId,
                            name,
                            dataSetIds,
                            null,
                            null,
                            null,
                            null,
                            calculations);

            final boolean expectReject = true;
            final String expectedRejectMessage = "CalculationDataFrame.dataTimestamps.timestampList must not be empty";
            annotationServiceWrapper.sendAndVerifySaveAnnotation(params, false, expectReject, expectedRejectMessage);
        }

        // createAnnotation() with calculations positive test using DataTimestamps.TimestampList
        {
            final String ownerId = "craigmcc";
            final List<String> dataSetIds = List.of(createDataSetScenarioResult.secondHalfDataSetId());
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
                        .setName("frame-" + i)
                        .setFrame(DataFrame.newBuilder()
                                .setDataTimestamps(dataTimestamps)
                                .addAllDataColumns(dataColumns))
                        .build();
                calculationsBuilder.addCalculationDataFrames(dataFrame);
            }
            final Calculations calculations = calculationsBuilder.build();

            final AnnotationTestBase.SaveAnnotationRequestParams params =
                    new AnnotationTestBase.SaveAnnotationRequestParams(
                            null, ownerId,
                            name,
                            dataSetIds,
                            null,
                            null,
                            null,
                            null,
                            calculations);

            final boolean expectReject = false;
            final String expectedRejectMessage = "";
            annotationServiceWrapper.sendAndVerifySaveAnnotation(params, false, expectReject, expectedRejectMessage);

            // queryAnnotations() positive test to verify calculations in query result annotation
            // uses calculation created above
            {
                final String nameText = "TimestampList";
                final AnnotationTestBase.QueryAnnotationsParams queryParams =
                        new AnnotationTestBase.QueryAnnotationsParams();
                queryParams.setTextCriterion(nameText);

                annotationServiceWrapper.sendAndVerifyQueryAnnotations(
                        queryParams,
                        expectReject,
                        expectedRejectMessage,
                        List.of(params));
            }
        }

    }


    /**
     * Covers the #248 Phase 4 typed-column export paths: HDF5 export of a calculations frame
     * carrying a column of each of the 16 types, written with the self-describing per-column
     * encoding tag (plan D32); CSV export of typed scalar columns through the tabular narrowing
     * (plan D33); and CSV export of a frame with an array column, rejected with HDF5 guidance
     * rather than errored (plan D30) or hung (the pre-D33 unchecked-throw hazard).
     */
    @Test
    public void testTypedCalculationsExport() {

        final long startSeconds = Instant.now().getEpochSecond();

        // ingest some data and create datasets over it (saveAnnotation requires a dataset reference)
        annotationIngestionScenario(startSeconds);
        final CreateDataSetScenarioResult scenarioResult = createDataSetScenario(startSeconds);

        final String ownerId = "craigmcc";

        // positive export test: hdf5 export of a frame carrying a column of each of the 16 types,
        // verified against the stored document including the per-column encoding tags (D32)
        {
            final ArrayDimensions dims = ArrayDimensions.newBuilder().addDims(2).build();
            final ImageDescriptor descriptor = ImageDescriptor.newBuilder()
                    .setWidth(2).setHeight(2).setChannels(1).setEncoding("gray8").build();
            final DataTimestamps dataTimestamps =
                    DataTimestampsUtility.dataTimestampsWithSamplingClock(
                            startSeconds, 0L, 500_000_000L, 2);
            final Calculations calculations = Calculations.newBuilder()
                    .addCalculationDataFrames(Calculations.CalculationsDataFrame.newBuilder()
                            .setName("frame-all-types")
                            .setFrame(DataFrame.newBuilder()
                                    .setDataTimestamps(dataTimestamps)
                                    .addDataColumns(DataColumnUtility.dataColumnWithDoubleValues(
                                            "calc:legacy", List.of(3.14, 2.71)))
                                    .addSerializedDataColumns(SerializedDataColumn.newBuilder()
                                            .setName("calc:serialized")
                                            .setEncoding("avro")
                                            .setPayload(ByteString.copyFrom(new byte[]{0x01, 0x02})))
                                    .addDoubleColumns(DoubleColumn.newBuilder()
                                            .setName("calc:double").addValues(1.0).addValues(1.5))
                                    .addFloatColumns(FloatColumn.newBuilder()
                                            .setName("calc:float").addValues(2.0f).addValues(2.5f))
                                    .addInt64Columns(Int64Column.newBuilder()
                                            .setName("calc:int64").addValues(3L).addValues(4L))
                                    .addInt32Columns(Int32Column.newBuilder()
                                            .setName("calc:int32").addValues(5).addValues(6))
                                    .addBoolColumns(BoolColumn.newBuilder()
                                            .setName("calc:bool").addValues(true).addValues(false))
                                    .addStringColumns(StringColumn.newBuilder()
                                            .setName("calc:string").addValues("a").addValues("b"))
                                    .addEnumColumns(EnumColumn.newBuilder()
                                            .setName("calc:enum").addValues(0).addValues(1))
                                    .addDoubleArrayColumns(DoubleArrayColumn.newBuilder()
                                            .setName("calc:doubleArray").setDimensions(dims)
                                            .addValues(1.0).addValues(2.0).addValues(3.0).addValues(4.0))
                                    .addFloatArrayColumns(FloatArrayColumn.newBuilder()
                                            .setName("calc:floatArray").setDimensions(dims)
                                            .addValues(2.0f).addValues(3.0f).addValues(4.0f).addValues(5.0f))
                                    .addInt32ArrayColumns(Int32ArrayColumn.newBuilder()
                                            .setName("calc:int32Array").setDimensions(dims)
                                            .addValues(7).addValues(8).addValues(9).addValues(10))
                                    .addInt64ArrayColumns(Int64ArrayColumn.newBuilder()
                                            .setName("calc:int64Array").setDimensions(dims)
                                            .addValues(9L).addValues(10L).addValues(11L).addValues(12L))
                                    .addBoolArrayColumns(BoolArrayColumn.newBuilder()
                                            .setName("calc:boolArray").setDimensions(dims)
                                            .addValues(true).addValues(false).addValues(true).addValues(false))
                                    .addStructColumns(StructColumn.newBuilder()
                                            .setName("calc:struct")
                                            .setSchemaId("schema-1")
                                            .addValues(ByteString.copyFrom(new byte[]{1, 2, 3}))
                                            .addValues(ByteString.copyFrom(new byte[]{4, 5, 6})))
                                    .addImageColumns(ImageColumn.newBuilder()
                                            .setName("calc:image")
                                            .setImageDescriptor(descriptor)
                                            .addImages(ByteString.copyFrom(new byte[]{1, 2, 3, 4}))
                                            .addImages(ByteString.copyFrom(new byte[]{5, 6, 7, 8})))))
                    .build();

            final AnnotationTestBase.SaveAnnotationRequestParams params =
                    new AnnotationTestBase.SaveAnnotationRequestParams(
                            null, ownerId, "annotation with all column types",
                            List.of(scenarioResult.firstHalfDataSetId()),
                            null, null, null, null,
                            calculations);
            annotationServiceWrapper.sendAndVerifySaveAnnotation(params, false, false, "");
            final String calculationsId = annotationServiceWrapper.lastSaveAnnotationCalculationsId;
            assertNotNull(calculationsId);

            final CalculationsSpec calculationsSpec = CalculationsSpec.newBuilder()
                    .setCalculationsId(calculationsId)
                    .build();
            final ExportDataRequest request = AnnotationTestBase.buildExportDataRequest(
                    null, null, calculationsSpec, ExportDataRequest.ExportOutputFormat.EXPORT_FORMAT_HDF5);
            final ExportDataResponse.ExportDataResult exportResult =
                    annotationServiceWrapper.sendExportData(request, false, "");
            assertNotNull(exportResult);

            final CalculationsDocument calculationsDocument = mongoClient.findCalculations(calculationsId);
            assertNotNull(calculationsDocument);
            final IHDF5Reader reader = HDF5Factory.openForReading(exportResult.getFilePath());
            AnnotationTestBase.verifyCalculationsDocumentHdf5Content(reader, calculationsDocument, null);
            reader.close();
        }

        // positive export test: csv export of typed scalar columns through the tabular narrowing (D33)
        {
            final DataTimestamps dataTimestamps =
                    DataTimestampsUtility.dataTimestampsWithSamplingClock(
                            startSeconds, 0L, 500_000_000L, 2);
            final Calculations calculations = Calculations.newBuilder()
                    .addCalculationDataFrames(Calculations.CalculationsDataFrame.newBuilder()
                            .setName("frame-csv-scalars")
                            .setFrame(DataFrame.newBuilder()
                                    .setDataTimestamps(dataTimestamps)
                                    .addDataColumns(DataColumnUtility.dataColumnWithDoubleValues(
                                            "calc:legacy", List.of(0.5, 1.5)))
                                    .addDoubleColumns(DoubleColumn.newBuilder()
                                            .setName("calc:double").addValues(1.0).addValues(2.0))
                                    .addStringColumns(StringColumn.newBuilder()
                                            .setName("calc:string").addValues("a").addValues("b"))))
                    .build();

            final AnnotationTestBase.SaveAnnotationRequestParams params =
                    new AnnotationTestBase.SaveAnnotationRequestParams(
                            null, ownerId, "annotation with scalar columns for csv",
                            List.of(scenarioResult.firstHalfDataSetId()),
                            null, null, null, null,
                            calculations);
            annotationServiceWrapper.sendAndVerifySaveAnnotation(params, false, false, "");
            final String calculationsId = annotationServiceWrapper.lastSaveAnnotationCalculationsId;
            assertNotNull(calculationsId);

            final CalculationsSpec calculationsSpec = CalculationsSpec.newBuilder()
                    .setCalculationsId(calculationsId)
                    .build();
            final ExportDataRequest request = AnnotationTestBase.buildExportDataRequest(
                    null, null, calculationsSpec, ExportDataRequest.ExportOutputFormat.EXPORT_FORMAT_CSV);
            final ExportDataResponse.ExportDataResult exportResult =
                    annotationServiceWrapper.sendExportData(request, false, "");
            assertNotNull(exportResult);

            // verify file content: header plus one row per clock sample, typed scalar values
            // rendered through their toDataColumn() narrowing
            try {
                final List<String> lines = Files.readAllLines(Path.of(exportResult.getFilePath()));
                assertEquals(3, lines.size());
                assertEquals("seconds,nanos,calc:legacy,calc:double,calc:string", lines.get(0));
                assertEquals(startSeconds + ",0,0.5,1.0,a", lines.get(1));
                assertEquals(startSeconds + ",500000000,1.5,2.0,b", lines.get(2));
            } catch (IOException e) {
                fail("error reading export file " + exportResult.getFilePath() + ": " + e.getMessage());
            }
        }

        // negative export test: csv export of a frame with an array column is rejected with
        // HDF5 guidance (D30) — wire status REJECT, not ERROR, and never a stream hang
        {
            final DataTimestamps dataTimestamps =
                    DataTimestampsUtility.dataTimestampsWithSamplingClock(
                            startSeconds, 0L, 500_000_000L, 2);
            final Calculations calculations = Calculations.newBuilder()
                    .addCalculationDataFrames(Calculations.CalculationsDataFrame.newBuilder()
                            .setName("frame-csv-array")
                            .setFrame(DataFrame.newBuilder()
                                    .setDataTimestamps(dataTimestamps)
                                    .addDoubleColumns(DoubleColumn.newBuilder()
                                            .setName("calc:double").addValues(1.0).addValues(2.0))
                                    .addDoubleArrayColumns(DoubleArrayColumn.newBuilder()
                                            .setName("calc:doubleArray")
                                            .setDimensions(ArrayDimensions.newBuilder().addDims(2))
                                            .addValues(1.0).addValues(2.0).addValues(3.0).addValues(4.0))))
                    .build();

            final AnnotationTestBase.SaveAnnotationRequestParams params =
                    new AnnotationTestBase.SaveAnnotationRequestParams(
                            null, ownerId, "annotation with array column for csv reject",
                            List.of(scenarioResult.firstHalfDataSetId()),
                            null, null, null, null,
                            calculations);
            annotationServiceWrapper.sendAndVerifySaveAnnotation(params, false, false, "");
            final String calculationsId = annotationServiceWrapper.lastSaveAnnotationCalculationsId;
            assertNotNull(calculationsId);

            final CalculationsSpec calculationsSpec = CalculationsSpec.newBuilder()
                    .setCalculationsId(calculationsId)
                    .build();
            final ExportDataRequest request = AnnotationTestBase.buildExportDataRequest(
                    null, null, calculationsSpec, ExportDataRequest.ExportOutputFormat.EXPORT_FORMAT_CSV);
            annotationServiceWrapper.sendExportData(
                    request,
                    true,
                    "tabular export supports scalar columns only: calculations column "
                            + "'frame-csv-array/calc:doubleArray' has non-scalar column type "
                            + "DoubleArrayColumnDocument; export to HDF5 instead");
        }
    }

}
