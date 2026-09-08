package com.ospreydcs.dp.service.integration.annotation;

import com.google.protobuf.ByteString;
import com.ospreydcs.dp.grpc.v1.annotation.Calculations;
import com.ospreydcs.dp.grpc.v1.common.ArrayDimensions;
import com.ospreydcs.dp.grpc.v1.common.ColumnMetadata;
import com.ospreydcs.dp.grpc.v1.common.ColumnProvenance;
import com.ospreydcs.dp.grpc.v1.common.DataColumn;
import com.ospreydcs.dp.grpc.v1.common.DataFrame;
import com.ospreydcs.dp.grpc.v1.common.DataTimestamps;
import com.ospreydcs.dp.grpc.v1.common.DoubleArrayColumn;
import com.ospreydcs.dp.grpc.v1.common.DoubleColumn;
import com.ospreydcs.dp.grpc.v1.common.EnumColumn;
import com.ospreydcs.dp.grpc.v1.common.SerializedDataColumn;
import com.ospreydcs.dp.grpc.v1.common.StringColumn;
import com.ospreydcs.dp.grpc.v1.common.StructColumn;
import com.ospreydcs.dp.grpc.v1.common.TimeRange;
import com.ospreydcs.dp.grpc.v1.common.Timestamp;
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

    /**
     * Typed calculations columns survive the full save/get round-trip (#248 Phase 4, D25/D28):
     * a representative set — legacy DataColumn, SerializedDataColumn, Double, String, Enum,
     * DoubleArray, Struct — with column metadata including derivedFrom provenance links (D29)
     * is accepted by the D28 validation, stored through the typed document hierarchy, and
     * returned identically by getCalculations(). Storage-level equality is asserted inside
     * sendAndVerifySaveAnnotation via CalculationsDocument.diffCalculations().
     */
    @Test
    public void testGetCalculationsTypedColumnsRoundTrip() {

        final long startSeconds = Instant.now().getEpochSecond();

        // ingest some data and create datasets over it
        annotationIngestionScenario(startSeconds);
        final CreateDataSetScenarioResult scenarioResult = createDataSetScenario(startSeconds);

        // metadata with both derivedFrom arms; links are stored as supplied and may dangle (D29)
        final ColumnMetadata metadata = ColumnMetadata.newBuilder()
                .setProvenance(ColumnProvenance.newBuilder()
                        .setSource("diff-calc")
                        .setProcess("subtract")
                        .addDerivedFrom(ColumnProvenance.ColumnSource.newBuilder()
                                .setPvName("pv:raw:input")
                                .setTimeRange(TimeRange.newBuilder()
                                        .setBeginTime(Timestamp.newBuilder()
                                                .setEpochSeconds(startSeconds - 60).setNanoseconds(0))
                                        .setEndTime(Timestamp.newBuilder()
                                                .setEpochSeconds(startSeconds).setNanoseconds(0))))
                        .addDerivedFrom(ColumnProvenance.ColumnSource.newBuilder()
                                .setCalculationsColumn(ColumnProvenance.CalculationsColumn.newBuilder()
                                        .setCalculationsId("66a1b2c3d4e5f60718293a4b")
                                        .setFrameName("frame-1")
                                        .setColumnName("mean"))))
                .build();

        // one frame over a two-sample clock, columns count-aligned per D28 (the array column
        // carries sampleCount * elementCount flattened values)
        final DataTimestamps dataTimestamps =
                DataTimestampsUtility.dataTimestampsWithSamplingClock(
                        startSeconds, 500_000_000L, 250_000_000L, 2);
        final Calculations calculations = Calculations.newBuilder()
                .addCalculationDataFrames(Calculations.CalculationsDataFrame.newBuilder()
                        .setName("frame-typed")
                        .setFrame(DataFrame.newBuilder()
                                .setDataTimestamps(dataTimestamps)
                                .addDataColumns(DataColumnUtility.dataColumnWithDoubleValues(
                                        "calc:legacy", List.of(0.0, 1.1)))
                                .addSerializedDataColumns(SerializedDataColumn.newBuilder()
                                        .setName("calc:serialized")
                                        .setEncoding("avro")
                                        .setPayload(ByteString.copyFrom(new byte[]{0x01, 0x02})))
                                .addDoubleColumns(DoubleColumn.newBuilder()
                                        .setName("calc:double")
                                        .addValues(1.0).addValues(1.5)
                                        .setMetadata(metadata))
                                .addStringColumns(StringColumn.newBuilder()
                                        .setName("calc:string").addValues("a").addValues("b"))
                                .addEnumColumns(EnumColumn.newBuilder()
                                        .setName("calc:enum").addValues(0).addValues(1))
                                .addDoubleArrayColumns(DoubleArrayColumn.newBuilder()
                                        .setName("calc:doubleArray")
                                        .setDimensions(ArrayDimensions.newBuilder().addDims(2))
                                        .addValues(1.0).addValues(2.0).addValues(3.0).addValues(4.0))
                                .addStructColumns(StructColumn.newBuilder()
                                        .setName("calc:struct")
                                        .setSchemaId("schema-1")
                                        .addValues(ByteString.copyFrom(new byte[]{1, 2, 3}))
                                        .addValues(ByteString.copyFrom(new byte[]{4, 5, 6})))))
                .build();

        final AnnotationTestBase.SaveAnnotationRequestParams params =
                new AnnotationTestBase.SaveAnnotationRequestParams(
                        null, "craigmcc", "annotation with typed calculations",
                        List.of(scenarioResult.firstHalfDataSetId()),
                        null, null, null, null,
                        calculations);
        annotationServiceWrapper.sendAndVerifySaveAnnotation(params, false, false, "");
        final String calculationsId = annotationServiceWrapper.lastSaveAnnotationCalculationsId;
        assertNotNull(calculationsId);

        // getCalculations() must return the frames exactly as saved, metadata included
        final Calculations retrieved =
                annotationServiceWrapper.sendAndVerifyGetCalculations(calculationsId, false, null);
        assertEquals(calculationsId, retrieved.getId());
        assertEquals(
                calculations.getCalculationDataFramesList(),
                retrieved.getCalculationDataFramesList());
    }
}
