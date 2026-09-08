package com.ospreydcs.dp.service.annotation.handler;

import com.google.protobuf.ByteString;
import com.ospreydcs.dp.grpc.v1.annotation.Calculations;
import com.ospreydcs.dp.grpc.v1.annotation.SaveAnnotationRequest;
import com.ospreydcs.dp.grpc.v1.common.*;
import com.ospreydcs.dp.service.common.model.ResultStatus;
import com.ospreydcs.dp.service.common.protobuf.DataColumnUtility;
import com.ospreydcs.dp.service.common.protobuf.DataTimestampsUtility;
import org.bson.types.ObjectId;
import org.junit.Test;

import java.util.List;
import java.util.function.Function;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Unit coverage for the D28 save-side calculations validation (#248 Phase 4): per-column
 * blank-name / empty-values / count-match rejects across all 16 column types, per-frame
 * at-least-one-column and unique-column-names checks, unique frame names per Calculations
 * object, and the shared column-metadata limits under the calculations field path.
 *
 * The count-match check is what closes plan finding 5: a stored column shorter than its frame's
 * timestamp axis fails during tabular export assembly with an unchecked
 * IndexOutOfBoundsException that hangs the caller's stream.
 */
public class AnnotationValidationUtilityTest {

    private static final long START_SECONDS = 1_700_000_000L;

    // -----------------------------------------------------------------------
    // helpers
    // -----------------------------------------------------------------------

    /** Two-sample clock shared by all fixtures; the frame's row axis is 2. */
    private static DataTimestamps twoSampleClock() {
        return DataTimestampsUtility.dataTimestampsWithSamplingClock(
                START_SECONDS, 500_000_000L, 250_000_000L, 2);
    }

    private static SaveAnnotationRequest requestWithCalculations(Calculations calculations) {
        return SaveAnnotationRequest.newBuilder()
                .setOwnerId("craigmcc")
                .setName("test annotation")
                .addDataSetIds(new ObjectId().toHexString())
                .setCalculations(calculations)
                .build();
    }

    private static Calculations singleFrameCalculations(Function<DataFrame.Builder, DataFrame.Builder> framer) {
        return Calculations.newBuilder()
                .addCalculationDataFrames(Calculations.CalculationsDataFrame.newBuilder()
                        .setName("frame-0")
                        .setFrame(framer.apply(
                                DataFrame.newBuilder().setDataTimestamps(twoSampleClock()))))
                .build();
    }

    private static ResultStatus validate(Calculations calculations) {
        return AnnotationValidationUtility.validateSaveAnnotationRequest(
                requestWithCalculations(calculations));
    }

    private static void assertRejectContains(ResultStatus status, String expected) {
        assertTrue("expected a rejection", status.isError);
        assertTrue("expected message containing [" + expected + "], got: " + status.msg,
                status.msg.contains(expected));
    }

    // -----------------------------------------------------------------------
    // positive: a count-aligned frame with a column of every one of the 16 types
    // -----------------------------------------------------------------------

    @Test
    public void testAllSixteenColumnTypesCountAlignedIsAccepted() {
        final ArrayDimensions dims = ArrayDimensions.newBuilder().addDims(2).build();
        final Calculations calculations = singleFrameCalculations(f -> f
                .addDataColumns(DataColumnUtility.dataColumnWithDoubleValues("c:legacy", List.of(0.0, 1.1)))
                .addSerializedDataColumns(SerializedDataColumn.newBuilder()
                        .setName("c:serialized").setEncoding("avro")
                        .setPayload(ByteString.copyFrom(new byte[]{1})))
                .addDoubleColumns(DoubleColumn.newBuilder().setName("c:double").addValues(1.0).addValues(2.0))
                .addFloatColumns(FloatColumn.newBuilder().setName("c:float").addValues(1f).addValues(2f))
                .addInt64Columns(Int64Column.newBuilder().setName("c:int64").addValues(1L).addValues(2L))
                .addInt32Columns(Int32Column.newBuilder().setName("c:int32").addValues(1).addValues(2))
                .addBoolColumns(BoolColumn.newBuilder().setName("c:bool").addValues(true).addValues(false))
                .addStringColumns(StringColumn.newBuilder().setName("c:string").addValues("a").addValues("b"))
                .addEnumColumns(EnumColumn.newBuilder().setName("c:enum").addValues(0).addValues(1))
                // arrays carry sampleCount * elementCount flattened values: 2 * 2 = 4
                .addDoubleArrayColumns(DoubleArrayColumn.newBuilder().setName("c:doubleArray")
                        .setDimensions(dims).addValues(1.0).addValues(2.0).addValues(3.0).addValues(4.0))
                .addFloatArrayColumns(FloatArrayColumn.newBuilder().setName("c:floatArray")
                        .setDimensions(dims).addValues(1f).addValues(2f).addValues(3f).addValues(4f))
                .addInt32ArrayColumns(Int32ArrayColumn.newBuilder().setName("c:int32Array")
                        .setDimensions(dims).addValues(1).addValues(2).addValues(3).addValues(4))
                .addInt64ArrayColumns(Int64ArrayColumn.newBuilder().setName("c:int64Array")
                        .setDimensions(dims).addValues(1L).addValues(2L).addValues(3L).addValues(4L))
                .addBoolArrayColumns(BoolArrayColumn.newBuilder().setName("c:boolArray")
                        .setDimensions(dims).addValues(true).addValues(false).addValues(true).addValues(false))
                .addStructColumns(StructColumn.newBuilder().setName("c:struct").setSchemaId("s1")
                        .addValues(ByteString.copyFrom(new byte[]{1}))
                        .addValues(ByteString.copyFrom(new byte[]{2})))
                .addImageColumns(ImageColumn.newBuilder().setName("c:image")
                        .setImageDescriptor(ImageDescriptor.newBuilder()
                                .setWidth(2).setHeight(2).setChannels(1).setEncoding("gray8"))
                        .addImages(ByteString.copyFrom(new byte[]{1}))
                        .addImages(ByteString.copyFrom(new byte[]{2}))));
        final ResultStatus status = validate(calculations);
        assertFalse("expected acceptance, got: " + status.msg, status.isError);
    }

    @Test
    public void testTypedColumnsOnlyFrameIsAccepted() {
        // before D28 a frame with no legacy dataColumns was rejected even when it carried typed
        // columns (#248 plan finding 1) — the emptiness check now spans all column types
        final Calculations calculations = singleFrameCalculations(f -> f
                .addDoubleColumns(DoubleColumn.newBuilder().setName("c:double").addValues(1.0).addValues(2.0)));
        final ResultStatus status = validate(calculations);
        assertFalse("expected acceptance, got: " + status.msg, status.isError);
    }

    @Test
    public void testSparseLegacyValuesStillExpressible() {
        // a DataValue entry with no value arm set still occupies its position; the count check
        // is on entry count, not set arms
        final Calculations calculations = singleFrameCalculations(f -> f
                .addDataColumns(DataColumn.newBuilder().setName("c:sparse")
                        .addDataValues(DataValue.newBuilder().setDoubleValue(1.0))
                        .addDataValues(DataValue.newBuilder())));
        final ResultStatus status = validate(calculations);
        assertFalse("expected acceptance, got: " + status.msg, status.isError);
    }

    @Test
    public void testTimestampListDrivesSampleCount() {
        final DataTimestamps timestampList = DataTimestampsUtility.dataTimestampsWithTimestampList(List.of(
                Timestamp.newBuilder().setEpochSeconds(START_SECONDS).setNanoseconds(0).build(),
                Timestamp.newBuilder().setEpochSeconds(START_SECONDS).setNanoseconds(500_000_000L).build(),
                Timestamp.newBuilder().setEpochSeconds(START_SECONDS + 1).setNanoseconds(0).build()));
        final Calculations mismatched = Calculations.newBuilder()
                .addCalculationDataFrames(Calculations.CalculationsDataFrame.newBuilder()
                        .setName("frame-0")
                        .setFrame(DataFrame.newBuilder()
                                .setDataTimestamps(timestampList)
                                .addDoubleColumns(DoubleColumn.newBuilder()
                                        .setName("c:double").addValues(1.0).addValues(2.0))))
                .build();
        assertRejectContains(validate(mismatched),
                "CalculationDataFrame.doubleColumns values count mismatch: expected 3, got: 2 for column: c:double");
    }

    // -----------------------------------------------------------------------
    // frame-level rejects
    // -----------------------------------------------------------------------

    @Test
    public void testEmptyFramesListRejected() {
        assertRejectContains(validate(Calculations.newBuilder().build()),
                "SaveAnnotationRequest.calculations.calculationDataFrames must not be empty");
    }

    @Test
    public void testDuplicateFrameNamesRejected() {
        final Calculations.CalculationsDataFrame frame = Calculations.CalculationsDataFrame.newBuilder()
                .setName("dup-frame")
                .setFrame(DataFrame.newBuilder()
                        .setDataTimestamps(twoSampleClock())
                        .addDoubleColumns(DoubleColumn.newBuilder()
                                .setName("c:double").addValues(1.0).addValues(2.0)))
                .build();
        final Calculations calculations = Calculations.newBuilder()
                .addCalculationDataFrames(frame)
                .addCalculationDataFrames(frame)
                .build();
        assertRejectContains(validate(calculations),
                "SaveAnnotationRequest.calculations.calculationDataFrames contains duplicate frame name: dup-frame");
    }

    @Test
    public void testFrameWithNoColumnsOfAnyTypeRejected() {
        assertRejectContains(validate(singleFrameCalculations(f -> f)),
                "CalculationDataFrame must include at least one column of any type: frame-0");
    }

    @Test
    public void testDuplicateColumnNamesAcrossTypesRejected() {
        // duplicate across two different column-type lists — the uniqueness check spans all types
        final Calculations calculations = singleFrameCalculations(f -> f
                .addDoubleColumns(DoubleColumn.newBuilder().setName("c:dup").addValues(1.0).addValues(2.0))
                .addStringColumns(StringColumn.newBuilder().setName("c:dup").addValues("a").addValues("b")));
        assertRejectContains(validate(calculations),
                "CalculationDataFrame contains duplicate column name: c:dup in frame: frame-0");
    }

    // -----------------------------------------------------------------------
    // legacy DataColumn checks
    // -----------------------------------------------------------------------

    @Test
    public void testLegacyColumnCountMismatchRejected() {
        final Calculations calculations = singleFrameCalculations(f -> f
                .addDataColumns(DataColumnUtility.dataColumnWithDoubleValues("c:legacy", List.of(0.0, 1.1, 2.2))));
        assertRejectContains(validate(calculations),
                "CalculationDataFrame.dataColumns values count mismatch: expected 2, got: 3 for column: c:legacy");
    }

    @Test
    public void testLegacyColumnPreexistingMessagesPreserved() {
        assertRejectContains(
                validate(singleFrameCalculations(f -> f
                        .addDataColumns(DataColumn.newBuilder()
                                .addDataValues(DataValue.newBuilder().setDoubleValue(1.0))
                                .addDataValues(DataValue.newBuilder().setDoubleValue(2.0))))),
                "CalculationDataFrame.dataColumns name must be specified for each DataColumn");
        assertRejectContains(
                validate(singleFrameCalculations(f -> f
                        .addDataColumns(DataColumn.newBuilder().setName("c:empty")))),
                "CalculationDataFrame.dataColumns contains a DataColumn with no values: c:empty");
    }

    // -----------------------------------------------------------------------
    // typed scalar checks
    // -----------------------------------------------------------------------

    @Test
    public void testScalarBlankNameRejected() {
        final Calculations calculations = singleFrameCalculations(f -> f
                .addDoubleColumns(DoubleColumn.newBuilder().addValues(1.0).addValues(2.0)));
        assertRejectContains(validate(calculations),
                "CalculationDataFrame.doubleColumns name must be specified for each column");
    }

    @Test
    public void testScalarEmptyValuesRejected() {
        final Calculations calculations = singleFrameCalculations(f -> f
                .addStringColumns(StringColumn.newBuilder().setName("c:string")));
        assertRejectContains(validate(calculations),
                "CalculationDataFrame.stringColumns contains a column with no values: c:string");
    }

    @Test
    public void testScalarCountMismatchRejected() {
        final Calculations calculations = singleFrameCalculations(f -> f
                .addEnumColumns(EnumColumn.newBuilder().setName("c:enum").addValues(0)));
        assertRejectContains(validate(calculations),
                "CalculationDataFrame.enumColumns values count mismatch: expected 2, got: 1 for column: c:enum");
    }

    // -----------------------------------------------------------------------
    // array checks — expected count is sampleCount * elementCount over the dims product
    // -----------------------------------------------------------------------

    @Test
    public void testArrayMissingDimensionsRejected() {
        final Calculations calculations = singleFrameCalculations(f -> f
                .addDoubleArrayColumns(DoubleArrayColumn.newBuilder().setName("c:array")
                        .addValues(1.0).addValues(2.0)));
        assertRejectContains(validate(calculations),
                "CalculationDataFrame.doubleArrayColumns dimensions.dims.size must be in {1, 2, 3}, got: 0 for column: c:array");
    }

    @Test
    public void testArrayNonPositiveDimensionRejected() {
        final Calculations calculations = singleFrameCalculations(f -> f
                .addInt32ArrayColumns(Int32ArrayColumn.newBuilder().setName("c:array")
                        .setDimensions(ArrayDimensions.newBuilder().addDims(2).addDims(0))
                        .addValues(1).addValues(2)));
        assertRejectContains(validate(calculations),
                "CalculationDataFrame.int32ArrayColumns dimensions.dims[1] must be > 0, got: 0 for column: c:array");
    }

    @Test
    public void testArrayCountMismatchRejected() {
        // dims [2] over a two-sample clock expects 4 flattened values
        final Calculations calculations = singleFrameCalculations(f -> f
                .addBoolArrayColumns(BoolArrayColumn.newBuilder().setName("c:array")
                        .setDimensions(ArrayDimensions.newBuilder().addDims(2))
                        .addValues(true).addValues(false).addValues(true)));
        assertRejectContains(validate(calculations),
                "CalculationDataFrame.boolArrayColumns values count mismatch: expected 4 "
                        + "(sampleCount=2 * elementCount=2), got: 3 for column: c:array");
    }

    // -----------------------------------------------------------------------
    // image / struct / serialized checks
    // -----------------------------------------------------------------------

    @Test
    public void testImageCountMismatchRejected() {
        final Calculations calculations = singleFrameCalculations(f -> f
                .addImageColumns(ImageColumn.newBuilder().setName("c:image")
                        .setImageDescriptor(ImageDescriptor.newBuilder()
                                .setWidth(2).setHeight(2).setChannels(1).setEncoding("gray8"))
                        .addImages(ByteString.copyFrom(new byte[]{1}))));
        assertRejectContains(validate(calculations),
                "CalculationDataFrame.imageColumns values count mismatch: expected 2, got: 1 for column: c:image");
    }

    @Test
    public void testStructCountMismatchRejected() {
        final Calculations calculations = singleFrameCalculations(f -> f
                .addStructColumns(StructColumn.newBuilder().setName("c:struct").setSchemaId("s1")
                        .addValues(ByteString.copyFrom(new byte[]{1}))));
        assertRejectContains(validate(calculations),
                "CalculationDataFrame.structColumns values count mismatch: expected 2, got: 1 for column: c:struct");
    }

    @Test
    public void testSerializedColumnHasNoCountCheck() {
        // serialized columns carry no countable values; a lone serialized column is a valid frame
        final Calculations calculations = singleFrameCalculations(f -> f
                .addSerializedDataColumns(SerializedDataColumn.newBuilder()
                        .setName("c:serialized").setEncoding("avro")
                        .setPayload(ByteString.copyFrom(new byte[]{1}))));
        final ResultStatus status = validate(calculations);
        assertFalse("expected acceptance, got: " + status.msg, status.isError);
    }

    @Test
    public void testSerializedColumnBlankNameRejected() {
        final Calculations calculations = singleFrameCalculations(f -> f
                .addSerializedDataColumns(SerializedDataColumn.newBuilder()
                        .setEncoding("avro").setPayload(ByteString.copyFrom(new byte[]{1}))));
        assertRejectContains(validate(calculations),
                "CalculationDataFrame.serializedDataColumns name must be specified for each column");
    }

    // -----------------------------------------------------------------------
    // shared column-metadata limits under the calculations field path
    // -----------------------------------------------------------------------

    @Test
    public void testColumnMetadataLimitsAppliedWithCalculationsFieldPath() {
        final ColumnMetadata oversized = ColumnMetadata.newBuilder()
                .setProvenance(ColumnProvenance.newBuilder().setSource("x".repeat(257)))
                .build();
        final Calculations calculations = singleFrameCalculations(f -> f
                .addDoubleColumns(DoubleColumn.newBuilder().setName("c:double")
                        .addValues(1.0).addValues(2.0).setMetadata(oversized)));
        assertRejectContains(validate(calculations),
                "SaveAnnotationRequest.calculations.calculationDataFrames[0].doubleColumns[0]"
                        + ".metadata.provenance.source length exceeds maximum");
    }
}
