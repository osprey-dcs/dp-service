package com.ospreydcs.dp.service.common.bson.column;

import com.google.protobuf.ByteString;
import com.ospreydcs.dp.grpc.v1.common.*;
import com.ospreydcs.dp.service.common.exception.DpException;
import org.junit.Test;

import java.util.List;

import static org.junit.Assert.*;

/**
 * Unit tests for the shared DataFrame-to-column-document dispatch. The dispatch must cover all 16
 * column types and preserve per-type request order — both storage paths (ingestion buckets,
 * annotation calculations) rely on it as the single source for this mapping.
 */
public class ColumnDocumentUtilityTest {

    @Test
    public void testFromDataFrame_allSixteenColumnTypes() throws DpException {
        final ArrayDimensions dims = ArrayDimensions.newBuilder().addDims(1).build();
        final ImageDescriptor descriptor = ImageDescriptor.newBuilder()
                .setWidth(2).setHeight(2).setChannels(1).setEncoding("gray8").build();

        final DataFrame frame = DataFrame.newBuilder()
                .addDataColumns(DataColumn.newBuilder()
                        .setName("pv:legacy")
                        .addDataValues(DataValue.newBuilder().setDoubleValue(3.14).build()))
                .addSerializedDataColumns(SerializedDataColumn.newBuilder()
                        .setName("pv:serialized")
                        .setEncoding("avro")
                        .setPayload(ByteString.copyFrom(new byte[]{0x01})))
                .addDoubleColumns(DoubleColumn.newBuilder().setName("pv:double").addValues(1.0))
                .addFloatColumns(FloatColumn.newBuilder().setName("pv:float").addValues(2.0f))
                .addInt64Columns(Int64Column.newBuilder().setName("pv:int64").addValues(3L))
                .addInt32Columns(Int32Column.newBuilder().setName("pv:int32").addValues(4))
                .addBoolColumns(BoolColumn.newBuilder().setName("pv:bool").addValues(true))
                .addStringColumns(StringColumn.newBuilder().setName("pv:string").addValues("a"))
                .addEnumColumns(EnumColumn.newBuilder().setName("pv:enum").addValues(0))
                .addDoubleArrayColumns(DoubleArrayColumn.newBuilder()
                        .setName("pv:doubleArray").setDimensions(dims).addValues(1.0))
                .addFloatArrayColumns(FloatArrayColumn.newBuilder()
                        .setName("pv:floatArray").setDimensions(dims).addValues(2.0f))
                .addInt32ArrayColumns(Int32ArrayColumn.newBuilder()
                        .setName("pv:int32Array").setDimensions(dims).addValues(7))
                .addInt64ArrayColumns(Int64ArrayColumn.newBuilder()
                        .setName("pv:int64Array").setDimensions(dims).addValues(8L))
                .addBoolArrayColumns(BoolArrayColumn.newBuilder()
                        .setName("pv:boolArray").setDimensions(dims).addValues(true))
                .addStructColumns(StructColumn.newBuilder()
                        .setName("pv:struct")
                        .setSchemaId("schema-1")
                        .addValues(ByteString.copyFrom(new byte[]{1, 2, 3})))
                .addImageColumns(ImageColumn.newBuilder()
                        .setName("pv:image")
                        .setImageDescriptor(descriptor)
                        .addImages(ByteString.copyFrom(new byte[]{1, 2, 3, 4})))
                .build();

        final List<ColumnDocumentBase> documents = ColumnDocumentUtility.fromDataFrame(frame);

        assertEquals(16, documents.size());

        final Object[][] expected = {
                {DataColumnDocument.class, "pv:legacy"},
                {SerializedDataColumnDocument.class, "pv:serialized"},
                {DoubleColumnDocument.class, "pv:double"},
                {FloatColumnDocument.class, "pv:float"},
                {Int64ColumnDocument.class, "pv:int64"},
                {Int32ColumnDocument.class, "pv:int32"},
                {BoolColumnDocument.class, "pv:bool"},
                {StringColumnDocument.class, "pv:string"},
                {EnumColumnDocument.class, "pv:enum"},
                {DoubleArrayColumnDocument.class, "pv:doubleArray"},
                {FloatArrayColumnDocument.class, "pv:floatArray"},
                {Int32ArrayColumnDocument.class, "pv:int32Array"},
                {Int64ArrayColumnDocument.class, "pv:int64Array"},
                {BoolArrayColumnDocument.class, "pv:boolArray"},
                {StructColumnDocument.class, "pv:struct"},
                {ImageColumnDocument.class, "pv:image"},
        };
        for (int i = 0; i < expected.length; i++) {
            assertEquals("document class at index " + i, expected[i][0], documents.get(i).getClass());
            assertEquals("document name at index " + i, expected[i][1], documents.get(i).getName());
        }
    }

    @Test
    public void testFromDataFrame_emptyFrame() throws DpException {
        final List<ColumnDocumentBase> documents =
                ColumnDocumentUtility.fromDataFrame(DataFrame.newBuilder().build());
        assertTrue(documents.isEmpty());
    }

    @Test
    public void testFromDataFrame_preservesOrderWithinType() throws DpException {
        final DataFrame frame = DataFrame.newBuilder()
                .addDoubleColumns(DoubleColumn.newBuilder().setName("pv:first").addValues(1.0))
                .addDoubleColumns(DoubleColumn.newBuilder().setName("pv:second").addValues(2.0))
                .build();

        final List<ColumnDocumentBase> documents = ColumnDocumentUtility.fromDataFrame(frame);

        assertEquals(2, documents.size());
        assertEquals("pv:first", documents.get(0).getName());
        assertEquals("pv:second", documents.get(1).getName());
    }
}
