package com.ospreydcs.dp.service.common.bson.column;

import com.ospreydcs.dp.grpc.v1.common.*;
import com.ospreydcs.dp.service.common.bson.ColumnMetadataDocument;
import com.ospreydcs.dp.service.common.exception.DpException;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;

import static org.junit.Assert.*;

/**
 * Unit tests verifying that all column document factory methods correctly extract
 * ColumnMetadata from incoming proto columns and store it in the columnMetadata field.
 */
public class ColumnDocumentMetadataTest {

    private static ColumnMetadata buildFullMetadata() {
        return ColumnMetadata.newBuilder()
                .setProvenance(ColumnProvenance.newBuilder()
                        .setSource("archiver")
                        .setProcess("epics-bridge")
                        .build())
                .addTags("fast")
                .addAttributes(Attribute.newBuilder().setName("units").setValue("mm").build())
                .build();
    }

    // -----------------------------------------------------------------------
    // Scalar columns
    // -----------------------------------------------------------------------

    @Test
    public void testDoubleColumn_withMetadata() {
        ColumnMetadata meta = buildFullMetadata();
        DoubleColumn col = DoubleColumn.newBuilder()
                .setName("pv:double")
                .addAllValues(Arrays.asList(1.0, 2.0))
                .setMetadata(meta)
                .build();

        DoubleColumnDocument doc = DoubleColumnDocument.fromDoubleColumn(col);

        assertNotNull(doc.getColumnMetadata());
        assertEquals("archiver", doc.getColumnMetadata().getProvenance().getSource());
        assertEquals("epics-bridge", doc.getColumnMetadata().getProvenance().getProcess());
        assertEquals(Collections.singletonList("fast"), doc.getColumnMetadata().getTags());
        assertEquals("mm", doc.getColumnMetadata().getAttributes().get("units"));
    }

    @Test
    public void testDoubleColumn_withoutMetadata() {
        DoubleColumn col = DoubleColumn.newBuilder()
                .setName("pv:double")
                .addAllValues(Arrays.asList(1.0, 2.0))
                .build();

        DoubleColumnDocument doc = DoubleColumnDocument.fromDoubleColumn(col);

        assertNull(doc.getColumnMetadata());
    }

    @Test
    public void testFloatColumn_withMetadata() {
        ColumnMetadata meta = buildFullMetadata();
        FloatColumn col = FloatColumn.newBuilder()
                .setName("pv:float")
                .addValues(1.0f)
                .setMetadata(meta)
                .build();

        FloatColumnDocument doc = FloatColumnDocument.fromFloatColumn(col);

        assertNotNull(doc.getColumnMetadata());
        assertEquals("archiver", doc.getColumnMetadata().getProvenance().getSource());
    }

    @Test
    public void testInt64Column_withMetadata() {
        ColumnMetadata meta = buildFullMetadata();
        Int64Column col = Int64Column.newBuilder()
                .setName("pv:int64")
                .addValues(100L)
                .setMetadata(meta)
                .build();

        Int64ColumnDocument doc = Int64ColumnDocument.fromInt64Column(col);

        assertNotNull(doc.getColumnMetadata());
        assertEquals("archiver", doc.getColumnMetadata().getProvenance().getSource());
    }

    @Test
    public void testInt32Column_withMetadata() {
        ColumnMetadata meta = buildFullMetadata();
        Int32Column col = Int32Column.newBuilder()
                .setName("pv:int32")
                .addValues(42)
                .setMetadata(meta)
                .build();

        Int32ColumnDocument doc = Int32ColumnDocument.fromInt32Column(col);

        assertNotNull(doc.getColumnMetadata());
        assertEquals("archiver", doc.getColumnMetadata().getProvenance().getSource());
    }

    @Test
    public void testBoolColumn_withMetadata() {
        ColumnMetadata meta = buildFullMetadata();
        BoolColumn col = BoolColumn.newBuilder()
                .setName("pv:bool")
                .addValues(true)
                .setMetadata(meta)
                .build();

        BoolColumnDocument doc = BoolColumnDocument.fromBoolColumn(col);

        assertNotNull(doc.getColumnMetadata());
        assertEquals("archiver", doc.getColumnMetadata().getProvenance().getSource());
    }

    @Test
    public void testStringColumn_withMetadata() {
        ColumnMetadata meta = buildFullMetadata();
        StringColumn col = StringColumn.newBuilder()
                .setName("pv:string")
                .addValues("hello")
                .setMetadata(meta)
                .build();

        StringColumnDocument doc = StringColumnDocument.fromStringColumn(col);

        assertNotNull(doc.getColumnMetadata());
        assertEquals("archiver", doc.getColumnMetadata().getProvenance().getSource());
    }

    @Test
    public void testEnumColumn_withMetadata() {
        ColumnMetadata meta = buildFullMetadata();
        EnumColumn col = EnumColumn.newBuilder()
                .setName("pv:enum")
                .addValues(0)
                .setMetadata(meta)
                .build();

        EnumColumnDocument doc = EnumColumnDocument.fromEnumColumn(col);

        assertNotNull(doc.getColumnMetadata());
        assertEquals("archiver", doc.getColumnMetadata().getProvenance().getSource());
    }

    // -----------------------------------------------------------------------
    // Array columns
    // -----------------------------------------------------------------------

    @Test
    public void testDoubleArrayColumn_withMetadata() throws DpException {
        ColumnMetadata meta = buildFullMetadata();
        ArrayDimensions dims = ArrayDimensions.newBuilder().addDims(2).build();
        DoubleArrayColumn col = DoubleArrayColumn.newBuilder()
                .setName("pv:doubleArray")
                .setDimensions(dims)
                .addValues(1.0)
                .addValues(2.0)
                .setMetadata(meta)
                .build();

        DoubleArrayColumnDocument doc = DoubleArrayColumnDocument.fromDoubleArrayColumn(col);

        assertNotNull(doc.getColumnMetadata());
        assertEquals("archiver", doc.getColumnMetadata().getProvenance().getSource());
    }

    @Test
    public void testDoubleArrayColumn_withoutMetadata() throws DpException {
        ArrayDimensions dims = ArrayDimensions.newBuilder().addDims(2).build();
        DoubleArrayColumn col = DoubleArrayColumn.newBuilder()
                .setName("pv:doubleArray")
                .setDimensions(dims)
                .addValues(1.0)
                .addValues(2.0)
                .build();

        DoubleArrayColumnDocument doc = DoubleArrayColumnDocument.fromDoubleArrayColumn(col);

        assertNull(doc.getColumnMetadata());
    }

    @Test
    public void testBoolArrayColumn_withMetadata() throws DpException {
        ColumnMetadata meta = buildFullMetadata();
        ArrayDimensions dims = ArrayDimensions.newBuilder().addDims(1).build();
        BoolArrayColumn col = BoolArrayColumn.newBuilder()
                .setName("pv:boolArray")
                .setDimensions(dims)
                .addValues(true)
                .setMetadata(meta)
                .build();

        BoolArrayColumnDocument doc = BoolArrayColumnDocument.fromBoolArrayColumn(col);

        assertNotNull(doc.getColumnMetadata());
        assertEquals("archiver", doc.getColumnMetadata().getProvenance().getSource());
    }

    @Test
    public void testInt32ArrayColumn_withMetadata() throws DpException {
        ColumnMetadata meta = buildFullMetadata();
        ArrayDimensions dims = ArrayDimensions.newBuilder().addDims(1).build();
        Int32ArrayColumn col = Int32ArrayColumn.newBuilder()
                .setName("pv:int32Array")
                .setDimensions(dims)
                .addValues(7)
                .setMetadata(meta)
                .build();

        Int32ArrayColumnDocument doc = Int32ArrayColumnDocument.fromInt32ArrayColumn(col);

        assertNotNull(doc.getColumnMetadata());
        assertEquals("archiver", doc.getColumnMetadata().getProvenance().getSource());
    }

    @Test
    public void testInt64ArrayColumn_withMetadata() throws DpException {
        ColumnMetadata meta = buildFullMetadata();
        ArrayDimensions dims = ArrayDimensions.newBuilder().addDims(1).build();
        Int64ArrayColumn col = Int64ArrayColumn.newBuilder()
                .setName("pv:int64Array")
                .setDimensions(dims)
                .addValues(999L)
                .setMetadata(meta)
                .build();

        Int64ArrayColumnDocument doc = Int64ArrayColumnDocument.fromInt64ArrayColumn(col);

        assertNotNull(doc.getColumnMetadata());
        assertEquals("archiver", doc.getColumnMetadata().getProvenance().getSource());
    }

    @Test
    public void testFloatArrayColumn_withMetadata() throws DpException {
        ColumnMetadata meta = buildFullMetadata();
        ArrayDimensions dims = ArrayDimensions.newBuilder().addDims(1).build();
        FloatArrayColumn col = FloatArrayColumn.newBuilder()
                .setName("pv:floatArray")
                .setDimensions(dims)
                .addValues(3.14f)
                .setMetadata(meta)
                .build();

        FloatArrayColumnDocument doc = FloatArrayColumnDocument.fromFloatArrayColumn(col);

        assertNotNull(doc.getColumnMetadata());
        assertEquals("archiver", doc.getColumnMetadata().getProvenance().getSource());
    }

    // -----------------------------------------------------------------------
    // Binary columns
    // -----------------------------------------------------------------------

    @Test
    public void testStructColumn_withMetadata() throws DpException {
        ColumnMetadata meta = buildFullMetadata();
        StructColumn col = StructColumn.newBuilder()
                .setName("pv:struct")
                .setSchemaId("schema-1")
                .addValues(com.google.protobuf.ByteString.copyFrom(new byte[]{1, 2, 3}))
                .setMetadata(meta)
                .build();

        StructColumnDocument doc = StructColumnDocument.fromStructColumn(col);

        assertNotNull(doc.getColumnMetadata());
        assertEquals("archiver", doc.getColumnMetadata().getProvenance().getSource());
    }

    @Test
    public void testImageColumn_withMetadata() throws DpException {
        ColumnMetadata meta = buildFullMetadata();
        ImageDescriptor descriptor = ImageDescriptor.newBuilder()
                .setWidth(2).setHeight(2).setChannels(1).setEncoding("gray8").build();
        ImageColumn col = ImageColumn.newBuilder()
                .setName("pv:image")
                .setImageDescriptor(descriptor)
                .addImages(com.google.protobuf.ByteString.copyFrom(new byte[]{1, 2, 3, 4}))
                .setMetadata(meta)
                .build();

        ImageColumnDocument doc = ImageColumnDocument.fromImageColumn(col);

        assertNotNull(doc.getColumnMetadata());
        assertEquals("archiver", doc.getColumnMetadata().getProvenance().getSource());
    }

    @Test
    public void testSerializedDataColumn_withMetadata() throws DpException {
        ColumnMetadata meta = buildFullMetadata();
        SerializedDataColumn col = SerializedDataColumn.newBuilder()
                .setName("pv:serialized")
                .setEncoding("avro")
                .setPayload(com.google.protobuf.ByteString.copyFrom(new byte[]{0x01}))
                .setMetadata(meta)
                .build();

        SerializedDataColumnDocument doc = SerializedDataColumnDocument.fromSerializedDataColumn(col);

        assertNotNull(doc.getColumnMetadata());
        assertEquals("archiver", doc.getColumnMetadata().getProvenance().getSource());
    }
}
