package com.ospreydcs.dp.service.common.bson.calculations;

import com.google.protobuf.ByteString;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.Filters;
import com.ospreydcs.dp.grpc.v1.annotation.Calculations;
import com.ospreydcs.dp.grpc.v1.common.*;
import com.ospreydcs.dp.service.common.exception.DpException;
import com.ospreydcs.dp.service.common.mongo.MongoTestClient;
import com.ospreydcs.dp.service.common.protobuf.DataTimestampsUtility;
import org.bson.types.ObjectId;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/**
 * Covers the #248 Phase 4 retype of CalculationsDataFrameDocument.dataColumns to the polymorphic
 * List&lt;ColumnDocumentBase&gt; (plan D25): a calculations frame carrying columns of every type
 * must round-trip losslessly through the document conversion, and — the part only a real server
 * can pin — through the BSON POJO codec, whose {@code _t} discriminator restores each concrete
 * class inside the nested frame array.
 */
public class CalculationsDocumentColumnTypesTest {

    private CalculationsTestClient testClient;

    private static class CalculationsTestClient extends MongoTestClient {
        MongoCollection<CalculationsDocument> calculationsPojo() {
            return mongoCollectionCalculations;
        }
    }

    @Before
    public void setUp() {
        testClient = new CalculationsTestClient();
        testClient.init();
    }

    @After
    public void tearDown() {
        testClient.fini();
    }

    /** One frame carrying a column of each of the 16 types over a two-sample clock. */
    private static Calculations allColumnTypesCalculations() {
        final ArrayDimensions dims = ArrayDimensions.newBuilder().addDims(2).build();
        final ImageDescriptor descriptor = ImageDescriptor.newBuilder()
                .setWidth(2).setHeight(2).setChannels(1).setEncoding("gray8").build();
        final DataTimestamps dataTimestamps =
                DataTimestampsUtility.dataTimestampsWithSamplingClock(
                        1_700_000_000L, 500_000_000L, 250_000_000L, 2);

        final DataFrame frame = DataFrame.newBuilder()
                .setDataTimestamps(dataTimestamps)
                .addDataColumns(DataColumn.newBuilder()
                        .setName("calc:legacy")
                        .addDataValues(DataValue.newBuilder().setDoubleValue(3.14))
                        .addDataValues(DataValue.newBuilder().setDoubleValue(2.71)))
                .addSerializedDataColumns(SerializedDataColumn.newBuilder()
                        .setName("calc:serialized")
                        .setEncoding("avro")
                        .setPayload(ByteString.copyFrom(new byte[]{0x01, 0x02})))
                .addDoubleColumns(DoubleColumn.newBuilder().setName("calc:double").addValues(1.0).addValues(1.5))
                .addFloatColumns(FloatColumn.newBuilder().setName("calc:float").addValues(2.0f).addValues(2.5f))
                .addInt64Columns(Int64Column.newBuilder().setName("calc:int64").addValues(3L).addValues(4L))
                .addInt32Columns(Int32Column.newBuilder().setName("calc:int32").addValues(5).addValues(6))
                .addBoolColumns(BoolColumn.newBuilder().setName("calc:bool").addValues(true).addValues(false))
                .addStringColumns(StringColumn.newBuilder().setName("calc:string").addValues("a").addValues("b"))
                .addEnumColumns(EnumColumn.newBuilder().setName("calc:enum").addValues(0).addValues(1))
                .addDoubleArrayColumns(DoubleArrayColumn.newBuilder()
                        .setName("calc:doubleArray").setDimensions(dims).addValues(1.0).addValues(2.0))
                .addFloatArrayColumns(FloatArrayColumn.newBuilder()
                        .setName("calc:floatArray").setDimensions(dims).addValues(2.0f).addValues(3.0f))
                .addInt32ArrayColumns(Int32ArrayColumn.newBuilder()
                        .setName("calc:int32Array").setDimensions(dims).addValues(7).addValues(8))
                .addInt64ArrayColumns(Int64ArrayColumn.newBuilder()
                        .setName("calc:int64Array").setDimensions(dims).addValues(9L).addValues(10L))
                .addBoolArrayColumns(BoolArrayColumn.newBuilder()
                        .setName("calc:boolArray").setDimensions(dims).addValues(true).addValues(false))
                .addStructColumns(StructColumn.newBuilder()
                        .setName("calc:struct")
                        .setSchemaId("schema-1")
                        .addValues(ByteString.copyFrom(new byte[]{1, 2, 3})))
                .addImageColumns(ImageColumn.newBuilder()
                        .setName("calc:image")
                        .setImageDescriptor(descriptor)
                        .addImages(ByteString.copyFrom(new byte[]{1, 2, 3, 4})))
                .build();

        return Calculations.newBuilder()
                .addCalculationDataFrames(Calculations.CalculationsDataFrame.newBuilder()
                        .setName("frame-all-types")
                        .setFrame(frame))
                .build();
    }

    @Test
    public void testDocumentConversionRoundTrip_allSixteenColumnTypes() throws DpException {
        final Calculations calculations = allColumnTypesCalculations();

        final CalculationsDocument document = CalculationsDocument.fromCalculations(calculations);
        document.setId(new ObjectId());

        assertEquals(16, document.getDataFrames().get(0).getDataColumns().size());
        assertEquals(
                calculations.getCalculationDataFramesList(),
                document.toCalculations().getCalculationDataFramesList());
        assertTrue("diffCalculations must report no differences",
                document.diffCalculations(calculations).isEmpty());
    }

    @Test
    public void testMongoCodecRoundTrip_allSixteenColumnTypes() throws DpException {
        final Calculations calculations = allColumnTypesCalculations();

        final CalculationsDocument document = CalculationsDocument.fromCalculations(calculations);
        document.setId(new ObjectId());
        testClient.calculationsPojo().insertOne(document);

        final CalculationsDocument decoded =
                testClient.calculationsPojo().find(Filters.eq("_id", document.getId())).first();
        assertNotNull(decoded);
        assertEquals(
                calculations.getCalculationDataFramesList(),
                decoded.toCalculations().getCalculationDataFramesList());
    }

    @Test
    public void testFrameColumnNamesMapCoversAllColumnTypes() throws DpException {
        final CalculationsDocument document =
                CalculationsDocument.fromCalculations(allColumnTypesCalculations());

        assertEquals(16, document.frameColumnNamesMap().get("frame-all-types").size());
        assertTrue(document.frameColumnNamesMap().get("frame-all-types").contains("calc:legacy"));
        assertTrue(document.frameColumnNamesMap().get("frame-all-types").contains("calc:image"));
    }
}
