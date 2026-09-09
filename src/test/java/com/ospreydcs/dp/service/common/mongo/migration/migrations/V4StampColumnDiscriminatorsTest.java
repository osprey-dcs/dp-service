package com.ospreydcs.dp.service.common.mongo.migration.migrations;

import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Filters;
import com.ospreydcs.dp.grpc.v1.common.DataColumn;
import com.ospreydcs.dp.grpc.v1.common.DataValue;
import com.ospreydcs.dp.service.common.bson.bucket.BucketDocument;
import com.ospreydcs.dp.service.common.bson.calculations.CalculationsDocument;
import com.ospreydcs.dp.service.common.bson.column.DataColumnDocument;
import com.ospreydcs.dp.service.common.exception.DpException;
import com.ospreydcs.dp.service.common.mongo.MongoClientBase;
import com.ospreydcs.dp.service.common.mongo.MongoTestClient;
import org.bson.Document;
import org.bson.types.ObjectId;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

/**
 * Covers the legacy column discriminator stamping migration (#248 Phase 4, plan D27). Columns
 * written before rel-1.13.0 predate the {@code @BsonDiscriminator} #173 added; both storage sites
 * now declare the abstract {@code ColumnDocumentBase}, under which an entry without {@code _t}
 * fails codec decode. The decode tests here pin both halves of that claim against a real server:
 * an unstamped entry is unreadable through the POJO codec, and a stamped one reads back losslessly.
 */
public class V4StampColumnDiscriminatorsTest {

    private MigrationTestClient testClient;
    private MongoCollection<Document> buckets;
    private MongoCollection<Document> calculations;
    private V4StampColumnDiscriminators migration;

    private static class MigrationTestClient extends MongoTestClient {
        MongoDatabase database() {
            return mongoDatabase;
        }
        MongoCollection<BucketDocument> bucketsPojo() {
            return mongoCollectionBuckets;
        }
        MongoCollection<CalculationsDocument> calculationsPojo() {
            return mongoCollectionCalculations;
        }
    }

    @Before
    public void setUp() {
        testClient = new MigrationTestClient();
        testClient.init();
        buckets = testClient.database().getCollection(MongoClientBase.COLLECTION_NAME_BUCKETS);
        calculations = testClient.database().getCollection(MongoClientBase.COLLECTION_NAME_CALCULATIONS);
        migration = new V4StampColumnDiscriminators();
        buckets.deleteMany(new Document());
        calculations.deleteMany(new Document());
    }

    @After
    public void tearDown() {
        buckets.deleteMany(new Document());
        calculations.deleteMany(new Document());
        testClient.fini();
    }

    private static DataColumn doubleDataColumn(String name) {
        return DataColumn.newBuilder()
                .setName(name)
                .addDataValues(DataValue.newBuilder().setDoubleValue(3.14).build())
                .build();
    }

    /** The pre-1.13 stored shape: name/valueCase/valueType/bytes, no {@code _t}. */
    private static Document legacyColumnSubdocument(DataColumn dataColumn) {
        return new Document("name", dataColumn.getName())
                .append("valueCase", 1)
                .append("valueType", "DOUBLEVALUE")
                .append("bytes", dataColumn.toByteArray());
    }

    private Document storedBucket(String id) {
        return buckets.find(Filters.eq("_id", id)).first();
    }

    @Test
    public void testStampsMissingDiscriminatorOnBuckets() throws DpException {
        buckets.insertOne(new Document("_id", "pv_legacy-100-0")
                .append("pvName", "pv_legacy")
                .append("dataColumn", legacyColumnSubdocument(doubleDataColumn("pv_legacy"))));
        buckets.insertOne(new Document("_id", "pv_typed-100-0")
                .append("pvName", "pv_typed")
                .append("dataColumn", new Document("_t", "doubleColumn")
                        .append("name", "pv_typed")
                        .append("values", List.of(1.0, 2.0))));
        buckets.insertOne(new Document("_id", "pv_v1shaped-100-0")
                .append("pvName", "pv_v1shaped")
                .append("columnDataType", 1));

        migration.apply(testClient.database());

        assertEquals("dataColumn",
                storedBucket("pv_legacy-100-0").get("dataColumn", Document.class).getString("_t"));
        // a typed discriminator is not the missing case and must not be overwritten
        assertEquals("doubleColumn",
                storedBucket("pv_typed-100-0").get("dataColumn", Document.class).getString("_t"));
        // a v1-shaped bucket has no embedded column document to stamp
        assertNull(storedBucket("pv_v1shaped-100-0").get("dataColumn"));
    }

    @Test
    public void testStampsMissingDiscriminatorOnCalculationsColumnEntries() throws DpException {
        final ObjectId mixedId = new ObjectId();
        calculations.insertOne(new Document("_id", mixedId)
                .append("dataFrames", List.of(
                        new Document("name", "frame-mixed")
                                .append("dataColumns", List.of(
                                        legacyColumnSubdocument(doubleDataColumn("calc-unstamped")),
                                        legacyColumnSubdocument(doubleDataColumn("calc-stamped"))
                                                .append("_t", "dataColumn"))),
                        // a frame without a dataColumns array must not fault the positional update
                        new Document("name", "frame-no-columns"))));

        migration.apply(testClient.database());

        final Document stored = calculations.find(Filters.eq("_id", mixedId)).first();
        final List<Document> frames = stored.getList("dataFrames", Document.class);
        final List<Document> mixedColumns = frames.get(0).getList("dataColumns", Document.class);
        assertEquals("dataColumn", mixedColumns.get(0).getString("_t"));
        assertEquals("dataColumn", mixedColumns.get(1).getString("_t"));
        assertNull(frames.get(1).get("dataColumns"));
    }

    @Test
    public void testIdempotentReRun() throws DpException {
        buckets.insertOne(new Document("_id", "pv_rerun-100-0")
                .append("pvName", "pv_rerun")
                .append("dataColumn", legacyColumnSubdocument(doubleDataColumn("pv_rerun"))));
        final ObjectId calculationsId = new ObjectId();
        calculations.insertOne(new Document("_id", calculationsId)
                .append("dataFrames", List.of(new Document("name", "frame-rerun")
                        .append("dataColumns", List.of(
                                legacyColumnSubdocument(doubleDataColumn("calc-rerun")))))));

        migration.apply(testClient.database());
        final Document bucketAfterFirstRun = storedBucket("pv_rerun-100-0");
        final Document calculationsAfterFirstRun = calculations.find(Filters.eq("_id", calculationsId)).first();
        migration.apply(testClient.database());

        assertEquals(bucketAfterFirstRun, storedBucket("pv_rerun-100-0"));
        assertEquals(calculationsAfterFirstRun, calculations.find(Filters.eq("_id", calculationsId)).first());
    }

    @Test
    public void testStampedLegacyBucketDecodesThroughPojoCodec() throws DpException {
        final DataColumn dataColumn = doubleDataColumn("pv_decode");
        buckets.insertOne(new Document("_id", "pv_decode-100-0")
                .append("pvName", "pv_decode")
                .append("dataColumn", legacyColumnSubdocument(dataColumn)));

        // before the migration the entry has no _t: the abstract declared field type cannot decode
        // it, which is the silent zero-buckets failure the migration repairs (finding 3)
        assertThrows(RuntimeException.class, () ->
                testClient.bucketsPojo().find(Filters.eq("_id", "pv_decode-100-0")).first());

        migration.apply(testClient.database());

        final BucketDocument decoded =
                testClient.bucketsPojo().find(Filters.eq("_id", "pv_decode-100-0")).first();
        assertTrue(decoded.getDataColumn() instanceof DataColumnDocument);
        assertEquals(dataColumn, ((DataColumnDocument) decoded.getDataColumn()).toDataColumn());
    }

    @Test
    public void testStampedLegacyCalculationsColumnDecodesThroughPojoCodec() throws DpException {
        final DataColumn dataColumn = doubleDataColumn("calc-decode");
        final ObjectId calculationsId = new ObjectId();
        calculations.insertOne(new Document("_id", calculationsId)
                .append("dataFrames", List.of(new Document("name", "frame-decode")
                        .append("dataColumns", List.of(legacyColumnSubdocument(dataColumn))))));

        assertThrows(RuntimeException.class, () -> {
            final List<CalculationsDocument> results = new ArrayList<>();
            testClient.calculationsPojo().find(Filters.eq("_id", calculationsId)).into(results);
        });

        migration.apply(testClient.database());

        final CalculationsDocument decoded =
                testClient.calculationsPojo().find(Filters.eq("_id", calculationsId)).first();
        assertEquals(1, decoded.getDataFrames().size());
        assertEquals("frame-decode", decoded.getDataFrames().get(0).getName());
        assertEquals(1, decoded.getDataFrames().get(0).getDataColumns().size());
        assertTrue(decoded.getDataFrames().get(0).getDataColumns().get(0) instanceof DataColumnDocument);
        assertEquals(dataColumn,
                ((DataColumnDocument) decoded.getDataFrames().get(0).getDataColumns().get(0)).toDataColumn());
    }
}
