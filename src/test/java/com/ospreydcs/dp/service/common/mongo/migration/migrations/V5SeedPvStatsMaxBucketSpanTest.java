package com.ospreydcs.dp.service.common.mongo.migration.migrations;

import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.Sorts;
import com.ospreydcs.dp.service.common.bson.pvstats.PvStatsDocument;
import com.ospreydcs.dp.service.common.exception.DpException;
import com.ospreydcs.dp.service.common.mongo.MongoClientBase;
import com.ospreydcs.dp.service.common.mongo.MongoTestClient;
import org.bson.BsonDocument;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

/**
 * Covers the pvStats seed migration (#232, plan D10). Every test runs against a real server: the
 * properties that matter — a larger stored value surviving the merge, null and negative group
 * results being skipped, the seeded value decoding through the POJO codec the query side reads
 * with — are all server-side behaviour that a mocked collection could not exercise.
 */
public class V5SeedPvStatsMaxBucketSpanTest {

    private MigrationTestClient testClient;
    private MongoCollection<Document> buckets;
    private MongoCollection<Document> pvStats;
    private V5SeedPvStatsMaxBucketSpan migration;

    private static class MigrationTestClient extends MongoTestClient {
        MongoDatabase database() {
            return mongoDatabase;
        }
        MongoCollection<PvStatsDocument> pvStatsPojo() {
            return mongoCollectionPvStats;
        }
    }

    @Before
    public void setUp() {
        testClient = new MigrationTestClient();
        testClient.init();
        buckets = testClient.database().getCollection(MongoClientBase.COLLECTION_NAME_BUCKETS);
        pvStats = testClient.database().getCollection(MongoClientBase.COLLECTION_NAME_PV_STATS);
        migration = new V5SeedPvStatsMaxBucketSpan();
        buckets.deleteMany(new Document());
        pvStats.deleteMany(new Document());
        legacyCollection().drop();
    }

    @After
    public void tearDown() {
        buckets.deleteMany(new Document());
        pvStats.deleteMany(new Document());
        legacyCollection().drop();
        testClient.fini();
    }

    private MongoCollection<Document> legacyCollection() {
        return testClient.database().getCollection(
                MongoClientBase.COLLECTION_NAME_BUCKET_SPAN_VERIFICATION_LEGACY);
    }

    /** A bucket in the stored shape the seed reads: pvName plus the two timestamp subdocuments. */
    private static Document bucket(String pvName, long firstSeconds, long lastSeconds) {
        return new Document("_id", pvName + "-" + firstSeconds + "-0")
                .append("pvName", pvName)
                .append("dataTimestamps", new Document()
                        .append("firstTime", new Document("seconds", firstSeconds).append("nanos", 0L))
                        .append("lastTime", new Document("seconds", lastSeconds).append("nanos", 0L)));
    }

    /** The stored span for a PV, or null when the PV has no pvStats document. */
    private Long storedSpan(String pvName) {
        final Document document = pvStats.find(Filters.eq("_id", pvName)).first();
        return document == null ? null : document.getLong("maxBucketSpanSeconds");
    }

    private List<Document> allPvStats() {
        return pvStats.find().sort(Sorts.ascending("_id")).into(new ArrayList<>());
    }

    private List<String> collectionNames() {
        return testClient.database().listCollectionNames().into(new ArrayList<>());
    }

    @Test
    public void testSeedsPerPvMaximumFromMixedSpanBuckets() throws DpException {
        buckets.insertMany(List.of(
                bucket("pvA", 100, 160),
                bucket("pvA", 200, 215),
                bucket("pvA", 300, 300),
                bucket("pvB", 1000, 90000),
                bucket("pvC", 5, 5)));

        migration.apply(testClient.database());

        assertEquals(Long.valueOf(60), storedSpan("pvA"));
        assertEquals(Long.valueOf(89000), storedSpan("pvB"));
        assertEquals(Long.valueOf(0), storedSpan("pvC"));
        assertEquals(3, allPvStats().size());
        // the stored value is a BSON int64, the type ingestion writes and the POJO field declares
        assertTrue(pvStats.find(Filters.eq("_id", "pvA")).first().get("maxBucketSpanSeconds") instanceof Long);
    }

    @Test
    public void testApplyTwiceIsNoOp() throws DpException {
        buckets.insertMany(List.of(
                bucket("pvA", 100, 160),
                bucket("pvB", 1000, 90000)));

        migration.apply(testClient.database());
        final List<Document> afterFirstRun = allPvStats();
        migration.apply(testClient.database());

        assertEquals(2, afterFirstRun.size());
        assertEquals(afterFirstRun, allPvStats());
    }

    @Test
    public void testLargerStoredValueSurvivesAndSmallerIsRaised() throws DpException {
        // pvA models ingestion having already recorded a longer bucket than the archive holds
        // (a re-run after ingestion started writing); pvB models a stale smaller value
        pvStats.insertMany(List.of(
                new Document("_id", "pvA").append("maxBucketSpanSeconds", 500000L),
                new Document("_id", "pvB").append("maxBucketSpanSeconds", 1L)));
        buckets.insertMany(List.of(
                bucket("pvA", 100, 160),
                bucket("pvB", 1000, 90000)));

        migration.apply(testClient.database());

        assertEquals(Long.valueOf(500000), storedSpan("pvA"));
        assertEquals(Long.valueOf(89000), storedSpan("pvB"));
        assertEquals(2, allPvStats().size());
    }

    @Test
    public void testEmptyBucketsSeedsNothingAndLeavesExistingStats() throws DpException {
        pvStats.insertOne(new Document("_id", "pvExisting").append("maxBucketSpanSeconds", 42L));

        migration.apply(testClient.database());

        final List<Document> stats = allPvStats();
        assertEquals(1, stats.size());
        assertEquals("pvExisting", stats.get(0).getString("_id"));
        assertEquals(Long.valueOf(42), storedSpan("pvExisting"));
    }

    @Test
    public void testSkipsPvsWhoseEveryBucketIsUnusable() throws DpException {
        // pvMissing: no dataTimestamps on its only bucket, so the group maximum is null
        buckets.insertOne(new Document("_id", "pvMissing-100-0")
                .append("pvName", "pvMissing")
                .append("columnDataType", 1));
        // pvInverted: lastTime before firstTime on its only bucket, so the maximum is negative;
        // its existing pvStats value must survive untouched
        buckets.insertOne(bucket("pvInverted", 500, 400));
        pvStats.insertOne(new Document("_id", "pvInverted").append("maxBucketSpanSeconds", 7L));
        // pvMixed: one bucket of each bad kind plus one well-formed bucket, whose span wins
        buckets.insertOne(new Document("_id", "pvMixed-1-0")
                .append("pvName", "pvMixed")
                .append("columnDataType", 1));
        buckets.insertOne(bucket("pvMixed", 500, 400));
        buckets.insertOne(bucket("pvMixed", 600, 630));

        migration.apply(testClient.database());

        assertNull(storedSpan("pvMissing"));
        assertEquals(Long.valueOf(7), storedSpan("pvInverted"));
        assertEquals(Long.valueOf(30), storedSpan("pvMixed"));
        assertEquals(2, allPvStats().size());
    }

    @Test
    public void testDropsLegacyCollectionAndSucceedsWhenAlreadyAbsent() throws DpException {
        legacyCollection().insertOne(new Document("_id", "bucketSpanVerification")
                .append("maxBucketSpanSeconds", 86400L)
                .append("verifiedAt", new java.util.Date()));
        assertTrue(collectionNames().contains(MongoClientBase.COLLECTION_NAME_BUCKET_SPAN_VERIFICATION_LEGACY));

        migration.apply(testClient.database());
        assertFalse(collectionNames().contains(MongoClientBase.COLLECTION_NAME_BUCKET_SPAN_VERIFICATION_LEGACY));

        // the second run finds no collection to drop and must not fail on that
        migration.apply(testClient.database());
        assertFalse(collectionNames().contains(MongoClientBase.COLLECTION_NAME_BUCKET_SPAN_VERIFICATION_LEGACY));
    }

    @Test
    public void testSeededDocumentDecodesThroughPojoCodec() throws DpException {
        // seconds stored as int32 (a hand-written or foreign bucket) seed an int32 span, which the
        // POJO codec must still widen into the document's long field
        buckets.insertOne(new Document("_id", "pvInt-100-0")
                .append("pvName", "pvInt")
                .append("dataTimestamps", new Document()
                        .append("firstTime", new Document("seconds", 100).append("nanos", 0))
                        .append("lastTime", new Document("seconds", 107).append("nanos", 0))));
        buckets.insertOne(bucket("pvLong", 1000, 90000));

        migration.apply(testClient.database());

        final PvStatsDocument intSeeded = testClient.pvStatsPojo().find(Filters.eq("_id", "pvInt")).first();
        assertNotNull(intSeeded);
        assertEquals("pvInt", intSeeded.getPvName());
        assertEquals(7L, intSeeded.getMaxBucketSpanSeconds());

        final PvStatsDocument longSeeded = testClient.pvStatsPojo().find(Filters.eq("_id", "pvLong")).first();
        assertNotNull(longSeeded);
        assertEquals("pvLong", longSeeded.getPvName());
        assertEquals(89000L, longSeeded.getMaxBucketSpanSeconds());
    }

    /**
     * A legacy bucket whose {@code pvName} is missing, null, or not a string must be skipped, not
     * allowed to reach the {@code $group}. Such a value becomes the {@code $merge} {@code on} key,
     * which the server rejects outright ("'on' field '_id' cannot be missing, null, undefined or an
     * array"), aborting the pipeline having written nothing — so without the guard one malformed
     * bucket fails this migration and, because the runner leaves the claim in place, blocks the
     * startup of every service. The well-formed PVs in the same archive must still be seeded.
     *
     * <p>The array case is the one a plain {@code {pvName: {$type: "string"}}} match would let
     * through: that form traverses into arrays and accepts {@code ["arrayPv"]}, which then fails
     * {@code $merge} as an array {@code _id}. It is here to pin the {@code $expr} form.
     */
    @Test
    public void testSkipsBucketsWithUnusablePvNameAndStillSeedsTheRest() throws DpException {
        buckets.insertOne(bucket("pvGood", 100, 130));
        // pvName missing entirely
        buckets.insertOne(new Document("_id", "noname-100-0")
                .append("dataTimestamps", new Document()
                        .append("firstTime", new Document("seconds", 100L).append("nanos", 0L))
                        .append("lastTime", new Document("seconds", 900L).append("nanos", 0L))));
        // pvName explicitly null
        buckets.insertOne(new Document("_id", "nullname-100-0")
                .append("pvName", null)
                .append("dataTimestamps", new Document()
                        .append("firstTime", new Document("seconds", 100L).append("nanos", 0L))
                        .append("lastTime", new Document("seconds", 900L).append("nanos", 0L))));
        // pvName of the wrong BSON type
        buckets.insertOne(new Document("_id", "intname-100-0")
                .append("pvName", 42)
                .append("dataTimestamps", new Document()
                        .append("firstTime", new Document("seconds", 100L).append("nanos", 0L))
                        .append("lastTime", new Document("seconds", 900L).append("nanos", 0L))));
        // pvName as an array — accepted by a plain $type match, rejected by $merge
        buckets.insertOne(new Document("_id", "arrayname-100-0")
                .append("pvName", List.of("arrayPv"))
                .append("dataTimestamps", new Document()
                        .append("firstTime", new Document("seconds", 100L).append("nanos", 0L))
                        .append("lastTime", new Document("seconds", 900L).append("nanos", 0L))));

        // must not throw
        migration.apply(testClient.database());

        assertEquals(Long.valueOf(30), storedSpan("pvGood"));
        assertEquals(1, allPvStats().size());
    }

    @Test
    public void testPipelineShapeMatchesPlanD10() {
        final List<Bson> pipeline = V5SeedPvStatsMaxBucketSpan.seedPipeline();
        assertEquals(4, pipeline.size());

        // the pvName type guard must come FIRST, ahead of the $group whose _id it feeds
        assertEquals(
                BsonDocument.parse("{$match: {$expr: {$eq: [{$type: '$pvName'}, 'string']}}}"),
                pipeline.get(0).toBsonDocument());

        final BsonDocument group = pipeline.get(1).toBsonDocument();
        assertEquals("$pvName", group.getDocument("$group").getString("_id").getValue());
        assertEquals(
                BsonDocument.parse("{$max: {$subtract: ['$dataTimestamps.lastTime.seconds', '$dataTimestamps.firstTime.seconds']}}"),
                group.getDocument("$group").getDocument("maxBucketSpanSeconds"));

        assertEquals(
                BsonDocument.parse("{$match: {maxBucketSpanSeconds: {$gte: NumberLong(0)}}}"),
                pipeline.get(2).toBsonDocument());

        final BsonDocument merge = pipeline.get(3).toBsonDocument().getDocument("$merge");
        assertEquals(MongoClientBase.COLLECTION_NAME_PV_STATS, merge.getString("into").getValue());
        assertEquals("_id", merge.getString("on").getValue());
        assertEquals("insert", merge.getString("whenNotMatched").getValue());
        assertEquals(
                BsonDocument.parse("{$set: {maxBucketSpanSeconds: {$max: ['$maxBucketSpanSeconds', '$$new.maxBucketSpanSeconds']}}}"),
                merge.getArray("whenMatched").get(0).asDocument());
    }
}
