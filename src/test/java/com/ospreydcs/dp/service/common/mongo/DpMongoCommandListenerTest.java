package com.ospreydcs.dp.service.common.mongo;

import com.mongodb.MongoException;
import com.ospreydcs.dp.service.common.telemetry.DpMetrics;
import com.ospreydcs.dp.service.common.telemetry.DpTelemetry;
import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.sdk.OpenTelemetrySdk;
import io.opentelemetry.sdk.metrics.SdkMeterProvider;
import io.opentelemetry.sdk.metrics.data.HistogramPointData;
import io.opentelemetry.sdk.metrics.data.MetricData;
import io.opentelemetry.sdk.testing.exporter.InMemoryMetricReader;
import org.bson.BsonDocument;
import org.bson.BsonInt64;
import org.bson.BsonString;
import org.bson.Document;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Covers the D4 command listener against a real MongoDB (issue #212): that every command this
 * process issues produces a {@code db.client.operation.duration} point, that the point carries the
 * operation, collection and namespace an operator filters on, and — the redaction half of D8 —
 * that nothing from the query filter is attached.
 *
 * <p>Runs against the {@code dp-test} database, but deliberately does not use
 * {@code MongoTestClient.init()}: that drops the database globally, which this test has no reason
 * to do. It connects through {@link MongoSyncClient}'s own client construction instead, which is
 * also what registers the listener — so a change that dropped the registration fails this test
 * rather than passing it against a hand-attached copy.
 */
@RunWith(JUnit4.class)
public class DpMongoCommandListenerTest {

    private static final String COLLECTION_NAME = "dpMongoCommandListenerTest";

    private InMemoryMetricReader metricReader;
    private OpenTelemetrySdk telemetrySdk;
    private ListenerTestClient client;

    /** Exposes the production client's database handle without dropping anything. */
    private static class ListenerTestClient extends MongoSyncClient {

        MongoDatabaseHandle open() {
            if (!initMongoClient(getMongoConnectString())) {
                fail("could not connect to mongo");
            }
            return new MongoDatabaseHandle(
                    mongoClient.getDatabase(MongoTestClient.MONGO_TEST_DATABASE_NAME));
        }

        void close() {
            if (mongoClient != null) {
                mongoClient.close();
                mongoClient = null;
            }
        }
    }

    /** Small holder so the test reads as database work rather than client plumbing. */
    private record MongoDatabaseHandle(com.mongodb.client.MongoDatabase database) {
    }

    private MongoDatabaseHandle handle;

    @Before
    public void setUp() {
        client = new ListenerTestClient();
        handle = client.open();

        // the connection handshake issues commands of its own; install the reader afterwards so
        // the assertions see only the commands each test issues
        metricReader = InMemoryMetricReader.create();
        telemetrySdk = OpenTelemetrySdk.builder()
                .setMeterProvider(
                        SdkMeterProvider.builder().registerMetricReader(metricReader).build())
                .build();
        DpTelemetry.resetForTest();
        DpTelemetry.initForTest(telemetrySdk);
        DpMongoCommandListener.clearThreadStateForTest();
    }

    @After
    public void tearDown() {
        if (handle != null) {
            try {
                handle.database().getCollection(COLLECTION_NAME).drop();
            } catch (MongoException ex) {
                // best effort; the test database is disposable
            }
            handle = null;
        }
        if (client != null) {
            client.close();
            client = null;
        }
        DpTelemetry.resetForTest();
        if (telemetrySdk != null) {
            telemetrySdk.close();
            telemetrySdk = null;
        }
        metricReader = null;
    }

    private List<HistogramPointData> dbPoints() {
        final MetricData metric = metricReader.collectAllMetrics().stream()
                .filter(m -> m.getName().equals(DpMetrics.METRIC_DB_OPERATION_DURATION))
                .findFirst()
                .orElse(null);
        assertNotNull("no db.client.operation.duration metric was recorded", metric);
        return List.copyOf(metric.getHistogramData().getPoints());
    }

    private HistogramPointData pointFor(String operation, String collection) {
        return dbPoints().stream()
                .filter(point -> operation.equals(
                        point.getAttributes().get(DpMetrics.ATTR_DB_OPERATION_NAME)))
                .filter(point -> collection == null || collection.equals(
                        point.getAttributes().get(DpMetrics.ATTR_DB_COLLECTION_NAME)))
                .findFirst()
                .orElseThrow(() -> new AssertionError(
                        "no point for operation=" + operation + " collection=" + collection
                                + "; recorded: " + describePoints()));
    }

    private String describePoints() {
        return dbPoints().stream()
                .map(point -> point.getAttributes().get(DpMetrics.ATTR_DB_OPERATION_NAME)
                        + "/" + point.getAttributes().get(DpMetrics.ATTR_DB_COLLECTION_NAME))
                .collect(Collectors.joining(", "));
    }

    /**
     * An insert and a find each record a point carrying the operation, the collection and the
     * namespace. These are the attributes a dashboard groups by, and they come from the driver's
     * command events rather than from any per-call instrumentation — which is the whole point of
     * D4: every collection and every service is covered without a line of call-site code.
     */
    @Test
    public void testInsertAndFindRecordTheirOperations() {

        final var collection = handle.database().getCollection(COLLECTION_NAME);
        collection.insertOne(new Document("_id", "one").append("pvName", "S01-GCC01"));

        final Document found = collection.find(new Document("_id", "one")).first();
        assertNotNull(found);

        final HistogramPointData insert = pointFor("insert", COLLECTION_NAME);
        assertEquals(1, insert.getCount());
        assertTrue("insert duration was not positive", insert.getSum() > 0.0);
        assertEquals(
                MongoTestClient.MONGO_TEST_DATABASE_NAME,
                insert.getAttributes().get(DpMetrics.ATTR_DB_NAMESPACE));

        final HistogramPointData find = pointFor("find", COLLECTION_NAME);
        assertEquals(1, find.getCount());
        assertEquals(
                MongoTestClient.MONGO_TEST_DATABASE_NAME,
                find.getAttributes().get(DpMetrics.ATTR_DB_NAMESPACE));

        // seconds, not millis or nanos: a command against a local server is well under a second
        assertTrue(
                "find duration " + find.getSum() + " is not seconds-scaled",
                find.getSum() > 0.0 && find.getSum() < 10.0);
    }

    /**
     * A command the server rejects records {@code error.type}.
     *
     * <p>The failure used here is a malformed command rather than a duplicate-key insert, and that
     * choice is a finding worth keeping: a duplicate key is a <em>write error carried in the
     * response body of a command the server answered successfully</em>, so the driver throws to the
     * caller while emitting {@code commandSucceeded}. Nothing in {@code error.type} will ever
     * reflect a duplicate key, a failed validation, or any other per-document write error — only
     * commands the server refused outright. An alert written on the assumption that every database
     * exception raises this rate would never fire.
     *
     * <p>Note also that a total database outage does not appear here at all: a server-selection
     * failure emits no command events. This attribute is a signal about commands the server
     * answered with an error, not about reachability.
     */
    @Test
    public void testFailedCommandRecordsErrorType() {

        try {
            // an unknown command: the server refuses it, so the driver reports commandFailed
            handle.database().runCommand(new Document("thisCommandDoesNotExist", 1));
            fail("the malformed command succeeded");
        } catch (MongoException expected) {
            // the command failure is what is being measured
        }

        final List<HistogramPointData> errorPoints = dbPoints().stream()
                .filter(point -> point.getAttributes().get(DpMetrics.ATTR_ERROR_TYPE) != null)
                .toList();
        assertEquals("expected exactly one failed command: " + describePoints(), 1, errorPoints.size());

        final HistogramPointData failure = errorPoints.get(0);
        assertEquals(
                "thisCommandDoesNotExist",
                failure.getAttributes().get(DpMetrics.ATTR_DB_OPERATION_NAME));
        assertNotNull(failure.getAttributes().get(DpMetrics.ATTR_ERROR_TYPE));
        assertEquals(
                MongoTestClient.MONGO_TEST_DATABASE_NAME,
                failure.getAttributes().get(DpMetrics.ATTR_DB_NAMESPACE));
    }

    /**
     * A duplicate-key insert throws to the caller but is recorded <b>without</b> {@code error.type},
     * because the server answered the command successfully and reported the write error inside the
     * response. Pinned deliberately: this is the gap between "the caller saw an exception" and
     * "the database metric shows an error", and it is the kind of thing a dashboard is built on the
     * wrong assumption about.
     */
    @Test
    public void testWriteErrorIsNotACommandFailure() {

        final var collection = handle.database().getCollection(COLLECTION_NAME);
        collection.insertOne(new Document("_id", "duplicate"));

        try {
            collection.insertOne(new Document("_id", "duplicate"));
            fail("the duplicate key insert succeeded");
        } catch (MongoException expected) {
            // the caller does see an exception
        }

        final HistogramPointData insert = pointFor("insert", COLLECTION_NAME);
        assertEquals("both inserts should be recorded", 2, insert.getCount());
        org.junit.Assert.assertNull(
                "a write error was recorded as a command failure",
                insert.getAttributes().get(DpMetrics.ATTR_ERROR_TYPE));
    }

    /**
     * The D8 redaction guard. A query filter carries PV names, and the listener reads only the
     * collection name out of the command document — so a filter value must never appear as an
     * attribute key or value. This is the assertion that would catch someone "improving" the
     * listener by attaching the filter for diagnosis.
     */
    @Test
    public void testFilterContentNeverBecomesAnAttribute() {

        final String secretPvName = "S99-SECRET-PV-NAME";
        handle.database().getCollection(COLLECTION_NAME)
                .find(new Document("pvName", secretPvName)).first();

        final Set<String> allowedKeys = Set.of(
                DpMetrics.ATTR_DB_OPERATION_NAME.getKey(),
                DpMetrics.ATTR_DB_COLLECTION_NAME.getKey(),
                DpMetrics.ATTR_DB_NAMESPACE.getKey(),
                DpMetrics.ATTR_ERROR_TYPE.getKey());

        for (HistogramPointData point : dbPoints()) {
            for (var entry : point.getAttributes().asMap().entrySet()) {
                final AttributeKey<?> key = entry.getKey();
                assertTrue(
                        "db metric carries attribute outside the D8 vocabulary: " + key.getKey(),
                        allowedKeys.contains(key.getKey()));
                assertTrue(
                        "the query filter's PV name leaked into attribute " + key.getKey(),
                        !String.valueOf(entry.getValue()).contains(secretPvName));
            }
        }
    }

    /**
     * {@code getMore} carries the cursor id in its first field and the collection in a separate
     * one, so it is the single command shape the naive "first field" read gets wrong — attributing
     * a batch fetch to a collection named by a numeric cursor id, or to nothing at all.
     */
    @Test
    public void testGetMoreIsAttributedToItsCollection() {

        final var collection = handle.database().getCollection(COLLECTION_NAME);
        final List<Document> documents = new java.util.ArrayList<>();
        for (int i = 0; i < 50; i++) {
            documents.add(new Document("_id", i).append("value", i));
        }
        collection.insertMany(documents);

        // a batch size below the document count forces at least one getMore round-trip
        final List<Document> read = collection.find().batchSize(10).into(new java.util.ArrayList<>());
        assertEquals(50, read.size());

        final HistogramPointData getMore = pointFor("getMore", COLLECTION_NAME);
        assertTrue("getMore was not recorded against its collection", getMore.getCount() >= 1);
    }

    /** The command-shape parser, over the shapes the listener actually meets. */
    @Test
    public void testCollectionNameFromCommandShapes() {

        // find/insert/aggregate and friends: collection is the value of the first field
        assertEquals(
                "buckets",
                DpMongoCommandListener.collectionNameFromCommand(
                        new BsonDocument("find", new BsonString("buckets"))));

        // getMore: first field is the cursor id, collection is its own field
        final BsonDocument getMore = new BsonDocument("getMore", new BsonInt64(1234567890L));
        getMore.put("collection", new BsonString("buckets"));
        assertEquals("buckets", DpMongoCommandListener.collectionNameFromCommand(getMore));

        // commands naming no collection
        assertEquals(
                "",
                DpMongoCommandListener.collectionNameFromCommand(
                        new BsonDocument("dropDatabase", new BsonInt64(1))));
        assertEquals("", DpMongoCommandListener.collectionNameFromCommand(new BsonDocument()));
        assertEquals("", DpMongoCommandListener.collectionNameFromCommand(null));
    }

    /**
     * A command that names no collection records without one, and does not inherit the collection
     * of the command that preceded it on the same thread. The listener clears its per-thread slot
     * in a {@code finally} for exactly this reason: a stale name would attribute one collection's
     * latency to another, which is a plausible wrong number rather than an error.
     */
    @Test
    public void testCollectionlessCommandDoesNotInheritThePreviousCollection() {

        // a command against the test collection, then one that names no collection, on this thread
        handle.database().getCollection(COLLECTION_NAME).find().first();
        handle.database().runCommand(new Document("ping", 1));

        final HistogramPointData ping = dbPoints().stream()
                .filter(point -> "ping".equals(
                        point.getAttributes().get(DpMetrics.ATTR_DB_OPERATION_NAME)))
                .findFirst()
                .orElseThrow(() -> new AssertionError("ping was not recorded: " + describePoints()));

        org.junit.Assert.assertNull(
                "a collectionless command inherited the previous command's collection",
                ping.getAttributes().get(DpMetrics.ATTR_DB_COLLECTION_NAME));
    }
}
