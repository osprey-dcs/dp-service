package com.ospreydcs.dp.service.query.handler.mongo.client;

import com.mongodb.client.MongoCursor;
import com.mongodb.client.model.Filters;
import com.ospreydcs.dp.grpc.v1.common.Timestamp;
import com.ospreydcs.dp.grpc.v1.query.QueryDataRequest;
import com.ospreydcs.dp.service.common.bson.BsonConstants;
import com.ospreydcs.dp.service.common.bson.bucket.BucketDocument;
import com.ospreydcs.dp.service.common.bson.bucket.BucketUtility;
import com.ospreydcs.dp.service.common.exception.DpException;
import com.ospreydcs.dp.service.common.mongo.MongoClientBase;
import com.ospreydcs.dp.service.common.mongo.MongoTestClient;
import com.ospreydcs.dp.service.ingest.handler.mongo.client.PvStatsMaxSpanUpdater;
import com.ospreydcs.dp.service.query.handler.model.ResolvedQuery;
import com.ospreydcs.dp.service.query.handler.model.TimeInterval;
import org.bson.Document;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

/**
 * Pins how the bucket retrieval methods report a database failure, using the one failure #271 made
 * routine: with every bucket query hinting {@link MongoClientBase#BUCKET_QUERY_INDEX_KEYS}, a
 * missing compound index fails <em>every</em> call with {@code BadValue} "hint provided does not
 * correspond to an existing index" (plan D3 chose that over a silent collection scan).
 *
 * <p>What matters is that the failure arrives as the null cursor, never as a throw. The driver
 * raises it from {@code cursor()} -- the find is issued there, not at first iteration -- so an
 * uncaught one escapes {@code QueryDataJob}/{@code QueryTableJob}/{@code QueryV2Job} into
 * {@code QueueHandlerBase}'s worker, which logs it and takes the next job. The dispatcher's
 * {@code handleResult()} then never runs and the caller's response stream stays open until it
 * times out: no buckets, no error, no completion. Every dispatcher turns a null cursor into an
 * error response instead, which is what the release notes and the SLAC runbook promise operators.
 * The V1 path had exactly this gap when the hint landed (issue #272 review).
 *
 * <p>Dropping the index is simply the most faithful way to provoke a real driver failure on every
 * path; the assertions are about the classification, which is the same for an outage mid-request.
 * The index is restored after each test so no later test in the shared {@code dp-test} database
 * sees the collection without it.
 */
public class MongoSyncQueryClientMissingIndexTest {

    private static final String PV_NAME = "missingindexpv_1";
    /** A second PV in a different span class (#274 D11), so a request naming both opens two cursors. */
    private static final String PV_NAME_LONG_SPAN = "missingindexpv_2";
    private static final String PV_NAME_BASE = "missingindexpv_";
    private static final long BASE_SECONDS = 1_700_000_000L;
    private static final int NUM_BUCKETS = 10;
    private static final int SAMPLES_PER_SECOND = 10;

    private static final long BEGIN_SECONDS = BASE_SECONDS + 2;
    private static final long END_SECONDS = BASE_SECONDS + 6;

    // two disjoint V2 retrieval fragments inside the window, so the hoisted-bounds path is used
    private static final TimeInterval FRAGMENT_1 =
            new TimeInterval(BEGIN_SECONDS, 0L, BASE_SECONDS + 4, 0L);
    private static final TimeInterval FRAGMENT_2 =
            new TimeInterval(BASE_SECONDS + 5, 0L, END_SECONDS, 0L);

    private static class TestSyncClient extends MongoSyncQueryClient {

        void insertBuckets(List<BucketDocument> buckets) {
            mongoCollectionBuckets.insertMany(buckets);
        }

        void recordSpan(String pvName, long spanSeconds) throws DpException {
            new PvStatsMaxSpanUpdater(mongoCollectionPvStats.withDocumentClass(Document.class))
                    .recordSpan(List.of(pvName), spanSeconds);
        }

        void dropBucketQueryIndex() {
            mongoCollectionBuckets.dropIndex(MongoClientBase.BUCKET_QUERY_INDEX_KEYS);
        }

        void createBucketQueryIndex() {
            mongoCollectionBuckets.createIndex(MongoClientBase.BUCKET_QUERY_INDEX_KEYS);
        }

        boolean bucketQueryIndexExists() {
            return mongoCollectionBuckets.listIndexes().into(new java.util.ArrayList<>()).stream()
                    .map(index -> index.get("key", Document.class).toBsonDocument())
                    .anyMatch(MongoClientBase.BUCKET_QUERY_INDEX_KEYS.toBsonDocument()::equals);
        }
    }

    private TestSyncClient client;

    @Before
    public void setUp() throws Exception {
        MongoTestClient.prepareTestDatabase();
        client = new TestSyncClient();
        assertTrue(client.init());

        client.insertBuckets(BucketUtility.createBucketDocuments(
                BASE_SECONDS, SAMPLES_PER_SECOND, 1, PV_NAME_BASE, 2, NUM_BUCKETS));
        client.recordSpan(PV_NAME, 1L);
        client.recordSpan(PV_NAME_LONG_SPAN, 300L);
    }

    @After
    public void tearDown() {
        if (client != null) {
            if (!client.bucketQueryIndexExists()) {
                client.createBucketQueryIndex();
            }
            client.fini();
            client = null;
        }
    }

    /**
     * The V1 path (queryData, queryTable, and the annotation data-block export, which all funnel
     * through executeBucketDocumentQuery). Without the catch this method throws, and the caller
     * hangs.
     */
    @Test
    public void testV1BucketQueryReportsMissingIndexAsNullCursor() throws DpException {
        assertQueryReturnsBucketsThenNullWithoutTheIndex(() -> client.executeBucketDocumentQuery(
                Filters.in(BsonConstants.BSON_KEY_PV_NAME, List.of(PV_NAME)),
                BEGIN_SECONDS, 0L, END_SECONDS, 0L,
                client.resolveMaxBucketSpanSeconds(List.of(PV_NAME))));
    }

    /**
     * A request whose PVs fall in two span classes opens one cursor per class and merges them
     * (#274, plan D11). The healthy case must return both PVs' buckets in {@code (pvName, firstTime)}
     * order -- the merged cursor is what every dispatcher iterates -- and the failure must still be
     * the null cursor: the second class's {@code cursor()} is where the driver throws here, after
     * the first was opened, and that throw must reach the same catch.
     */
    @Test
    public void testMultiClassRequestMergesBucketsAndReportsMissingIndexAsNullCursor() throws DpException {
        final List<String> pvNames = List.of(PV_NAME, PV_NAME_LONG_SPAN);
        assertEquals("fixture must span two classes", 2, client.resolveSpanClasses(pvNames).size());

        final QueryDataRequest.QuerySpec querySpec = QueryDataRequest.QuerySpec.newBuilder()
                .addAllPvNames(pvNames)
                .setBeginTime(Timestamp.newBuilder().setEpochSeconds(BEGIN_SECONDS))
                .setEndTime(Timestamp.newBuilder().setEpochSeconds(END_SECONDS))
                .build();

        try (final MongoCursor<BucketDocument> cursor = client.executeQueryData(querySpec)) {
            assertNotNull(cursor);
            final List<String> keys = new java.util.ArrayList<>();
            while (cursor.hasNext()) {
                final BucketDocument bucket = cursor.next();
                keys.add(bucket.getPvName() + "@" + bucket.getDataTimestamps().getFirstTime().getSeconds());
            }
            // four buckets per PV overlap [BASE+2, BASE+6), PV_NAME's first in sort order
            final List<String> expected = new java.util.ArrayList<>();
            for (String pvName : pvNames) {
                for (long second = BEGIN_SECONDS; second < END_SECONDS; second++) {
                    expected.add(pvName + "@" + second);
                }
            }
            assertEquals(expected, keys);
        }

        assertQueryReturnsBucketsThenNullWithoutTheIndex(() -> client.executeQueryData(querySpec));
    }

    /** The V2 unary buckets path. */
    @Test
    public void testV2BucketsQueryReportsMissingIndexAsNullCursor() {
        assertQueryReturnsBucketsThenNullWithoutTheIndex(
                () -> client.executeQueryBucketsV2(resolvedQuery(false)));
    }

    /** The V2 streaming buckets path. */
    @Test
    public void testV2BucketsStreamQueryReportsMissingIndexAsNullCursor() {
        assertQueryReturnsBucketsThenNullWithoutTheIndex(
                () -> client.executeQueryBucketsV2Stream(resolvedQuery(false)));
    }

    /** The V2 samples path, whose fragment $or is built separately. */
    @Test
    public void testV2SamplesQueryReportsMissingIndexAsNullCursor() {
        assertQueryReturnsBucketsThenNullWithoutTheIndex(
                () -> client.executeQuerySamplesV2(resolvedQuery(true), BEGIN_SECONDS, 0L, END_SECONDS, 0L));
    }

    /**
     * An empty retrieval-interval list is a caller bug, not a query: the hoisted bounds would
     * otherwise come from the loop's Long.MAX_VALUE/MIN_VALUE sentinels and match nothing, which
     * reads as an ordinary empty result. Both production callers screen it, so this pins the
     * guard through the package-private builder.
     */
    @Test
    public void testEmptyRetrievalIntervalsIsRejected() {
        final ResolvedQuery resolvedQuery = new ResolvedQuery(
                List.of(PV_NAME), List.of(FRAGMENT_1, FRAGMENT_2), 100, null, false, false,
                ResolvedQuery.ResultMode.SAMPLE, false);
        final IllegalArgumentException ex = assertThrows(
                IllegalArgumentException.class,
                () -> client.bucketSamplesQueryV2(resolvedQuery, List.of(), 1L));
        assertTrue(ex.getMessage(), ex.getMessage().contains("at least one retrieval interval"));
    }

    private static ResolvedQuery resolvedQuery(boolean samples) {
        return new ResolvedQuery(
                List.of(PV_NAME),
                List.of(FRAGMENT_1, FRAGMENT_2),
                100,
                null,
                false,
                false,
                samples ? ResolvedQuery.ResultMode.SAMPLE : ResolvedQuery.ResultMode.BUCKET,
                false);
    }

    @FunctionalInterface
    private interface CursorSupplier {
        MongoCursor<BucketDocument> get() throws DpException;
    }

    /**
     * With the hinted index present the query returns a usable cursor; with it dropped the same
     * call returns null rather than throwing. Asserting the healthy case first keeps a query that
     * is broken for an unrelated reason from passing as a "correctly reported failure".
     */
    private void assertQueryReturnsBucketsThenNullWithoutTheIndex(CursorSupplier supplier) {
        try {
            try (final MongoCursor<BucketDocument> cursor = supplier.get()) {
                assertNotNull("query returned a null cursor with the index present", cursor);
                assertTrue("query returned no buckets with the index present", cursor.hasNext());
            }

            client.dropBucketQueryIndex();

            assertNull("a missing hinted index must be reported as a null cursor, not a throw",
                    supplier.get());
        } catch (DpException ex) {
            throw new AssertionError("unexpected checked exception", ex);
        }
    }
}
