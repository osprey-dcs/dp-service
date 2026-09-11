package com.ospreydcs.dp.service.query.handler.mongo.client;

import com.mongodb.ExplainVerbosity;
import com.mongodb.client.model.Filters;
import com.ospreydcs.dp.service.common.bson.BsonConstants;
import com.ospreydcs.dp.service.common.bson.bucket.BucketDocument;
import com.ospreydcs.dp.service.common.bson.bucket.BucketUtility;
import com.ospreydcs.dp.service.common.exception.DpException;
import com.ospreydcs.dp.service.common.mongo.MongoTestClient;
import com.ospreydcs.dp.service.ingest.handler.mongo.client.PvStatsMaxSpanUpdater;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/**
 * Plan-shape tests for the V1 bucket retrieval query (#232, plan Task 10): the per-PV
 * {@code firstTime.seconds} lower bound resolved from {@code pvStats} must reach the planner as an
 * index bound on the compound {@code (pvName, firstTime, ...)} index, not merely as a filter.
 *
 * <p>Why explain rather than results: the bound is a performance device, and every way of losing
 * it -- a predicate moved inside an {@code $or}, a changed index declaration, a sort the index
 * cannot serve -- still returns the right buckets, only after scanning each PV's whole history.
 * Result-level tests and filter-BSON equality tests (see {@code MongoQueryFilterBuilderTest})
 * cannot observe that, and the repo had no plan-shape test before this one (plan triage 5). The
 * two halves of the overlap predicate are nested {@code (seconds, nanos)} {@code $or}s the planner
 * cannot turn into bounds, so they run as a residual filter on the fetched documents; the scan
 * size is set entirely by the {@code firstTime.seconds >= begin - span} bound this test pins.
 *
 * <p>The query under test is {@link MongoSyncQueryClient#bucketDocumentQuery}, the exact filter
 * and sort the service issues, with the name filter built the way {@code executeQueryData} builds
 * it. The span is resolved through the production {@code resolveMaxBucketSpanSeconds} from
 * {@code pvStats} documents seeded through the production updater, so each test pins the chain
 * from stored statistic to planner bound.
 *
 * <p>Fixture: two PVs with one-second buckets over three hundred consecutive seconds, and a
 * seeded span of 300 s for the first PV and 7 s for the second. The window queried lies near the
 * end of that history so the two spans give visibly different scans.
 *
 * <p>The plan walker reads the unsharded explain shape (a stage tree under
 * {@code queryPlanner.winningPlan}, or under its {@code queryPlan} when the slot-based engine
 * reports); a sharded explain nests one such tree per shard and is not handled.
 */
public class MongoBucketQueryPlanTest {

    private static final String PV_NAME_BASE = "planpv_";
    private static final String PV_1 = PV_NAME_BASE + "1";
    private static final String PV_2 = PV_NAME_BASE + "2";
    private static final String PV_WITHOUT_STATS = PV_NAME_BASE + "none";

    private static final long BASE_SECONDS = 1_700_000_000L;
    private static final int NUM_BUCKETS_PER_PV = 300;
    private static final int SAMPLES_PER_SECOND = 10;
    private static final long SPAN_PV_1_SECONDS = 300L;
    private static final long SPAN_PV_2_SECONDS = 7L;

    // query window [BASE+290, BASE+295): the five buckets starting at seconds 290..294
    private static final long BEGIN_SECONDS = BASE_SECONDS + 290;
    private static final long END_SECONDS = BASE_SECONDS + 295;
    private static final int BUCKETS_IN_WINDOW = 5;

    private static final String KEY_STAGE = "stage";
    private static final String KEY_KEY_PATTERN = "keyPattern";
    private static final String KEY_INDEX_BOUNDS = "indexBounds";
    private static final String STAGE_COLLSCAN = "COLLSCAN";
    private static final String STAGE_IXSCAN = "IXSCAN";

    /**
     * An inclusive interval as explain renders it, e.g. {@code "[1700000283, inf.0]"}; the group
     * is the lower bound. Anchored on {@code [}: an exclusive {@code (} would drop the buckets
     * starting exactly {@code span} seconds before {@code begin}, which can still overlap.
     */
    private static final Pattern INCLUSIVE_LOWER_BOUND = Pattern.compile("^\\[(-?\\d+)(?:\\.0)?,");

    private static class TestSyncClient extends MongoSyncQueryClient {

        void insertBuckets(List<BucketDocument> buckets) {
            mongoCollectionBuckets.insertMany(buckets);
        }

        /** Seeds pvStats through the production updater, as ingestion does. */
        void recordSpan(String pvName, long spanSeconds) throws DpException {
            new PvStatsMaxSpanUpdater(mongoCollectionPvStats.withDocumentClass(Document.class))
                    .recordSpan(List.of(pvName), spanSeconds);
        }
    }

    private static TestSyncClient client;

    @BeforeClass
    public static void setUp() throws Exception {
        MongoTestClient.prepareTestDatabase();
        client = new TestSyncClient();
        assertTrue(client.init());

        // one-second buckets for PV_1 and PV_2 (BucketUtility names them base + 1, base + 2)
        final List<BucketDocument> buckets = BucketUtility.createBucketDocuments(
                BASE_SECONDS, SAMPLES_PER_SECOND, 1, PV_NAME_BASE, 2, NUM_BUCKETS_PER_PV);
        assertEquals(2 * NUM_BUCKETS_PER_PV, buckets.size());
        client.insertBuckets(buckets);

        client.recordSpan(PV_1, SPAN_PV_1_SECONDS);
        client.recordSpan(PV_2, SPAN_PV_2_SECONDS);
    }

    @AfterClass
    public static void tearDown() {
        if (client != null) {
            client.fini();
            client = null;
        }
    }

    @Test
    public void testNamedPvsBoundFirstTimeSecondsAtBeginMinusResolvedSpan() throws DpException {
        final List<String> pvNames = List.of(PV_1, PV_2);
        final long spanSeconds = client.resolveMaxBucketSpanSeconds(pvNames);
        assertEquals("span is the maximum over the request's PVs (plan D1)", SPAN_PV_1_SECONDS, spanSeconds);

        final Document winningPlan = winningPlan(explain(pvNames, spanSeconds, ExplainVerbosity.QUERY_PLANNER));
        assertIndexScanBoundedAt(winningPlan, BEGIN_SECONDS - SPAN_PV_1_SECONDS, pvNames);
    }

    @Test
    public void testSinglePvBoundUsesItsOwnSpan() throws DpException {
        // the per-PV point of #232: a query naming only the short-span PV gets the tighter bound,
        // not the largest span in the archive
        final List<String> pvNames = List.of(PV_2);
        final long spanSeconds = client.resolveMaxBucketSpanSeconds(pvNames);
        assertEquals(SPAN_PV_2_SECONDS, spanSeconds);

        final Document winningPlan = winningPlan(explain(pvNames, spanSeconds, ExplainVerbosity.QUERY_PLANNER));
        assertIndexScanBoundedAt(winningPlan, BEGIN_SECONDS - SPAN_PV_2_SECONDS, pvNames);
    }

    @Test
    public void testPvWithoutStatsBoundsAtBegin() throws DpException {
        // plan D6: no pvStats document contributes nothing, so the bound sits at begin itself
        final List<String> pvNames = List.of(PV_WITHOUT_STATS);
        final long spanSeconds = client.resolveMaxBucketSpanSeconds(pvNames);
        assertEquals(0L, spanSeconds);

        final Document winningPlan = winningPlan(explain(pvNames, spanSeconds, ExplainVerbosity.QUERY_PLANNER));
        assertIndexScanBoundedAt(winningPlan, BEGIN_SECONDS, pvNames);
    }

    @Test
    public void testTighterSpanExaminesFewerKeysForTheSameResult() {
        // Same PV, same window, same five buckets returned; only the span differs. The index scan
        // runs from begin - span to the end of the PV's history: the "firstTime < end" half of the
        // overlap predicate is a residual filter, not an upper index bound, so keys examined is
        // the count of the PV's buckets from the lower bound onward.
        final List<String> pvNames = List.of(PV_2);

        final Document tight = explain(pvNames, SPAN_PV_2_SECONDS, ExplainVerbosity.EXECUTION_STATS);
        final Document wide = explain(pvNames, SPAN_PV_1_SECONDS, ExplainVerbosity.EXECUTION_STATS);

        assertEquals(BUCKETS_IN_WINDOW, executionStat(tight, "nReturned"));
        assertEquals(BUCKETS_IN_WINDOW, executionStat(wide, "nReturned"));

        final long tightLowerBoundSeconds = BEGIN_SECONDS - SPAN_PV_2_SECONDS;
        final long expectedTightKeys = NUM_BUCKETS_PER_PV - (tightLowerBoundSeconds - BASE_SECONDS);
        assertEquals("keys examined under the PV's own span", expectedTightKeys, executionStat(tight, "totalKeysExamined"));

        // begin - 300 precedes the PV's first bucket, so the wide bound scans its whole history
        assertEquals("keys examined under the archive-wide span", NUM_BUCKETS_PER_PV, executionStat(wide, "totalKeysExamined"));
    }

    /**
     * Runs explain on the exact query the service issues for {@code pvNames} over the fixture
     * window, with the name filter built as {@code executeQueryData} builds it.
     */
    private static Document explain(List<String> pvNames, long maxBucketSpanSeconds, ExplainVerbosity verbosity) {
        final Bson pvNameFilter = Filters.in(BsonConstants.BSON_KEY_PV_NAME, pvNames);
        return client.bucketDocumentQuery(pvNameFilter, BEGIN_SECONDS, 0L, END_SECONDS, 0L, maxBucketSpanSeconds)
                .explain(verbosity);
    }

    private static Document winningPlan(Document explanation) {
        final Document winningPlan = explanation.getEmbedded(List.of("queryPlanner", "winningPlan"), Document.class);
        assertNotNull("explain output has no queryPlanner.winningPlan: " + explanation.toJson(), winningPlan);
        return winningPlan;
    }

    private static long executionStat(Document explanation, String statName) {
        final Number value = explanation.getEmbedded(List.of("executionStats", statName), Number.class);
        assertNotNull("explain output has no executionStats." + statName + ": " + explanation.toJson(), value);
        return value.longValue();
    }

    /**
     * The winning plan must contain no collection scan and at least one index scan; every index
     * scan must be on the compound index led by {@code (pvName, firstTime.seconds)}, with a single
     * inclusive interval on {@code firstTime.seconds} whose lower end is {@code expectedLowerBound}
     * and one point interval on {@code pvName} per named PV.
     */
    private static void assertIndexScanBoundedAt(Document winningPlan, long expectedLowerBound, List<String> pvNames) {
        final String planJson = winningPlan.toJson();

        final List<Document> stages = new ArrayList<>();
        collectStages(winningPlan, stages);
        assertFalse("no stages found in winning plan: " + planJson, stages.isEmpty());

        final List<Document> indexScans = new ArrayList<>();
        for (Document stage : stages) {
            final String stageName = stage.getString(KEY_STAGE);
            assertNotEquals("winning plan contains a collection scan: " + planJson, STAGE_COLLSCAN, stageName);
            if (STAGE_IXSCAN.equals(stageName)) {
                indexScans.add(stage);
            }
        }
        assertFalse("winning plan has no index scan: " + planJson, indexScans.isEmpty());

        for (Document indexScan : indexScans) {
            final Document keyPattern = indexScan.get(KEY_KEY_PATTERN, Document.class);
            assertNotNull("index scan has no keyPattern: " + planJson, keyPattern);
            final List<String> keyNames = new ArrayList<>(keyPattern.keySet());
            assertTrue("index scan is not on the (pvName, firstTime.seconds, ...) index: " + planJson,
                    keyNames.size() >= 2
                            && BsonConstants.BSON_KEY_PV_NAME.equals(keyNames.get(0))
                            && BsonConstants.BSON_KEY_BUCKET_FIRST_TIME_SECS.equals(keyNames.get(1)));

            final Document indexBounds = indexScan.get(KEY_INDEX_BOUNDS, Document.class);
            assertNotNull("index scan has no indexBounds: " + planJson, indexBounds);

            final List<String> secondsIntervals =
                    indexBounds.getList(BsonConstants.BSON_KEY_BUCKET_FIRST_TIME_SECS, String.class);
            assertNotNull("no bounds on firstTime.seconds: " + planJson, secondsIntervals);
            assertEquals("expected one interval on firstTime.seconds: " + secondsIntervals, 1, secondsIntervals.size());
            assertEquals("firstTime.seconds lower bound in " + secondsIntervals.get(0),
                    expectedLowerBound, inclusiveLowerBound(secondsIntervals.get(0)));

            final List<String> pvNameIntervals = indexBounds.getList(BsonConstants.BSON_KEY_PV_NAME, String.class);
            assertNotNull("no bounds on pvName: " + planJson, pvNameIntervals);
            assertEquals("one point interval per named PV: " + pvNameIntervals, pvNames.size(), pvNameIntervals.size());
            for (String pvName : pvNames) {
                final String pointInterval = "[\"" + pvName + "\", \"" + pvName + "\"]";
                assertTrue("missing point interval " + pointInterval + " in " + pvNameIntervals,
                        pvNameIntervals.contains(pointInterval));
            }
        }
    }

    private static long inclusiveLowerBound(String interval) {
        final Matcher matcher = INCLUSIVE_LOWER_BOUND.matcher(interval);
        assertTrue("not an inclusive numeric interval: " + interval, matcher.find());
        return Long.parseLong(matcher.group(1));
    }

    /** Collects every stage document in the plan tree, depth first. */
    private static void collectStages(Document node, List<Document> stages) {
        if (node.containsKey(KEY_STAGE)) {
            stages.add(node);
        }
        for (String childKey : List.of("queryPlan", "inputStage", "outerStage", "innerStage", "thenStage", "elseStage")) {
            if (node.get(childKey) instanceof Document child) {
                collectStages(child, stages);
            }
        }
        if (node.get("inputStages") instanceof List<?> children) {
            for (Object child : children) {
                if (child instanceof Document childDocument) {
                    collectStages(childDocument, stages);
                }
            }
        }
    }
}
