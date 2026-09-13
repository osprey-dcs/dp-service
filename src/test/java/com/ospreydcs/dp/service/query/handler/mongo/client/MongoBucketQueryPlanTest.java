package com.ospreydcs.dp.service.query.handler.mongo.client;

import com.mongodb.ExplainVerbosity;
import com.mongodb.client.FindIterable;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.Indexes;
import com.ospreydcs.dp.service.common.bson.BsonConstants;
import com.ospreydcs.dp.service.common.bson.bucket.BucketDocument;
import com.ospreydcs.dp.service.common.bson.bucket.BucketUtility;
import com.ospreydcs.dp.service.common.exception.DpException;
import com.ospreydcs.dp.service.common.mongo.MongoClientBase;
import com.ospreydcs.dp.service.common.mongo.MongoQueryFilterBuilder;
import com.ospreydcs.dp.service.common.mongo.MongoTestClient;
import com.ospreydcs.dp.service.ingest.handler.mongo.client.PvStatsMaxSpanUpdater;
import com.ospreydcs.dp.service.query.handler.model.KeysetPosition;
import com.ospreydcs.dp.service.query.handler.model.ResolvedQuery;
import com.ospreydcs.dp.service.query.handler.model.TimeInterval;
import org.bson.BsonDocument;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static com.mongodb.client.model.Sorts.ascending;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/**
 * Plan-shape tests for the bucket retrieval queries (#232 plan Task 10, extended by #271): the
 * per-PV {@code firstTime.seconds} lower bound resolved from {@code pvStats} must reach the
 * planner as an index bound on the compound {@code (pvName, firstTime, ...)} index, not merely
 * as a filter, and the plan must be that index's streaming plan even when the collection carries
 * other {@code pvName}-prefixed indexes.
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
 * <p>Why an adversarial index set (#271): a long-lived archive carries indexes the current code
 * never declared -- the standalone {@code pvName_1} retired in rel-1.15.0 (#197), the
 * {@code (pvName, firstTime.seconds, firstTime.nanos)} compound retired in beta-1.6.0, and an
 * operator-built {@code (pvName, lastTime, firstTime)} -- because startup never drops an index.
 * Each is a candidate for these queries; the multi-plan trial then executes every candidate's scan
 * before choosing, and on a recent-window query the planner was measured choosing the
 * {@code lastTime}-led index with a blocking {@code SORT}. The fixture creates all three so the
 * assertions hold only if {@code MongoSyncQueryClient.bucketFind()} hints the shipped index; the
 * {@link #testWithoutTheHintThePlannerConsidersOtherIndexes} counterfactual keeps the fixture
 * honest (were the extra indexes dropped from setup, the hint would be untested).
 *
 * <p>The queries under test are the exact filter, sort, and hint the service issues, obtained
 * from the package-private {@code FindIterable} builders on {@link MongoSyncQueryClient}
 * ({@code bucketDocumentQuery} for V1, {@code bucketQueryV2} and {@code bucketSamplesQueryV2}
 * for V2), with the name filters built the way the production callers build them. The span is
 * resolved through the production {@code resolveMaxBucketSpanSeconds} from {@code pvStats}
 * documents seeded through the production updater, so each test pins the chain from stored
 * statistic to planner bound.
 *
 * <p>Fixture: two PVs with one-second buckets over three hundred consecutive seconds, and a
 * seeded span of 300 s for the first PV and 7 s for the second. The window queried lies near the
 * end of that history so the two spans give visibly different scans.
 *
 * <p>The plan walker reads the unsharded explain shape (a stage tree under
 * {@code queryPlanner.winningPlan}, or under its {@code queryPlan} when the slot-based engine
 * reports); a sharded explain nests one such tree per shard and is not handled, so the sharded
 * plan shape (the customer deployment) remains unpinned.
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

    // two disjoint V2 retrieval fragments inside the window: [290, 292) and [293, 295)
    private static final TimeInterval FRAGMENT_1 = new TimeInterval(BEGIN_SECONDS, 0L, BASE_SECONDS + 292, 0L);
    private static final TimeInterval FRAGMENT_2 = new TimeInterval(BASE_SECONDS + 293, 0L, END_SECONDS, 0L);

    private static final String KEY_STAGE = "stage";
    private static final String KEY_KEY_PATTERN = "keyPattern";
    private static final String KEY_INDEX_BOUNDS = "indexBounds";
    private static final String STAGE_COLLSCAN = "COLLSCAN";
    private static final String STAGE_IXSCAN = "IXSCAN";
    private static final String STAGE_SORT = "SORT";

    /** The shipped compound index's key order, as explain renders {@code keyPattern}. */
    private static final List<String> SHIPPED_INDEX_KEYS = List.of(
            BsonConstants.BSON_KEY_PV_NAME,
            BsonConstants.BSON_KEY_BUCKET_FIRST_TIME_SECS,
            BsonConstants.BSON_KEY_BUCKET_FIRST_TIME_NANOS,
            BsonConstants.BSON_KEY_BUCKET_LAST_TIME_SECS,
            BsonConstants.BSON_KEY_BUCKET_LAST_TIME_NANOS);

    /**
     * An interval as explain renders it, e.g. {@code "[1700000283, 1700000295]"} or
     * {@code "[1700000283, inf.0]"}; group 1 is the lower end, group 2 the upper end (a number, or
     * {@code inf}), group 3 the closing bracket. Anchored on an inclusive {@code [}: an exclusive
     * {@code (} would drop the buckets starting exactly {@code span} seconds before {@code begin},
     * which can still overlap.
     */
    private static final Pattern INCLUSIVE_LOWER_BOUND =
            Pattern.compile("^\\[(-?\\d+)(?:\\.0)?, (-?\\d+|inf)(?:\\.0)?([\\]\\)])$");

    private static class TestSyncClient extends MongoSyncQueryClient {

        void insertBuckets(List<BucketDocument> buckets) {
            mongoCollectionBuckets.insertMany(buckets);
        }

        /** Seeds pvStats through the production updater, as ingestion does. */
        void recordSpan(String pvName, long spanSeconds) throws DpException {
            new PvStatsMaxSpanUpdater(mongoCollectionPvStats.withDocumentClass(Document.class))
                    .recordSpan(List.of(pvName), spanSeconds);
        }

        /** Adds an index the current code does not declare, as a long-lived archive has. */
        void createLeftoverBucketIndex(Bson keys) {
            mongoCollectionBuckets.createIndex(keys);
        }

        List<Document> listBucketIndexes() {
            return mongoCollectionBuckets.listIndexes().into(new ArrayList<>());
        }

        /** The V1 query as issued before #271: same filter and sort, no hint. */
        FindIterable<BucketDocument> unhintedBucketFind(Bson filter) {
            return mongoCollectionBuckets
                    .find(filter)
                    .sort(ascending(
                            BsonConstants.BSON_KEY_PV_NAME,
                            BsonConstants.BSON_KEY_BUCKET_FIRST_TIME_SECS,
                            BsonConstants.BSON_KEY_BUCKET_FIRST_TIME_NANOS));
        }
    }

    private static TestSyncClient client;

    @BeforeClass
    public static void setUp() throws Exception {
        MongoTestClient.prepareTestDatabase();
        client = new TestSyncClient();
        assertTrue(client.init());

        // the adversarial index set (see the class Javadoc): every one leads with pvName and is a
        // planner candidate for the bucket queries
        client.createLeftoverBucketIndex(Indexes.ascending(BsonConstants.BSON_KEY_PV_NAME));
        client.createLeftoverBucketIndex(Indexes.ascending(
                BsonConstants.BSON_KEY_PV_NAME,
                BsonConstants.BSON_KEY_BUCKET_FIRST_TIME_SECS,
                BsonConstants.BSON_KEY_BUCKET_FIRST_TIME_NANOS));
        client.createLeftoverBucketIndex(Indexes.ascending(
                BsonConstants.BSON_KEY_PV_NAME,
                BsonConstants.BSON_KEY_BUCKET_LAST_TIME_SECS,
                BsonConstants.BSON_KEY_BUCKET_LAST_TIME_NANOS,
                BsonConstants.BSON_KEY_BUCKET_FIRST_TIME_SECS,
                BsonConstants.BSON_KEY_BUCKET_FIRST_TIME_NANOS));

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

    // ---- the hint and the declaration are the same index --------------------------------------

    @Test
    public void testHintedKeyPatternIsAnIndexStartupCreates() {
        // The hint names an index by key pattern; if init() ever stopped creating that exact
        // index every bucket query would fail. Pin from the collection side that the hinted key
        // pattern exists, and from the code side that it is the five-key compound this test
        // asserts plans against.
        final BsonDocument hinted = MongoClientBase.BUCKET_QUERY_INDEX_KEYS.toBsonDocument();
        assertEquals(SHIPPED_INDEX_KEYS, new ArrayList<>(hinted.keySet()));

        final List<Document> indexes = client.listBucketIndexes();
        final boolean present = indexes.stream()
                .map(index -> index.get("key", Document.class).toBsonDocument())
                .anyMatch(hinted::equals);
        assertTrue("no bucket index with the hinted key pattern " + hinted.toJson() + " in " + indexes, present);
    }

    // ---- V1 named-PV path (queryData, queryTable by list, annotation data-block export) ---------

    @Test
    public void testNamedPvsBoundFirstTimeSecondsAtBeginMinusResolvedSpan() throws DpException {
        final List<String> pvNames = List.of(PV_1, PV_2);
        final long spanSeconds = client.resolveMaxBucketSpanSeconds(pvNames);
        assertEquals("span is the maximum over the request's PVs (plan D1)", SPAN_PV_1_SECONDS, spanSeconds);

        final Document explanation = explainV1(pvNames, spanSeconds, ExplainVerbosity.QUERY_PLANNER);
        assertShippedIndexPlan(explanation);
        assertIndexScanBoundedAt(winningPlan(explanation), BEGIN_SECONDS - SPAN_PV_1_SECONDS, pvNames);
    }

    @Test
    public void testSinglePvBoundUsesItsOwnSpan() throws DpException {
        // the per-PV point of #232: a query naming only the short-span PV gets the tighter bound,
        // not the largest span in the archive
        final List<String> pvNames = List.of(PV_2);
        final long spanSeconds = client.resolveMaxBucketSpanSeconds(pvNames);
        assertEquals(SPAN_PV_2_SECONDS, spanSeconds);

        final Document explanation = explainV1(pvNames, spanSeconds, ExplainVerbosity.QUERY_PLANNER);
        assertShippedIndexPlan(explanation);
        assertIndexScanBoundedAt(winningPlan(explanation), BEGIN_SECONDS - SPAN_PV_2_SECONDS, pvNames);
    }

    @Test
    public void testPvWithoutStatsBoundsAtBegin() throws DpException {
        // plan D6: no pvStats document contributes nothing, so the bound sits at begin itself
        final List<String> pvNames = List.of(PV_WITHOUT_STATS);
        final long spanSeconds = client.resolveMaxBucketSpanSeconds(pvNames);
        assertEquals(0L, spanSeconds);

        final Document explanation = explainV1(pvNames, spanSeconds, ExplainVerbosity.QUERY_PLANNER);
        assertShippedIndexPlan(explanation);
        assertIndexScanBoundedAt(winningPlan(explanation), BEGIN_SECONDS, pvNames);
    }

    @Test
    public void testTighterSpanExaminesFewerKeysForTheSameResult() {
        // Same PV, same window, same five buckets returned; only the span differs. The index scan
        // covers exactly [begin - span, end] on firstTime.seconds (one bucket per second in the
        // fixture): the lower bound is the span, and the upper bound (#271) is the window's end
        // second -- before it, the "firstTime < end" half of the overlap predicate was only a
        // residual filter and the scan ran on to the end of the PV's history.
        final List<String> pvNames = List.of(PV_2);

        final Document tight = explainV1(pvNames, SPAN_PV_2_SECONDS, ExplainVerbosity.EXECUTION_STATS);
        final Document wide = explainV1(pvNames, SPAN_PV_1_SECONDS, ExplainVerbosity.EXECUTION_STATS);

        assertEquals(BUCKETS_IN_WINDOW, executionStat(tight, "nReturned"));
        assertEquals(BUCKETS_IN_WINDOW, executionStat(wide, "nReturned"));

        final long tightLowerBoundSeconds = BEGIN_SECONDS - SPAN_PV_2_SECONDS;
        final long expectedTightKeys = END_SECONDS - tightLowerBoundSeconds + 1;
        assertEquals("keys examined under the PV's own span", expectedTightKeys, executionStat(tight, "totalKeysExamined"));

        // begin - 300 precedes the PV's first bucket, so the wide bound scans its history up to end
        final long expectedWideKeys = END_SECONDS - BASE_SECONDS + 1;
        assertEquals("keys examined under the archive-wide span", expectedWideKeys, executionStat(wide, "totalKeysExamined"));
        assertTrue("the upper bound must stop the scan before the PV's last bucket", expectedWideKeys < NUM_BUCKETS_PER_PV);
    }

    // ---- V1 pattern path (queryTable by pvNamePattern) -----------------------------------------

    @Test
    public void testPatternPathStaysOnTheShippedIndexWithTheSpanBound() throws DpException {
        // executeQueryTable compiles the pattern CASE_INSENSITIVE, which denies the planner a
        // prefix range on pvName -- the scan visits every PV -- but the firstTime.seconds bound
        // still applies within each PV, and the sort still streams from the index.
        final Pattern pvNamePattern = Pattern.compile("^" + PV_NAME_BASE, Pattern.CASE_INSENSITIVE);
        final long spanSeconds = client.resolveMaxBucketSpanSeconds(pvNamePattern);
        assertEquals("pattern span is the maximum over the matching pvStats ids", SPAN_PV_1_SECONDS, spanSeconds);

        final Bson pvNameFilter = Filters.regex(BsonConstants.BSON_KEY_PV_NAME, pvNamePattern);
        final Document explanation = client
                .bucketDocumentQuery(pvNameFilter, BEGIN_SECONDS, 0L, END_SECONDS, 0L, spanSeconds)
                .explain(ExplainVerbosity.EXECUTION_STATS);
        assertShippedIndexPlan(explanation);
        assertIndexScansWithin(winningPlan(explanation), BEGIN_SECONDS - SPAN_PV_1_SECONDS, END_SECONDS);
        assertEquals(2 * BUCKETS_IN_WINDOW, executionStat(explanation, "nReturned"));
    }

    // ---- V2 paths (queryBuckets unary + keyset page, queryBuckets stream, querySamples) ---------

    @Test
    public void testV2FragmentOrStaysOnTheShippedIndexWithEveryFragmentBounded() throws DpException {
        // Two disjoint fragments (#203's shape): the $or of two overlap predicates, each carrying
        // its own copy of the lower bound. No index scan may reach below the earliest fragment's
        // bound, and the sort must still stream from the index.
        final List<String> pvNames = List.of(PV_1, PV_2);
        final long spanSeconds = client.resolveMaxBucketSpanSeconds(pvNames);
        final ResolvedQuery resolvedQuery = resolvedQuery(pvNames, null, false);

        final Document explanation = client.bucketQueryV2(resolvedQuery, spanSeconds)
                .explain(ExplainVerbosity.EXECUTION_STATS);
        assertShippedIndexPlan(explanation);
        assertIndexScansWithin(winningPlan(explanation), BEGIN_SECONDS - SPAN_PV_1_SECONDS, END_SECONDS);
        // buckets 290, 291 from fragment 1 and 293, 294 from fragment 2, for each PV; bucket 292
        // overlaps neither ([292, 293) meets fragment 1's exclusive end and fragment 2's begin)
        assertEquals(2 * 4, executionStat(explanation, "nReturned"));
    }

    @Test
    public void testV2KeysetSeekPageStaysOnTheShippedIndex() throws DpException {
        // A continuation page ANDs the keyset seek $or at top level (Q3); the planner must keep
        // the shipped index and the streaming sort with that extra predicate in play.
        final List<String> pvNames = List.of(PV_1, PV_2);
        final long spanSeconds = client.resolveMaxBucketSpanSeconds(pvNames);
        final KeysetPosition pageStart = KeysetPosition.ofBucket(PV_1, BASE_SECONDS + 293, 0L);
        final ResolvedQuery resolvedQuery = resolvedQuery(pvNames, pageStart, false);

        final Document explanation = client.bucketQueryV2(resolvedQuery, spanSeconds)
                .explain(ExplainVerbosity.EXECUTION_STATS);
        assertShippedIndexPlan(explanation);
        assertIndexScansWithin(winningPlan(explanation), BEGIN_SECONDS - SPAN_PV_1_SECONDS, END_SECONDS);
        // PV_1 strictly after (293, 0): bucket 294; PV_2: all four
        assertEquals(1 + 4, executionStat(explanation, "nReturned"));
    }

    @Test
    public void testV2SamplesFragmentOrStaysOnTheShippedIndex() throws DpException {
        // executeQuerySamplesV2 builds its own fragment $or over the clamped intervals (#207)
        final List<String> pvNames = List.of(PV_1, PV_2);
        final long spanSeconds = client.resolveMaxBucketSpanSeconds(pvNames);
        final ResolvedQuery resolvedQuery = resolvedQuery(pvNames, null, true);
        final List<TimeInterval> clamped = TimeInterval.clampToWindowBegin(
                resolvedQuery.getRetrievalIntervals(), BEGIN_SECONDS, 0L);
        assertEquals(2, clamped.size());

        final Document explanation = client.bucketSamplesQueryV2(resolvedQuery, clamped, spanSeconds)
                .explain(ExplainVerbosity.EXECUTION_STATS);
        assertShippedIndexPlan(explanation);
        assertIndexScansWithin(winningPlan(explanation), BEGIN_SECONDS - SPAN_PV_1_SECONDS, END_SECONDS);
        assertEquals(2 * 4, executionStat(explanation, "nReturned"));
    }

    // ---- the counterfactual that keeps the adversarial fixture honest --------------------------

    @Test
    public void testWithoutTheHintThePlannerConsidersOtherIndexes() {
        // The same V1 filter and sort with no hint: with the leftover indexes in place the planner
        // has more than one candidate and records the losers under rejectedPlans. If this stops
        // holding, the fixture is no longer adversarial and the hinted tests above prove nothing.
        final Bson filter = Filters.and(
                Filters.in(BsonConstants.BSON_KEY_PV_NAME, List.of(PV_1, PV_2)),
                MongoQueryFilterBuilder.bucketOverlapsRangeFilter(
                        BEGIN_SECONDS, 0L, END_SECONDS, 0L, SPAN_PV_1_SECONDS));
        final Document explanation = client.unhintedBucketFind(filter).explain(ExplainVerbosity.QUERY_PLANNER);
        assertFalse("expected rejected candidate plans without the hint: " + explanation.toJson(),
                rejectedPlans(explanation).isEmpty());
    }

    // ---- helpers ---------------------------------------------------------------------------------

    /**
     * Runs explain on the exact V1 query the service issues for {@code pvNames} over the fixture
     * window, with the name filter built as {@code executeQueryData} builds it.
     */
    private static Document explainV1(List<String> pvNames, long maxBucketSpanSeconds, ExplainVerbosity verbosity) {
        final Bson pvNameFilter = Filters.in(BsonConstants.BSON_KEY_PV_NAME, pvNames);
        return client.bucketDocumentQuery(pvNameFilter, BEGIN_SECONDS, 0L, END_SECONDS, 0L, maxBucketSpanSeconds)
                .explain(verbosity);
    }

    private static ResolvedQuery resolvedQuery(List<String> pvNames, KeysetPosition pageStart, boolean samples) {
        return new ResolvedQuery(
                pvNames,
                List.of(FRAGMENT_1, FRAGMENT_2),
                100,
                pageStart,
                false,
                false,
                samples ? ResolvedQuery.ResultMode.SAMPLE : ResolvedQuery.ResultMode.BUCKET,
                false);
    }

    private static Document winningPlan(Document explanation) {
        final Document winningPlan = explanation.getEmbedded(List.of("queryPlanner", "winningPlan"), Document.class);
        assertNotNull("explain output has no queryPlanner.winningPlan: " + explanation.toJson(), winningPlan);
        return winningPlan;
    }

    private static List<Document> rejectedPlans(Document explanation) {
        final Document queryPlanner = explanation.get("queryPlanner", Document.class);
        assertNotNull("explain output has no queryPlanner: " + explanation.toJson(), queryPlanner);
        final List<Document> rejected = queryPlanner.getList("rejectedPlans", Document.class);
        return rejected == null ? List.of() : rejected;
    }

    private static long executionStat(Document explanation, String statName) {
        final Number value = explanation.getEmbedded(List.of("executionStats", statName), Number.class);
        assertNotNull("explain output has no executionStats." + statName + ": " + explanation.toJson(), value);
        return value.longValue();
    }

    /**
     * The #271 plan shape. The hint restricts the planner to one index, not to one plan: it still
     * enumerates alternatives on that index (the {@code (seconds, nanos)} {@code $or} halves
     * exploded into {@code SORT_MERGE}d scans) and ranks them, so {@code rejectedPlans} is not
     * empty -- but every candidate, winning or rejected, must scan the shipped five-key compound
     * index exactly (a prefix check would pass on the retired
     * {@code (pvName, firstTime.seconds, firstTime.nanos)}). The winning plan additionally has no
     * collection scan and no blocking {@code SORT}: the index's leading {@code (pvName, firstTime)}
     * streams the sort.
     */
    private static void assertShippedIndexPlan(Document explanation) {
        final Document winningPlan = winningPlan(explanation);
        assertFalse("winning plan has no index scan: " + winningPlan.toJson(), indexScans(winningPlan).isEmpty());
        assertEveryIndexScanOnShippedIndex(winningPlan);
        for (Document rejectedPlan : rejectedPlans(explanation)) {
            assertEveryIndexScanOnShippedIndex(rejectedPlan);
        }
    }

    private static void assertEveryIndexScanOnShippedIndex(Document plan) {
        final String planJson = plan.toJson();
        final List<Document> stages = new ArrayList<>();
        collectStages(plan, stages);
        for (Document stage : stages) {
            if (!STAGE_IXSCAN.equals(stage.getString(KEY_STAGE))) {
                continue;
            }
            final Document keyPattern = stage.get(KEY_KEY_PATTERN, Document.class);
            assertNotNull("index scan has no keyPattern: " + planJson, keyPattern);
            assertEquals("candidate plan scans an index other than the shipped compound index: " + planJson,
                    SHIPPED_INDEX_KEYS, new ArrayList<>(keyPattern.keySet()));
        }
    }

    /**
     * Collects the winning plan's index scans, asserting on the way that it contains no collection
     * scan and no blocking sort.
     */
    private static List<Document> indexScans(Document winningPlan) {
        final String planJson = winningPlan.toJson();
        final List<Document> stages = new ArrayList<>();
        collectStages(winningPlan, stages);
        assertFalse("no stages found in winning plan: " + planJson, stages.isEmpty());

        final List<Document> indexScans = new ArrayList<>();
        for (Document stage : stages) {
            final String stageName = stage.getString(KEY_STAGE);
            assertNotEquals("winning plan contains a collection scan: " + planJson, STAGE_COLLSCAN, stageName);
            assertNotEquals("winning plan contains a blocking sort: " + planJson, STAGE_SORT, stageName);
            if (STAGE_IXSCAN.equals(stageName)) {
                indexScans.add(stage);
            }
        }
        return indexScans;
    }

    /**
     * Every index scan must be on the compound index led by {@code (pvName, firstTime.seconds)},
     * with a single inclusive interval on {@code firstTime.seconds} running from
     * {@code expectedLowerBound} to the window's end second (the implied upper bound, #271), and
     * one point interval on {@code pvName} per named PV.
     */
    private static void assertIndexScanBoundedAt(Document winningPlan, long expectedLowerBound, List<String> pvNames) {
        final String planJson = winningPlan.toJson();
        final List<Document> indexScans = indexScans(winningPlan);
        assertFalse("winning plan has no index scan: " + planJson, indexScans.isEmpty());

        for (Document indexScan : indexScans) {
            final Document indexBounds = indexBounds(indexScan, planJson);

            final List<String> secondsIntervals =
                    indexBounds.getList(BsonConstants.BSON_KEY_BUCKET_FIRST_TIME_SECS, String.class);
            assertNotNull("no bounds on firstTime.seconds: " + planJson, secondsIntervals);
            assertEquals("expected one interval on firstTime.seconds: " + secondsIntervals, 1, secondsIntervals.size());
            assertEquals("firstTime.seconds interval " + secondsIntervals.get(0),
                    "[" + expectedLowerBound + ", " + END_SECONDS + "]", normalizedInterval(secondsIntervals.get(0)));

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

    /**
     * For the shapes where the planner may split the scan into several intervals (a fragment
     * {@code $or}, a keyset seek, a pattern name filter): every {@code firstTime.seconds} interval
     * of every index scan lies within {@code [expectedLowerBound, expectedUpperBound]}, and the
     * scans together start exactly at the lower bound and end exactly at the upper -- no scan
     * reaches outside the window's bounds, and neither bound is merely a filter.
     */
    private static void assertIndexScansWithin(Document winningPlan, long expectedLowerBound, long expectedUpperBound) {
        final String planJson = winningPlan.toJson();
        final List<Document> indexScans = indexScans(winningPlan);
        assertFalse("winning plan has no index scan: " + planJson, indexScans.isEmpty());

        long lowestLowerBound = Long.MAX_VALUE;
        long highestUpperBound = Long.MIN_VALUE;
        for (Document indexScan : indexScans) {
            final Document indexBounds = indexBounds(indexScan, planJson);
            final List<String> secondsIntervals =
                    indexBounds.getList(BsonConstants.BSON_KEY_BUCKET_FIRST_TIME_SECS, String.class);
            assertNotNull("no bounds on firstTime.seconds: " + planJson, secondsIntervals);
            assertFalse("empty bounds on firstTime.seconds: " + planJson, secondsIntervals.isEmpty());
            for (String interval : secondsIntervals) {
                final Matcher matcher = intervalMatcher(interval);
                final long lowerBound = Long.parseLong(matcher.group(1));
                assertTrue("index scan reaches below the span bound " + expectedLowerBound + ": " + interval,
                        lowerBound >= expectedLowerBound);
                lowestLowerBound = Math.min(lowestLowerBound, lowerBound);

                assertNotEquals("index scan has no upper bound: " + interval, "inf", matcher.group(2));
                final long upperBound = Long.parseLong(matcher.group(2));
                assertTrue("index scan reaches above the window end " + expectedUpperBound + ": " + interval,
                        upperBound <= expectedUpperBound);
                highestUpperBound = Math.max(highestUpperBound, upperBound);
            }
        }
        assertEquals("no index scan starts at the span bound: " + planJson, expectedLowerBound, lowestLowerBound);
        assertEquals("no index scan ends at the window end: " + planJson, expectedUpperBound, highestUpperBound);
    }

    private static Document indexBounds(Document indexScan, String planJson) {
        final Document indexBounds = indexScan.get(KEY_INDEX_BOUNDS, Document.class);
        assertNotNull("index scan has no indexBounds: " + planJson, indexBounds);
        return indexBounds;
    }

    private static Matcher intervalMatcher(String interval) {
        final Matcher matcher = INCLUSIVE_LOWER_BOUND.matcher(interval);
        assertTrue("not an inclusive numeric interval: " + interval, matcher.find());
        return matcher;
    }

    /** The interval with explain's {@code .0} decorations stripped, e.g. {@code "[1, 2]"}. */
    private static String normalizedInterval(String interval) {
        final Matcher matcher = intervalMatcher(interval);
        return "[" + matcher.group(1) + ", " + matcher.group(2) + matcher.group(3);
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
