package com.ospreydcs.dp.service.query.benchmark;

import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Pins the agreement between what the loader writes and what the V1 clients wait for (issue #275).
 *
 * <p>The V1 clients terminate on a bucket count rather than on {@code onCompleted()}, so
 * {@link QueryBenchmarkBase.QueryTaskParams#expectedBucketCount()} must never exceed what
 * {@link QueryBenchmarkBase.LoadParams#bucketsPerPv()} loaded: one bucket too many and every task
 * hangs to its latch timeout and reports a 0.0 rate, which is the failure this ticket removed. The
 * two were a ceiling against a floor, so they disagreed for every history that did not divide
 * evenly by the bucket length -- invisible at the default of one-second buckets.
 */
public class QueryBenchmarkFixtureShapeTest {

    /** The query window is the last 60 s of the history, as {@code LoadMarker} computes it. */
    private static final int SCENARIO_SECONDS = 60;

    private static QueryBenchmarkBase.LoadParams load(long historySeconds, int secondsPerBucket) {
        return new QueryBenchmarkBase.LoadParams(
                3, 10, secondsPerBucket, historySeconds, 0, 0, false, false);
    }

    private static QueryBenchmarkBase.QueryTaskParams query(
            long historySeconds, int secondsPerBucket, int numPvs, int longSpanPvs) {
        final List<String> names = new ArrayList<>();
        for (int i = 1; i <= numPvs; i++) {
            names.add("testpv_" + i);
        }
        for (int i = 1; i <= longSpanPvs; i++) {
            names.add(QueryBenchmarkBase.LONG_SPAN_PV_BASE + i);
        }
        final int windowSeconds = (int) Math.min(SCENARIO_SECONDS, historySeconds);
        return new QueryBenchmarkBase.QueryTaskParams(
                1, names, 0L, windowSeconds, longSpanPvs, secondsPerBucket);
    }

    /**
     * Copilot's example on PR #281: 60 s of history at 7 s per bucket loaded 8 buckets per PV
     * (floor) while each V1 client waited for 9 (ceiling), so every task timed out.
     */
    @Test
    public void testUnevenHistoryLoadsEveryBucketTheClientsWaitFor() {
        final QueryBenchmarkBase.LoadParams loadParams = load(60, 7);
        assertEquals("the trailing partial period is a bucket", 9, loadParams.bucketsPerPv());
        assertEquals("the client must not wait for a bucket that was never loaded",
                9, query(60, 7, 1, 0).expectedBucketCount());
    }

    /** The short final bucket carries the remainder, so the fixture covers the whole history. */
    @Test
    public void testFinalShortBucketCoversTheRemainderOfTheHistory() {
        final QueryBenchmarkBase.LoadParams loadParams = load(60, 7);
        long covered = 0;
        for (int i = 0; i < loadParams.bucketsPerPv(); i++) {
            final int seconds = loadParams.secondsInBucket(i);
            assertTrue("no bucket may be empty or negative", seconds >= 1);
            assertTrue("no bucket may exceed the configured length", seconds <= 7);
            covered += seconds;
        }
        assertEquals("the loaded buckets must cover the history exactly", 60, covered);
        assertEquals("only the last bucket is short", 4, loadParams.secondsInBucket(8));
    }

    /**
     * The general invariant, over the shapes the loader accepts: the clients may never wait for
     * more buckets than the loader wrote.
     */
    @Test
    public void testClientExpectationNeverExceedsWhatWasLoaded() {
        final int[] bucketLengths = {1, 2, 3, 7, 13, 60, 90};
        final long[] histories = {60, 61, 100, 3600, 86400};
        for (int secondsPerBucket : bucketLengths) {
            for (long historySeconds : histories) {
                final QueryBenchmarkBase.LoadParams loadParams = load(historySeconds, secondsPerBucket);
                final int loaded = loadParams.bucketsPerPv();
                final int expectedPerPv =
                        query(historySeconds, secondsPerBucket, 1, 0).expectedBucketCount();
                assertTrue(
                        "history " + historySeconds + "s at " + secondsPerBucket
                                + "s/bucket: clients wait for " + expectedPerPv
                                + " but the loader wrote " + loaded,
                        expectedPerPv <= loaded);

                long covered = 0;
                for (int i = 0; i < loaded; i++) {
                    covered += loadParams.secondsInBucket(i);
                }
                assertEquals("history " + historySeconds + "s at " + secondsPerBucket
                        + "s/bucket must be covered exactly", historySeconds, covered);
            }
        }
    }

    /** A long-span PV holds one bucket regardless of the fixture's bucket length. */
    @Test
    public void testLongSpanPvsContributeOneBucketEach() {
        final QueryBenchmarkBase.QueryTaskParams params = query(60, 7, 2, 3);
        // 2 regular PVs x ceil(60/7)=9 buckets, plus one bucket per long-span PV
        assertEquals(2 * 9 + 3, params.expectedBucketCount());
    }
}
