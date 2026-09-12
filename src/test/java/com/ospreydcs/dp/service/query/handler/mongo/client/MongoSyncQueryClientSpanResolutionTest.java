package com.ospreydcs.dp.service.query.handler.mongo.client;

import com.ospreydcs.dp.service.common.bson.pvstats.PvStatsDocument;
import com.ospreydcs.dp.service.common.exception.DpException;
import com.ospreydcs.dp.service.common.mongo.MongoTestClient;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.List;
import java.util.regex.Pattern;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Covers {@code MongoSyncQueryClient.resolveMaxBucketSpanSeconds} against a real server (#232).
 *
 * <p>The cases that matter here are the ones that decide how much of a PV's history the bucket
 * overlap filter scans, and every wrong answer is silent: a bound that is too tight drops
 * overlapping buckets from the result rather than reporting an error.
 */
public class MongoSyncQueryClientSpanResolutionTest {

    private static class TestClient extends MongoSyncQueryClient {
        void storeSpan(String pvName, long spanSeconds) {
            final PvStatsDocument document = new PvStatsDocument();
            document.setPvName(pvName);
            document.setMaxBucketSpanSeconds(spanSeconds);
            mongoCollectionPvStats.insertOne(document);
        }
    }

    private static TestClient client;

    @BeforeClass
    public static void setUp() {
        MongoTestClient.prepareTestDatabase();
        client = new TestClient();
        assertTrue(client.init());
        client.storeSpan("spanres_short", 7L);
        client.storeSpan("spanres_long", 300L);
        // No writing path can store a negative: ingestion only ever issues $max with a non-negative
        // span, and the v5 seed filters $gte 0. Written directly to model hand-editing or corruption.
        client.storeSpan("spanres_negative", -5L);
    }

    @AfterClass
    public static void tearDown() {
        if (client != null) {
            client.fini();
        }
    }

    @Test
    public void testResolvesMaximumOverNamedPvs() throws DpException {
        assertEquals(300L, client.resolveMaxBucketSpanSeconds(
                List.of("spanres_short", "spanres_long")));
        assertEquals(7L, client.resolveMaxBucketSpanSeconds(List.of("spanres_short")));
    }

    /** A PV with no document contributes nothing, so the bound is begin itself (plan D6). */
    @Test
    public void testUnknownPvResolvesToZero() throws DpException {
        assertEquals(0L, client.resolveMaxBucketSpanSeconds(List.of("spanres_absent")));
        assertEquals(0L, client.resolveMaxBucketSpanSeconds(List.of()));
    }

    /**
     * A corrupt negative stored value is clamped to 0 and logged, not raised as a query error.
     * Rejecting instead would fail every query naming the PV — and, since the bound is a maximum
     * over the request's PVs, every multi-PV query that happens to include it. Clamping narrows
     * results for that one PV to the same bound a PV with no document gets, which is the outcome
     * plan D10 already chose at write time by refusing to seed negatives.
     */
    @Test
    public void testNegativeStoredSpanIsClampedNotRejected() throws DpException {
        assertEquals(0L, client.resolveMaxBucketSpanSeconds(List.of("spanres_negative")));
    }

    /** A healthy PV in the same request is unaffected by a corrupt peer. */
    @Test
    public void testNegativeStoredSpanDoesNotSuppressAHealthyPeer() throws DpException {
        assertEquals(300L, client.resolveMaxBucketSpanSeconds(
                List.of("spanres_negative", "spanres_long")));
    }

    /** The pattern variant matches the same regex against pvStats ids (plan D11). */
    @Test
    public void testPatternVariantResolvesOverMatchingIds() throws DpException {
        assertEquals(300L, client.resolveMaxBucketSpanSeconds(
                Pattern.compile("^spanres_(short|long)$")));
        assertEquals(0L, client.resolveMaxBucketSpanSeconds(Pattern.compile("^spanres_nomatch$")));
    }
}
