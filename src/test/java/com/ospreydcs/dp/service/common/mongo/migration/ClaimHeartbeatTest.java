package com.ospreydcs.dp.service.common.mongo.migration;

import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.ospreydcs.dp.service.common.exception.DpException;
import com.ospreydcs.dp.service.common.mongo.migration.SchemaVersionMarker.Claim;
import org.bson.Document;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/**
 * Covers the heartbeat thread of a held migration claim (issue #292). Its two failures are silent
 * and opposite: a heartbeat that dies makes a live migrator look dead, and one that cannot be
 * stopped keeps a dead claim fresh.
 */
public class ClaimHeartbeatTest {

    private static final long INTERVAL_MILLIS = 20L;

    private LeaseTestSupport.TestClient testClient;
    private MongoDatabase database;
    private MongoCollection<Document> markerCollection;
    private Claim claim;

    @Before
    public void setUp() throws DpException {
        testClient = new LeaseTestSupport.TestClient();
        testClient.init();
        database = testClient.database();
        markerCollection = SchemaVersionMarker.collection(database);
        markerCollection.deleteMany(new Document());
        SchemaVersionMarker.createUnmanagedMarker(database);
        claim = SchemaVersionMarker.claimForMigration(database, 0, "holder");
        assertNotNull(claim);
    }

    @After
    public void tearDown() {
        markerCollection.deleteMany(new Document());
        testClient.fini();
    }

    private long sequence() throws DpException {
        return SchemaVersionMarker.read(database).heartbeatSeq();
    }

    private void awaitSequenceAbove(long value) throws Exception {
        final long deadline = System.currentTimeMillis() + 10_000L;
        while (sequence() <= value) {
            assertTrue("heartbeat did not advance", System.currentTimeMillis() < deadline);
            Thread.sleep(10L);
        }
    }

    @Test
    public void testBeatsUntilStoppedAndNotAfter() throws Exception {

        final ClaimHeartbeat heartbeat = new ClaimHeartbeat(database, claim, INTERVAL_MILLIS);
        heartbeat.start();
        awaitSequenceAbove(2L);

        heartbeat.stop();
        heartbeat.stop();  // idempotent
        final long afterStop = sequence();
        Thread.sleep(10 * INTERVAL_MILLIS);

        assertEquals(
                "a heartbeat that outlives stop() keeps a dead claim fresh", afterStop, sequence());
        assertFalse(heartbeat.isClaimLost());
    }

    @Test
    public void testAFailingBeatDoesNotEndTheHeartbeat() throws Exception {

        // A beat that throws, an unchecked exception and then an Error, must not end the thread:
        // the claim would read as abandoned while its migration is still running.
        final AtomicInteger beats = new AtomicInteger();
        final ClaimHeartbeat heartbeat = new ClaimHeartbeat(database, claim, INTERVAL_MILLIS) {
            @Override
            void beat() throws Exception {
                switch (beats.incrementAndGet()) {
                    case 1 -> throw new com.mongodb.MongoException("simulated connection failure");
                    case 2 -> throw new IllegalStateException("simulated");
                    case 3 -> throw new StackOverflowError("simulated");
                    default -> super.beat();
                }
            }
        };
        heartbeat.start();
        try {
            awaitSequenceAbove(1L);
            assertTrue(beats.get() > 3);
            assertFalse(heartbeat.isClaimLost());
        } finally {
            heartbeat.stop();
        }
    }

    @Test
    public void testAFencedBeatReportsTheClaimLostAndStops() throws Exception {

        final ClaimHeartbeat heartbeat = new ClaimHeartbeat(database, claim, INTERVAL_MILLIS);
        heartbeat.start();
        try {
            awaitSequenceAbove(0L);

            // another process takes the claim over between two beats
            Claim taken = null;
            while (taken == null) {
                taken = SchemaVersionMarker.takeOverClaim(
                        database, claim.token(), sequence(), "new-holder",
                        SchemaMigrationRunner.MAX_TAKEOVERS_PER_VERSION);
            }

            final long deadline = System.currentTimeMillis() + 10_000L;
            while (!heartbeat.isClaimLost()) {
                assertTrue("lost claim was not noticed", System.currentTimeMillis() < deadline);
                Thread.sleep(10L);
            }
            Thread.sleep(10 * INTERVAL_MILLIS);
            assertEquals(
                    "the displaced holder must not advance the new holder's heartbeat",
                    0L, sequence());
            assertEquals(taken.token(), SchemaVersionMarker.read(database).claimToken());
        } finally {
            heartbeat.stop();
        }
    }
}
