package com.ospreydcs.dp.service.common.mongo.migration;

import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.ospreydcs.dp.service.common.exception.DpException;
import com.ospreydcs.dp.service.common.mongo.migration.SchemaVersionMarker.Claim;
import com.ospreydcs.dp.service.common.mongo.migration.SchemaVersionMarker.MarkerState;
import org.bson.Document;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Covers the lease on the migration claim at the marker (issue #292): what a claim writes, the
 * compare-and-swap that takes one over, and the fencing of every write a holder makes.
 *
 * <p>The fencing is the part with a silent failure. A holder displaced while it was stalled still
 * believes the claim is its own; an unfenced write from it would overwrite the new holder's
 * version or release the new holder's claim, and neither would raise an error anywhere.
 */
public class SchemaVersionMarkerLeaseTest {

    private static final int CAP = SchemaMigrationRunner.MAX_TAKEOVERS_PER_VERSION;

    private LeaseTestSupport.TestClient testClient;
    private MongoDatabase database;
    private MongoCollection<Document> markerCollection;

    @Before
    public void setUp() throws DpException {
        testClient = new LeaseTestSupport.TestClient();
        testClient.init();
        database = testClient.database();
        markerCollection = SchemaVersionMarker.collection(database);
        markerCollection.deleteMany(new Document());
        SchemaVersionMarker.createUnmanagedMarker(database);
    }

    @After
    public void tearDown() {
        markerCollection.deleteMany(new Document());
        testClient.fini();
    }

    private Claim claim(String host) throws DpException {
        final Claim claim = SchemaVersionMarker.claimForMigration(database, 0, host);
        assertNotNull(claim);
        return claim;
    }

    /** A write a holder makes, to run each fencing case against all four. */
    private interface HolderWrite {
        void run(Claim claim) throws DpException;
    }

    private List<HolderWrite> holderWrites() {
        return List.of(
                claim -> SchemaVersionMarker.recordApplied(database, claim, 1, "change"),
                claim -> SchemaVersionMarker.releaseClaim(database, claim),
                claim -> SchemaVersionMarker.heartbeat(database, claim),
                claim -> SchemaVersionMarker.markFailed(database, claim, 1, "failure"));
    }

    // ------------------------------------------------------------------
    // what a claim writes
    // ------------------------------------------------------------------

    @Test
    public void testClaimWritesTheLeaseFields() throws DpException {

        final Claim claim = claim("holder");

        final MarkerState state = SchemaVersionMarker.read(database);
        assertEquals(claim.token(), state.claimToken());
        assertEquals(0, claim.fromVersion());
        assertEquals(0L, state.heartbeatSeq());
        assertNotNull(state.migratingHeartbeat());
        assertEquals(0, state.takeoverCount());
        assertFalse(state.migrationFailed());
    }

    @Test
    public void testHeartbeatAdvancesTheSequence() throws DpException {

        final Claim claim = claim("holder");
        SchemaVersionMarker.heartbeat(database, claim);
        SchemaVersionMarker.heartbeat(database, claim);

        assertEquals(2L, SchemaVersionMarker.read(database).heartbeatSeq());
    }

    @Test
    public void testReleaseRemovesEveryLeaseField() throws DpException {

        final Claim claim = claim("holder");
        SchemaVersionMarker.heartbeat(database, claim);
        SchemaVersionMarker.releaseClaim(database, claim);

        final Document marker = LeaseTestSupport.rawMarker(database);
        assertEquals(false, marker.get("migrating"));
        for (String field : List.of(
                "migratingSince", "migratingHost", "claimToken", "heartbeatSeq",
                "migratingHeartbeat", "takeoverCount")) {
            assertFalse(field + " must not outlive the claim", marker.containsKey(field));
        }
    }

    @Test
    public void testFreshClaimRemovesAFailureFlagLeftByTheOldManualClear() throws DpException {

        // The runbook clear shipped with 1.16.0 unsets migratingSince and migratingHost only. An
        // operator following it after a failed migration leaves the flag on an unclaimed marker.
        LeaseTestSupport.setOnMarker(database, new Document("migrating", false)
                .append("migrationFailed", true)
                .append("migrationFailedVersion", 1)
                .append("migrationFailedMessage", "an old failure"));

        claim("holder");

        final Document marker = LeaseTestSupport.rawMarker(database);
        for (String field : List.of(
                "migrationFailed", "migrationFailedVersion", "migrationFailedMessage")) {
            assertFalse(
                    field + " describes a failure an operator has dealt with; kept, every waiter "
                            + "would read this healthy claim as failed",
                    marker.containsKey(field));
        }
        assertFalse(SchemaVersionMarker.read(database).migrationFailed());
    }

    @Test
    public void testMarkFailedStoresARedactedTruncatedMessage() throws DpException {

        final Claim claim = claim("holder");
        final String message = "cannot reach mongodb://dpuser:hunter2@db.example:27017/dp "
                + "x".repeat(2000);

        SchemaVersionMarker.markFailed(database, claim, 4, message);

        final MarkerState state = SchemaVersionMarker.read(database);
        assertTrue(state.migrationFailed());
        assertEquals(Integer.valueOf(4), state.migrationFailedVersion());
        assertFalse(
                "the stored message is shown to operators and waiters: " + state.migrationFailedMessage(),
                state.migrationFailedMessage().contains("hunter2"));
        assertTrue(state.migrationFailedMessage().contains("db.example"));
        assertEquals(
                SchemaVersionMarker.MIGRATION_FAILED_MESSAGE_MAX_LENGTH,
                state.migrationFailedMessage().length());
        assertTrue("a failed claim stays held", state.migrating());
    }

    // ------------------------------------------------------------------
    // takeover
    // ------------------------------------------------------------------

    @Test
    public void testTakeOverSwapsTheTokenAndCountsIt() throws DpException {

        final Claim old = claim("old-holder");
        SchemaVersionMarker.heartbeat(database, old);
        SchemaVersionMarker.recordApplied(database, old, 1, "first");

        final Claim taken =
                SchemaVersionMarker.takeOverClaim(database, old.token(), 1L, "new-holder", CAP);

        assertNotNull(taken);
        assertNotEquals(old.token(), taken.token());
        assertEquals(
                "the new holder resumes from the version the marker recorded", 1, taken.fromVersion());

        final MarkerState state = SchemaVersionMarker.read(database);
        assertTrue(state.migrating());
        assertEquals(taken.token(), state.claimToken());
        assertEquals("new-holder", state.migratingHost());
        assertEquals(0L, state.heartbeatSeq());
        assertEquals(1, state.takeoverCount());
    }

    @Test
    public void testTakeOverIsRefusedWhenTheHeartbeatHasMoved() throws DpException {

        final Claim holder = claim("live-holder");
        // the waiter watched sequence 0; the holder, alive after all, beats before the takeover
        SchemaVersionMarker.heartbeat(database, holder);

        assertNull(
                "a holder whose heartbeat moved is alive and must keep its claim",
                SchemaVersionMarker.takeOverClaim(database, holder.token(), 0L, "waiter", CAP));
        assertEquals(holder.token(), SchemaVersionMarker.read(database).claimToken());
    }

    @Test
    public void testTakeOverIsRefusedForAFailedClaim() throws DpException {

        final Claim holder = claim("failed-holder");
        SchemaVersionMarker.markFailed(database, holder, 1, "it threw");

        assertNull(
                "a migration that failed would fail again; its claim waits for an operator",
                SchemaVersionMarker.takeOverClaim(database, holder.token(), 0L, "waiter", CAP));
        assertEquals(holder.token(), SchemaVersionMarker.read(database).claimToken());
    }

    @Test
    public void testTakeOverIsRefusedAtTheCapAndTheCountResetsWithTheVersion() throws DpException {

        Claim current = claim("holder-0");
        for (int i = 1; i <= CAP; i++) {
            current = SchemaVersionMarker.takeOverClaim(
                    database, current.token(), 0L, "holder-" + i, CAP);
            assertNotNull("takeover " + i + " is within the cap", current);
        }
        assertEquals(CAP, SchemaVersionMarker.read(database).takeoverCount());

        assertNull(
                "without the cap a migration that kills its own process is retried forever",
                SchemaVersionMarker.takeOverClaim(database, current.token(), 0L, "one-more", CAP));

        // an advanced version is progress: the cap is per version
        SchemaVersionMarker.recordApplied(database, current, 1, "progress");
        assertEquals(0, SchemaVersionMarker.read(database).takeoverCount());
        assertNotNull(
                SchemaVersionMarker.takeOverClaim(database, current.token(), 0L, "after", CAP));
    }

    @Test
    public void testOnlyOneOfSeveralConcurrentTakeoversWins() throws Exception {

        final Claim dead = claim("dead-holder");

        final int waiters = 8;
        final ExecutorService executor = Executors.newFixedThreadPool(waiters);
        try {
            final List<Future<Claim>> attempts = new ArrayList<>();
            for (int i = 0; i < waiters; i++) {
                final String host = "waiter-" + i;
                attempts.add(executor.submit((Callable<Claim>) () ->
                        SchemaVersionMarker.takeOverClaim(database, dead.token(), 0L, host, CAP)));
            }
            int won = 0;
            for (Future<Claim> attempt : attempts) {
                if (attempt.get() != null) {
                    won++;
                }
            }
            assertEquals("two winners would be two concurrent migrators", 1, won);
        } finally {
            executor.shutdownNow();
        }
        assertEquals(1, SchemaVersionMarker.read(database).takeoverCount());
    }

    // ------------------------------------------------------------------
    // fencing: a write that matches nothing says which of three things happened
    // ------------------------------------------------------------------

    @Test
    public void testDisplacedHoldersWritesAreFencedAndReportTheTakeover() throws DpException {

        for (HolderWrite write : holderWrites()) {
            markerCollection.deleteMany(new Document());
            SchemaVersionMarker.createUnmanagedMarker(database);

            final Claim displaced = claim("stalled-holder");
            assertNotNull(SchemaVersionMarker.takeOverClaim(
                    database, displaced.token(), 0L, "new-holder", CAP));
            final Document before = LeaseTestSupport.rawMarker(database);

            try {
                write.run(displaced);
                fail("a displaced holder's marker write must not succeed");
            } catch (ClaimLostException ex) {
                assertTrue(
                        "message should say another process took over: " + ex.getMessage(),
                        ex.getMessage().contains("took the schema migration over")
                                && ex.getMessage().contains("new-holder"));
                assertFalse(
                        "a displaced holder has left no claim in place: " + ex.getMessage(),
                        ex.getMessage().contains("left in place"));
            }
            assertEquals(
                    "the new holder's marker must be untouched",
                    before, LeaseTestSupport.rawMarker(database));
        }
    }

    @Test
    public void testWritesAfterAManualClearReportTheClearedClaim() throws DpException {

        for (HolderWrite write : holderWrites()) {
            markerCollection.deleteMany(new Document());
            SchemaVersionMarker.createUnmanagedMarker(database);

            final Claim holder = claim("live-holder");
            // the runbook's manual clear, run against a holder that was alive
            markerCollection.updateOne(
                    new Document("_id", SchemaVersionMarker.MARKER_ID),
                    new Document("$set", new Document("migrating", false))
                            .append("$unset", new Document("migratingSince", "")
                                    .append("migratingHost", "").append("claimToken", "")
                                    .append("heartbeatSeq", "").append("migratingHeartbeat", "")
                                    .append("takeoverCount", "")));
            final Document before = LeaseTestSupport.rawMarker(database);

            try {
                write.run(holder);
                fail("a holder whose claim was cleared must stop at its next marker write");
            } catch (ClaimLostException ex) {
                assertTrue(
                        "message should say the claim was cleared: " + ex.getMessage(),
                        ex.getMessage().contains("cleared or released"));
            }
            assertEquals(before, LeaseTestSupport.rawMarker(database));
        }
    }

    @Test
    public void testWritesAfterThe1160ManualClearReportTheClearedClaim() throws DpException {

        // The clear in the 1.16.0 runbook unsets only migratingSince and migratingHost, so the
        // holder's token stays on the marker. A fence on the token alone would let a holder cleared
        // that way go on recording versions and beating against a marker that reads unclaimed.
        for (HolderWrite write : holderWrites()) {
            markerCollection.deleteMany(new Document());
            SchemaVersionMarker.createUnmanagedMarker(database);

            final Claim holder = claim("live-holder");
            markerCollection.updateOne(
                    new Document("_id", SchemaVersionMarker.MARKER_ID),
                    new Document("$set", new Document("migrating", false))
                            .append("$unset", new Document("migratingSince", "")
                                    .append("migratingHost", "")));
            final Document before = LeaseTestSupport.rawMarker(database);
            assertEquals(holder.token(), before.getString("claimToken"));

            try {
                write.run(holder);
                fail("a holder whose claim was cleared must stop at its next marker write");
            } catch (ClaimLostException ex) {
                assertTrue(
                        "message should say the claim was cleared: " + ex.getMessage(),
                        ex.getMessage().contains("cleared or released"));
            }
            assertEquals(before, LeaseTestSupport.rawMarker(database));
        }
    }

    @Test
    public void testWritesAgainstAMissingMarkerReportItGone() throws DpException {

        for (HolderWrite write : holderWrites()) {
            markerCollection.deleteMany(new Document());
            SchemaVersionMarker.createUnmanagedMarker(database);

            final Claim holder = claim("holder");
            markerCollection.deleteMany(new Document());

            try {
                write.run(holder);
                fail("a write against an absent marker must be reported");
            } catch (ClaimLostException ex) {
                fail("a missing marker is not a lost claim: " + ex.getMessage());
            } catch (DpException ex) {
                assertTrue(
                        "message should say the marker is gone: " + ex.getMessage(),
                        ex.getMessage().contains("marker document is gone"));
            }
        }
    }
}
