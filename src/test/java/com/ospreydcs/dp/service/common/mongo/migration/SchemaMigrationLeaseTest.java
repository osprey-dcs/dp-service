package com.ospreydcs.dp.service.common.mongo.migration;

import com.mongodb.MongoException;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.UpdateOptions;
import com.ospreydcs.dp.service.common.exception.DpException;
import com.ospreydcs.dp.service.common.mongo.MongoClientBase;
import com.ospreydcs.dp.service.common.mongo.migration.SchemaVersionMarker.Claim;
import com.ospreydcs.dp.service.common.mongo.migration.SchemaVersionMarker.MarkerState;
import com.ospreydcs.dp.service.common.server.InitializationStatus;
import org.bson.Document;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Covers what a starting process does about a migration claim it does not hold (issue #292): wait
 * for a live holder, take over from one that has gone away, and refuse in each of the three cases
 * that bound the takeover (a failed migration, an operation still running on the server, the cap).
 *
 * <p>Every wrong decision here is silent. Taking over from a live holder runs two migrators at
 * once; taking over a failed migration re-runs it without end; not taking over at all is the stuck
 * claim this ticket was filed for.
 */
public class SchemaMigrationLeaseTest {

    private static final String SCRATCH_COLLECTION = "schemaMigrationLeaseTestScratch";

    private static final long POLL_MILLIS = 50L;

    // Short enough for a dead holder to be detected quickly.
    private static final long STALE_AFTER_MILLIS = 400L;

    // For tests with a live holder: far above the heartbeat interval, so a pause on a loaded
    // machine does not read as a dead holder.
    private static final long LIVE_HEARTBEAT_MILLIS = 100L;
    private static final long LIVE_STALE_AFTER_MILLIS = 3_000L;

    private static final long LONG_WAIT_MILLIS = 60_000L;

    // For tests about the deadline itself. Above STALE_AFTER_MILLIS, as the runner requires.
    private static final long SHORT_WAIT_MILLIS = 1_500L;

    private LeaseTestSupport.TestClient testClient;
    private MongoDatabase database;
    private MongoDatabase adminDatabase;
    private MongoCollection<Document> markerCollection;
    private ExecutorService executor;

    /** A runner with test timings; the deadline is long unless a test is about the deadline. */
    private static class LeaseRunner extends SchemaMigrationRunner {

        LeaseRunner(
                MongoDatabase database, MongoDatabase adminDatabase, List<Migration> migrations,
                int targetVersion, long claimWaitTimeoutMillis, long staleAfterMillis,
                long orphanCheckIntervalMillis) {
            super(database, adminDatabase, migrations, targetVersion, claimWaitTimeoutMillis,
                    POLL_MILLIS,
                    new LeaseTimings(staleAfterMillis / 8, staleAfterMillis, orphanCheckIntervalMillis));
        }

        LeaseRunner(
                MongoDatabase database, MongoDatabase adminDatabase, List<Migration> migrations,
                int targetVersion, LeaseTimings timings, long claimWaitTimeoutMillis) {
            super(database, adminDatabase, migrations, targetVersion, claimWaitTimeoutMillis,
                    POLL_MILLIS, timings);
        }
    }

    private LeaseRunner runner(List<Migration> migrations, int targetVersion) {
        return runner(database, migrations, targetVersion);
    }

    private LeaseRunner runner(MongoDatabase database, List<Migration> migrations, int targetVersion) {
        return new LeaseRunner(
                database, adminDatabase, migrations, targetVersion,
                LONG_WAIT_MILLIS, STALE_AFTER_MILLIS, 200L);
    }

    /** Counts its runs, and runs an optional action, given the marker state it ran under. */
    private class TestMigration implements Migration {

        final int version;
        final AtomicInteger applyCount = new AtomicInteger();
        volatile MarkerState stateWhenApplied;
        volatile String commentWhenApplied;
        volatile InitializationStatus.Snapshot statusWhenApplied;
        private final Callable<Void> action;

        TestMigration(int version) {
            this(version, null);
        }

        TestMigration(int version, Callable<Void> action) {
            this.version = version;
            this.action = action;
        }

        @Override
        public int version() {
            return version;
        }

        @Override
        public String description() {
            return "test migration v" + version;
        }

        @Override
        public void apply(MongoDatabase database, MigrationContext context) throws DpException {
            applyCount.incrementAndGet();
            stateWhenApplied = SchemaVersionMarker.read(SchemaMigrationLeaseTest.this.database);
            commentWhenApplied = context.operationComment();
            statusWhenApplied = InitializationStatus.current();
            if (action != null) {
                try {
                    action.call();
                } catch (DpException | RuntimeException | Error ex) {
                    throw ex;
                } catch (Exception ex) {
                    throw new IllegalStateException(ex);
                }
            }
        }
    }

    @Before
    public void setUp() throws DpException {
        testClient = new LeaseTestSupport.TestClient();
        testClient.init();
        database = testClient.database();
        adminDatabase = testClient.adminDatabase();
        markerCollection = SchemaVersionMarker.collection(database);
        executor = Executors.newCachedThreadPool();
        InitializationStatus.resetForTest();
        clearAll();
        // existing data and no marker: a database to be migrated from version 0
        database.getCollection(MongoClientBase.COLLECTION_NAME_ANNOTATIONS)
                .insertOne(new Document("name", "seeded"));
        SchemaVersionMarker.createUnmanagedMarker(database);
    }

    @After
    public void tearDown() throws InterruptedException {
        executor.shutdownNow();
        executor.awaitTermination(10, TimeUnit.SECONDS);
        clearAll();
        testClient.fini();
        InitializationStatus.resetForTest();
    }

    private void clearAll() {
        markerCollection.deleteMany(new Document());
        database.getCollection(SCRATCH_COLLECTION).drop();
        for (String name : SchemaMigrationRunner.MANAGED_COLLECTION_NAMES) {
            database.getCollection(name).deleteMany(new Document());
        }
    }

    /** A claim whose holder has gone away: written, and never heartbeaten. */
    private Claim abandonedClaim(String host) throws DpException {
        final Claim claim = SchemaVersionMarker.claimForMigration(database, 0, host);
        assertNotNull(claim);
        return claim;
    }

    private Future<Void> runInBackground(SchemaMigrationRunner runner) {
        return executor.submit(() -> {
            runner.run();
            return null;
        });
    }

    private long heartbeatSeq() throws DpException {
        return SchemaVersionMarker.read(database).heartbeatSeq();
    }

    private void assertHeartbeatHasStopped() throws Exception {
        final long before = heartbeatSeq();
        Thread.sleep(STALE_AFTER_MILLIS);
        assertEquals(
                "a heartbeat that outlives its migration keeps a dead claim fresh forever",
                before, heartbeatSeq());
    }

    // ------------------------------------------------------------------
    // takeover
    // ------------------------------------------------------------------

    @Test
    public void testAbandonedClaimIsTakenOverAndResumedFromTheRecordedVersion() throws Exception {

        final Claim dead = abandonedClaim("dead-host");
        SchemaVersionMarker.recordApplied(database, dead, 1, "applied before the holder died");

        final TestMigration first = new TestMigration(1);
        final TestMigration second = new TestMigration(2);
        final TestMigration third = new TestMigration(3);
        runner(List.of(first, second, third), 3).run();

        assertEquals("version 1 was recorded and must not re-run", 0, first.applyCount.get());
        assertEquals(1, second.applyCount.get());
        assertEquals(1, third.applyCount.get());

        assertEquals(1, second.stateWhenApplied.takeoverCount());
        assertFalse(dead.token().equals(second.stateWhenApplied.claimToken()));
        assertEquals(
                "operations carry the comment of the claim they run under",
                MigrationContext.OPERATION_COMMENT_PREFIX + second.stateWhenApplied.claimToken(),
                second.commentWhenApplied);
        assertEquals(
                "an advanced version is progress and resets the count",
                0, third.stateWhenApplied.takeoverCount());

        // what the readiness gate tells a client, and the migration.active gauge, read this
        assertEquals(InitializationStatus.Phase.MIGRATING, second.statusWhenApplied.phase());
        assertEquals(2, second.statusWhenApplied.migrationVersion());
        assertEquals(3, second.statusWhenApplied.targetVersion());
        assertEquals(3, third.statusWhenApplied.migrationVersion());
        assertEquals(
                "the migration is over once run() returns",
                InitializationStatus.Phase.STARTING, InitializationStatus.current().phase());

        final MarkerState state = SchemaVersionMarker.read(database);
        assertEquals(3, state.version());
        assertFalse(state.migrating());
        assertNull(state.claimToken());
    }

    @Test
    public void testLiveHolderIsNotTakenOverAndTheWaiterReturnsWhenItReleases() throws Exception {

        final Claim live = abandonedClaim("live-host");
        final ClaimHeartbeat holderHeartbeat =
                new ClaimHeartbeat(database, live, LIVE_HEARTBEAT_MILLIS);
        holderHeartbeat.start();
        try {
            final TestMigration migration = new TestMigration(1);
            final Future<Void> waiter = runInBackground(new LeaseRunner(
                    database, adminDatabase, List.of(migration), 1,
                    LONG_WAIT_MILLIS, LIVE_STALE_AFTER_MILLIS, 200L));

            // well past stale-after: a holder that is beating must still hold its claim
            Thread.sleep(LIVE_STALE_AFTER_MILLIS + 1_500L);
            assertFalse("the waiter must still be waiting", waiter.isDone());
            assertEquals(
                    InitializationStatus.Phase.WAITING, InitializationStatus.current().phase());
            final MarkerState held = SchemaVersionMarker.read(database);
            assertEquals(live.token(), held.claimToken());
            assertEquals(0, held.takeoverCount());

            SchemaVersionMarker.recordApplied(database, live, 1, "applied by the holder");
            holderHeartbeat.stop();
            SchemaVersionMarker.releaseClaim(database, live);

            waiter.get(30, TimeUnit.SECONDS);
            assertEquals("the waiter must not migrate", 0, migration.applyCount.get());
        } finally {
            holderHeartbeat.stop();
        }
    }

    @Test
    public void testTwoWaitersRacingATakeoverMigrateOnce() throws Exception {

        abandonedClaim("dead-host");

        final AtomicInteger applied = new AtomicInteger();
        final List<Future<Void>> waiters = new ArrayList<>();
        for (int i = 0; i < 2; i++) {
            waiters.add(runInBackground(runner(
                    List.of(new TestMigration(1, () -> {
                        applied.incrementAndGet();
                        Thread.sleep(300L);
                        return null;
                    })), 1)));
        }
        for (Future<Void> waiter : waiters) {
            waiter.get(30, TimeUnit.SECONDS);
        }

        assertEquals("exactly one waiter wins the takeover", 1, applied.get());
        assertEquals(1, SchemaVersionMarker.read(database).version());
    }

    @Test
    public void testTakeoverAtTheTargetVersionAppliesNothingAndReleases() throws Exception {

        // The takeover's filter does not include the version, and recording a migration does not
        // move the heartbeat. So the holder can record its last migration and die between the
        // waiter's read and its takeover; the version the takeover returns is then the target.
        final Claim dead = abandonedClaim("dead-host");
        final AtomicBoolean advanced = new AtomicBoolean();
        final MongoDatabase intercepted = LeaseTestSupport.interceptingMarkerWrites(
                database, (filter, update) -> {
                    if (filter.toJson().contains("heartbeatSeq") && advanced.compareAndSet(false, true)) {
                        LeaseTestSupport.setOnMarker(database, new Document("version", 1));
                    }
                });

        final TestMigration migration = new TestMigration(1);
        runner(intercepted, List.of(migration), 1).run();

        assertTrue("the takeover must have been attempted", advanced.get());
        assertEquals("version 1 was already recorded", 0, migration.applyCount.get());
        final MarkerState state = SchemaVersionMarker.read(database);
        assertEquals(1, state.version());
        assertFalse("the claim must be released, or it is stuck for good", state.migrating());
        assertNull(state.claimToken());
        assertFalse(dead.token().isEmpty());
    }

    @Test
    public void testTakeoverAboveTheTargetVersionReleasesAndRefusesToStart() throws Exception {

        // A newer build's migrator stopped partway and this older binary took its claim over.
        abandonedClaim("newer-build-host");
        final AtomicBoolean advanced = new AtomicBoolean();
        final MongoDatabase intercepted = LeaseTestSupport.interceptingMarkerWrites(
                database, (filter, update) -> {
                    if (filter.toJson().contains("heartbeatSeq") && advanced.compareAndSet(false, true)) {
                        LeaseTestSupport.setOnMarker(database, new Document("version", 7));
                    }
                });

        final TestMigration migration = new TestMigration(1);
        try {
            runner(intercepted, List.of(migration), 1).run();
            fail("expected refusal on a schema newer than this binary");
        } catch (DpException ex) {
            assertTrue(ex.getMessage(), ex.getMessage().contains("newer"));
        }

        assertEquals(0, migration.applyCount.get());
        final MarkerState state = SchemaVersionMarker.read(database);
        assertEquals(7, state.version());
        assertFalse(
                "released, so a process of the newer build can claim the migration afresh",
                state.migrating());
    }

    // ------------------------------------------------------------------
    // the three bounds on a takeover
    // ------------------------------------------------------------------

    @Test
    public void testFailedMigrationMarksItsClaimAndIsNeverTakenOver() throws Exception {

        final TestMigration failing = new TestMigration(1, () -> {
            throw new DpException(
                    "deliberate failure reaching mongodb://dpuser:hunter2@db.example:27017/dp");
        });
        try {
            runner(List.of(failing), 1).run();
            fail("expected the migration failure to propagate");
        } catch (DpException ex) {
            assertTrue(ex.getMessage(), ex.getMessage().contains("deliberate failure"));
            assertTrue(ex.getMessage(), ex.getMessage().contains("marked failed"));
        }

        MarkerState state = SchemaVersionMarker.read(database);
        assertTrue("the claim stays held", state.migrating());
        assertTrue(state.migrationFailed());
        assertEquals(Integer.valueOf(1), state.migrationFailedVersion());
        assertTrue(state.migrationFailedMessage().contains("deliberate failure"));
        assertFalse(state.migrationFailedMessage().contains("hunter2"));
        assertHeartbeatHasStopped();

        // A waiter does not take the claim over, and does not wait out its deadline to say so:
        // only an operator ever removes the flag.
        final TestMigration retry = new TestMigration(1);
        final long started = System.nanoTime();
        try {
            runner(List.of(retry), 1).run();
            fail("a failed claim must block startup");
        } catch (DpException ex) {
            assertTrue(
                    "message should name the failed version and why: " + ex.getMessage(),
                    ex.getMessage().contains("version 1 failed")
                            && ex.getMessage().contains("deliberate failure"));
        }
        assertTrue(
                "the waiter must throw at once, not at its deadline",
                TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - started) < LONG_WAIT_MILLIS / 4);
        assertEquals(0, retry.applyCount.get());

        state = SchemaVersionMarker.read(database);
        assertTrue(state.migrating());
        assertEquals(0, state.takeoverCount());
    }

    @Test
    public void testWaiterBesideAClaimThatReplacedAnOldFailureFlagWaits() throws Exception {

        // The 1.16.0 runbook clear leaves migrationFailed on an unclaimed marker. The next claim
        // removes it; a waiter that still saw it would exit at once against a healthy holder.
        LeaseTestSupport.setOnMarker(database, new Document("migrating", false)
                .append("migrationFailed", true)
                .append("migrationFailedVersion", 1)
                .append("migrationFailedMessage", "an old failure"));

        final Claim live = abandonedClaim("live-host");
        final ClaimHeartbeat holderHeartbeat =
                new ClaimHeartbeat(database, live, LIVE_HEARTBEAT_MILLIS);
        holderHeartbeat.start();
        try {
            final TestMigration migration = new TestMigration(1);
            final Future<Void> waiter = runInBackground(new LeaseRunner(
                    database, adminDatabase, List.of(migration), 1,
                    LONG_WAIT_MILLIS, LIVE_STALE_AFTER_MILLIS, 200L));

            Thread.sleep(1_000L);
            assertFalse("the waiter must wait for the healthy holder", waiter.isDone());

            SchemaVersionMarker.recordApplied(database, live, 1, "applied by the holder");
            holderHeartbeat.stop();
            SchemaVersionMarker.releaseClaim(database, live);
            waiter.get(30, TimeUnit.SECONDS);
            assertEquals(0, migration.applyCount.get());
        } finally {
            holderHeartbeat.stop();
        }
    }

    @Test
    public void testAbandonedClaimAtTheCapIsNotTakenOver() throws Exception {

        final Claim dead = abandonedClaim("dead-host");
        LeaseTestSupport.setOnMarker(database, new Document(
                "takeoverCount", SchemaMigrationRunner.MAX_TAKEOVERS_PER_VERSION));

        final TestMigration migration = new TestMigration(1);
        final long started = System.nanoTime();
        try {
            runner(List.of(migration), 1).run();
            fail("a claim at the cap must not be taken over");
        } catch (DpException ex) {
            assertTrue(
                    "message should say how often it was abandoned: " + ex.getMessage(),
                    ex.getMessage().contains("abandoned by 4 processes"));
        }
        assertTrue(
                "the waiter must throw once the claim is stale, not at its deadline",
                TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - started) < LONG_WAIT_MILLIS / 4);
        assertEquals(0, migration.applyCount.get());
        assertEquals(dead.token(), SchemaVersionMarker.read(database).claimToken());
    }

    @Test
    public void testTakeoverWaitsForAnOperationOfThePreviousClaimStillRunning() throws Exception {

        final Claim dead = abandonedClaim("dead-host");

        // The dead holder's operation, still running on the server: a slow update carrying the
        // migration comment. A client that is killed leaves exactly this behind.
        final int documents = 30;
        final MongoCollection<Document> scratch = database.getCollection(SCRATCH_COLLECTION);
        for (int i = 0; i < documents; i++) {
            scratch.insertOne(new Document("n", i));
        }
        final Future<Void> orphan = executor.submit(() -> {
            scratch.updateMany(
                    new Document("$where", "sleep(100); return true;"),
                    new Document("$set", new Document("done", true)),
                    new UpdateOptions().comment(
                            MigrationContext.OPERATION_COMMENT_PREFIX + dead.token()));
            return null;
        });

        // wait until the server reports it, so the runner cannot check before it has started
        final long visibleBy = System.currentTimeMillis() + 10_000L;
        while (SchemaVersionMarker.findOrphanedOperations(adminDatabase, database.getName()).isEmpty()) {
            assertTrue("the slow update never appeared in $currentOp",
                    System.currentTimeMillis() < visibleBy);
            Thread.sleep(20L);
        }

        final long orphanCheckIntervalMillis = 500L;
        final long[] unfinishedWhenApplied = {-1};
        final TestMigration migration = new TestMigration(1, () -> {
            unfinishedWhenApplied[0] = scratch.countDocuments(new Document("done", new Document("$ne", true)));
            return null;
        });

        try (LeaseTestSupport.RecordingClient recording = new LeaseTestSupport.RecordingClient()) {
            final long runStarted = System.nanoTime();
            new LeaseRunner(
                    database, recording.database(MongoClientBase.ADMIN_DATABASE_NAME),
                    List.of(migration), 1,
                    LONG_WAIT_MILLIS, STALE_AFTER_MILLIS, orphanCheckIntervalMillis).run();
            final long runMillis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - runStarted);

            assertEquals(1, migration.applyCount.get());
            assertEquals(
                    "the takeover started while the previous claim's update was still running",
                    0L, unfinishedWhenApplied[0]);

            // The update runs for about three seconds. Checked on every 50 ms poll that is about
            // sixty $currentOp commands; at the 500 ms orphan interval it is about seven. The
            // bound follows the time the run actually took, so a slow machine, where the update
            // runs longer and is rightly checked more often, does not fail it: one check per
            // interval, plus the startup probe, the first check and the one before the takeover.
            final long maxChecks = runMillis / orphanCheckIntervalMillis + 3;
            final long checks = recording.started.stream()
                    .filter(LeaseTestSupport.StartedCommand::isCurrentOp).count();
            assertTrue("expected the check to repeat while the update ran: " + checks, checks >= 2);
            assertTrue(
                    "$currentOp must follow the orphan check interval, not the marker poll: " + checks
                            + " checks in " + runMillis + " ms",
                    checks <= maxChecks);
        }
        orphan.get(30, TimeUnit.SECONDS);
        assertEquals(1, SchemaVersionMarker.read(database).version());
    }

    @Test
    public void testOrphanCheckThatCannotRunRefusesTheTakeover() throws Exception {

        final Claim dead = abandonedClaim("dead-host");

        // $currentOp must be run on the admin database; handed any other, it fails, which stands
        // in for a server or a privilege that does not allow the check.
        final TestMigration migration = new TestMigration(1);
        final DpException ex = assertGivesUpAtTheClaimWait(new LeaseRunner(
                database, database, List.of(migration), 1,
                SHORT_WAIT_MILLIS, STALE_AFTER_MILLIS, 200L));
        assertTrue(
                "message should say the check failed: " + ex.getMessage(),
                ex.getMessage().contains("check for its operations")
                        && ex.getMessage().contains("$currentOp"));
        assertEquals(0, migration.applyCount.get());
        assertEquals(dead.token(), SchemaVersionMarker.read(database).claimToken());
    }

    @Test
    public void testStartupProbeReportsACheckThatCannotRunWithoutFailingStartup() throws Exception {

        // With nothing to migrate the check is never needed, so without the probe a deployment
        // where it is refused learns of that only when a claim is already stuck.
        markerCollection.deleteMany(new Document());
        SchemaVersionMarker.stampFresh(database, 1);

        assertNull(
                "the check runs against the admin database",
                runner(List.of(new TestMigration(1)), 1).probeOrphanCheck());

        // The result is kept for the takeover.enabled gauge: unknown until a runner has run, so
        // that "not yet known" is never reported as "refused".
        assertNull(InitializationStatus.current().takeoverEnabled());
        runner(List.of(new TestMigration(1)), 1).run();
        assertEquals(Boolean.TRUE, InitializationStatus.current().takeoverEnabled());

        final LeaseRunner unable = new LeaseRunner(
                database, database, List.of(new TestMigration(1)), 1,
                LONG_WAIT_MILLIS, STALE_AFTER_MILLIS, 200L);
        final String failure = unable.probeOrphanCheck();
        assertNotNull(failure);
        assertTrue(failure, failure.contains("$currentOp"));

        // reported, not fatal: the service is no worse off than before the lease
        unable.run();
        assertEquals(Boolean.FALSE, InitializationStatus.current().takeoverEnabled());
    }

    // ------------------------------------------------------------------
    // how long a waiter waits (issue #293)
    // ------------------------------------------------------------------

    /**
     * Asserts that a waiter gives up, and that it does so one claim wait after it started: not
     * sooner, and not after a deadline that something kept pushing out. Run in the background
     * with a bound, so that a waiter whose deadline is extended without end fails the test
     * instead of hanging it.
     */
    private DpException assertGivesUpAtTheClaimWait(SchemaMigrationRunner waiter) throws Exception {
        final long started = System.nanoTime();
        try {
            runInBackground(waiter).get(SHORT_WAIT_MILLIS * 6, TimeUnit.MILLISECONDS);
            fail("expected the waiter to give up");
            return null;
        } catch (ExecutionException ex) {
            final long elapsedMillis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - started);
            assertTrue("unexpected failure: " + ex.getCause(), ex.getCause() instanceof DpException);
            assertTrue(
                    "gave up after " + elapsedMillis + " ms, before the claim wait",
                    elapsedMillis >= SHORT_WAIT_MILLIS);
            return (DpException) ex.getCause();
        } catch (java.util.concurrent.TimeoutException ex) {
            throw new AssertionError(
                    "the waiter was still waiting after six claim waits: something that is not a "
                            + "sign of life is extending its deadline", ex);
        }
    }

    @Test
    public void testWaiterOutlastsTheClaimWaitWhileTheHoldersHeartbeatAdvances() throws Exception {

        // A long migration is the case #293 was filed for. Before it, a waiter exited one claim
        // wait after it started, however alive the holder was, and was restarted to do it again.
        final long staleAfterMillis = 2_000L;
        final long claimWaitMillis = 2_500L;

        final Claim live = abandonedClaim("live-host");
        final ClaimHeartbeat holderHeartbeat =
                new ClaimHeartbeat(database, live, LIVE_HEARTBEAT_MILLIS);
        holderHeartbeat.start();
        try {
            final TestMigration migration = new TestMigration(1);
            final Future<Void> waiter = runInBackground(new LeaseRunner(
                    database, adminDatabase, List.of(migration), 1,
                    claimWaitMillis, staleAfterMillis, 200L));

            Thread.sleep(claimWaitMillis * 2);
            assertFalse(
                    "a waiter must not give up on a migration whose heartbeat is advancing",
                    waiter.isDone());

            SchemaVersionMarker.recordApplied(database, live, 1, "applied by the holder");
            holderHeartbeat.stop();
            SchemaVersionMarker.releaseClaim(database, live);

            waiter.get(30, TimeUnit.SECONDS);
            assertEquals("the waiter must not migrate", 0, migration.applyCount.get());
        } finally {
            holderHeartbeat.stop();
        }
    }

    @Test
    public void testWaiterOutlastsTheClaimWaitWhileAnOrphanedOperationRunsAndThenTakesOver()
            throws Exception {

        // What a migrator stopped by a SIGTERM leaves: a stale claim, and its operation still
        // running on the server. The next process must sit through it, not crash-loop through it.
        final Claim dead = abandonedClaim("dead-host");

        final int documents = 40;
        final MongoCollection<Document> scratch = database.getCollection(SCRATCH_COLLECTION);
        for (int i = 0; i < documents; i++) {
            scratch.insertOne(new Document("n", i));
        }
        final Future<Void> orphan = executor.submit(() -> {
            scratch.updateMany(
                    new Document("$where", "sleep(100); return true;"),
                    new Document("$set", new Document("done", true)),
                    new UpdateOptions().comment(
                            MigrationContext.OPERATION_COMMENT_PREFIX + dead.token()));
            return null;
        });
        final long visibleBy = System.currentTimeMillis() + 10_000L;
        while (SchemaVersionMarker.findOrphanedOperations(adminDatabase, database.getName()).isEmpty()) {
            assertTrue("the slow update never appeared in $currentOp",
                    System.currentTimeMillis() < visibleBy);
            Thread.sleep(20L);
        }

        // the update runs for about four seconds, well past the claim wait
        final TestMigration migration = new TestMigration(1);
        final long started = System.nanoTime();
        new LeaseRunner(
                database, adminDatabase, List.of(migration), 1,
                SHORT_WAIT_MILLIS, STALE_AFTER_MILLIS, 200L).run();
        final long elapsedMillis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - started);

        assertTrue(
                "the test proves nothing unless the wait outlasted the claim wait: " + elapsedMillis,
                elapsedMillis > SHORT_WAIT_MILLIS);
        assertEquals("the waiter takes over once the operation has ended", 1, migration.applyCount.get());
        assertEquals(1, migration.stateWhenApplied.takeoverCount());
        orphan.get(30, TimeUnit.SECONDS);
        assertEquals(1, SchemaVersionMarker.read(database).version());
    }

    @Test
    public void testWaiterOnAClaimWithNoTokenStillGivesUpAtTheClaimWait() throws Exception {

        // A 1.16.0 holder: migrating, with no token and no heartbeat. Nothing distinguishes a slow
        // one from a dead one, so the wait keeps its bound.
        LeaseTestSupport.setOnMarker(database, new Document("migrating", true)
                .append("migratingHost", "1.16.0-host"));

        final TestMigration migration = new TestMigration(1);
        final DpException ex = assertGivesUpAtTheClaimWait(new LeaseRunner(
                database, adminDatabase, List.of(migration), 1,
                SHORT_WAIT_MILLIS, STALE_AFTER_MILLIS, 200L));
        assertTrue(ex.getMessage(), ex.getMessage().contains("no sign of life"));
        assertTrue(ex.getMessage(), ex.getMessage().contains("predates migration takeover"));
        assertEquals(0, migration.applyCount.get());
    }

    @Test
    public void testClaimWhoseHeartbeatNeverMovesDoesNotExtendTheWaitWhenItsWatchRestarts()
            throws Exception {

        // The staleness watch is restarted every time a takeover is refused, and then sees the
        // same dead (token, heartbeat) pair as if for the first time. A deadline extended whenever
        // the pair "differs from the watched pair" would be extended by each restart, for ever,
        // by a holder that has shown nothing.
        final Claim dead = abandonedClaim("dead-host");
        final AtomicInteger refused = new AtomicInteger();
        final MongoDatabase refusing = LeaseTestSupport.refusingTakeovers(database, refused);

        final TestMigration migration = new TestMigration(1);
        final DpException ex = assertGivesUpAtTheClaimWait(new LeaseRunner(
                refusing, adminDatabase, List.of(migration), 1,
                SHORT_WAIT_MILLIS, STALE_AFTER_MILLIS, 200L));

        assertTrue(ex.getMessage(), ex.getMessage().contains("no sign of life"));
        assertTrue(
                "the test proves nothing unless the watch restarted: " + refused.get() + " refusals",
                refused.get() >= 2);
        assertEquals(0, migration.applyCount.get());
        assertEquals(dead.token(), SchemaVersionMarker.read(database).claimToken());
    }

    // ------------------------------------------------------------------
    // failures that leave no flag
    // ------------------------------------------------------------------

    @Test
    public void testFailureThatCannotBeMarkedStillReportsTheMigrationFailure() throws Exception {

        final MongoDatabase intercepted = LeaseTestSupport.interceptingMarkerWrites(
                database, (filter, update) -> {
                    // the failure mark, not the claim, whose update unsets the same field
                    if (update.containsKey("$set")
                            && update.getDocument("$set").containsKey("migrationFailed")) {
                        throw new MongoException("simulated outage");
                    }
                });

        final TestMigration failing = new TestMigration(1, () -> {
            throw new DpException("deliberate test failure");
        });
        try {
            runner(intercepted, List.of(failing), 1).run();
            fail("expected the migration failure to propagate");
        } catch (DpException ex) {
            assertTrue(
                    "the migration's own failure is what must be reported: " + ex.getMessage(),
                    ex.getMessage().contains("deliberate test failure"));
            assertTrue(ex.getMessage(), ex.getMessage().contains("could NOT be recorded"));
            assertFalse(ex.getMessage(), ex.getMessage().contains("marked failed"));
        }

        final MarkerState state = SchemaVersionMarker.read(database);
        assertTrue(state.migrating());
        assertFalse(state.migrationFailed());
        assertHeartbeatHasStopped();
    }

    @Test
    public void testErrorFromAMigrationStopsTheHeartbeatAndWritesNoFlag() throws Exception {

        final TestMigration failing = new TestMigration(1, () -> {
            throw new StackOverflowError("deliberate test error");
        });
        try {
            runner(List.of(failing), 1).run();
            fail("expected the error to propagate");
        } catch (StackOverflowError expected) {
            // not converted, not flagged
        }

        final MarkerState state = SchemaVersionMarker.read(database);
        assertTrue(state.migrating());
        assertFalse(
                "an Error leaves no flag; the takeover cap is what bounds a retry",
                state.migrationFailed());
        assertHeartbeatHasStopped();
    }

    @Test
    public void testHolderDisplacedDuringAMigrationStopsWithoutFlaggingTheNewHoldersClaim()
            throws Exception {

        // The holder stalls inside migration 1, long enough to be taken over.
        final TestMigration first = new TestMigration(1, () -> {
            final MarkerState state = SchemaVersionMarker.read(database);
            assertNotNull(SchemaVersionMarker.takeOverClaim(
                    database, state.claimToken(), state.heartbeatSeq(), "new-holder",
                    SchemaMigrationRunner.MAX_TAKEOVERS_PER_VERSION));
            return null;
        });
        final TestMigration second = new TestMigration(2);

        // a long heartbeat interval, so the sequence read above cannot move before the takeover
        final LeaseRunner displaced = new LeaseRunner(
                database, adminDatabase, List.of(first, second), 2,
                new SchemaMigrationRunner.LeaseTimings(5_000L, 20_000L, 200L), LONG_WAIT_MILLIS);
        try {
            displaced.run();
            fail("a displaced holder must stop");
        } catch (ClaimLostException ex) {
            assertTrue(ex.getMessage(), ex.getMessage().contains("took the schema migration over"));
        }

        assertEquals("the displaced holder must not go on to the next migration",
                0, second.applyCount.get());
        final MarkerState state = SchemaVersionMarker.read(database);
        assertEquals("new-holder", state.migratingHost());
        assertEquals("the displaced holder's version must not be recorded", 0, state.version());
        assertFalse(
                "a flag here would block the process that took over", state.migrationFailed());
    }

    // ------------------------------------------------------------------
    // configuration
    // ------------------------------------------------------------------

    private void assertTimingsRejected(
            SchemaMigrationRunner.LeaseTimings timings, long claimWaitMillis, String expected)
            throws DpException {

        // Raised with nothing to migrate: a bad value must fail the deployment that introduced
        // it, not the next schema upgrade.
        markerCollection.deleteMany(new Document());
        SchemaVersionMarker.stampFresh(database, 1);
        try {
            new LeaseRunner(
                    database, adminDatabase, List.of(new TestMigration(1)), 1, timings,
                    claimWaitMillis).run();
            fail("expected lease timings to be rejected: " + timings);
        } catch (DpException ex) {
            assertTrue(ex.getMessage(), ex.getMessage().contains(expected));
        }
    }

    @Test
    public void testLeaseTimingsAreValidatedOnEveryStartup() throws DpException {

        assertTimingsRejected(
                new SchemaMigrationRunner.LeaseTimings(0L, 60_000L, 5_000L), 300_000L,
                SchemaMigrationRunner.CFG_KEY_HEARTBEAT_INTERVAL_SECONDS + " must be at least 1");
        assertTimingsRejected(
                new SchemaMigrationRunner.LeaseTimings(10_000L, 29_000L, 5_000L), 300_000L,
                "at least three times");
        assertTimingsRejected(
                new SchemaMigrationRunner.LeaseTimings(10_000L, 300_000L, 5_000L), 300_000L,
                "below the schema migration claim wait");

        // the shipped defaults pass
        SchemaMigrationRunner.validateLeaseTimings(
                new SchemaMigrationRunner.LeaseTimings(10_000L, 60_000L, 5_000L),
                SchemaMigrationRunner.CLAIM_WAIT_TIMEOUT_MILLIS);
    }
}
