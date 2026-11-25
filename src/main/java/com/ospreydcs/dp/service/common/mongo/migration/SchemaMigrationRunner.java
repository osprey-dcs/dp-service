package com.ospreydcs.dp.service.common.mongo.migration;

import com.mongodb.MongoException;
import com.mongodb.client.MongoDatabase;
import com.ospreydcs.dp.service.common.config.ConfigurationManager;
import com.ospreydcs.dp.service.common.exception.DpException;
import com.ospreydcs.dp.service.common.mongo.MongoClientBase;
import com.ospreydcs.dp.service.common.mongo.migration.migrations.V1AnnotationCommentToDescription;
import com.ospreydcs.dp.service.common.mongo.migration.migrations.V2NormalizeAnnotationTags;
import com.ospreydcs.dp.service.common.mongo.migration.migrations.V3CanonicalizeAnnotationReferenceIds;
import com.ospreydcs.dp.service.common.mongo.migration.migrations.V4StampColumnDiscriminators;
import com.ospreydcs.dp.service.common.mongo.migration.migrations.V5SeedPvStatsMaxBucketSpan;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.lang.management.ManagementFactory;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * Applies pending schema migrations at startup, and refuses to start on a schema this binary cannot
 * handle.
 *
 * <p>The mechanism fails <b>closed</b>. Its first migration — the annotation {@code comment} →
 * {@code description} rename — fails silently if skipped: an unmigrated annotation reads back with a
 * null description rather than an error, and a null description is indistinguishable from a record
 * legitimately saved without one. A delivery mechanism that also failed silently would compound one
 * silent failure with another, so a database this binary cannot establish the shape of stops the
 * service rather than being served from.
 *
 * <p><b>Absence of a marker is resolved by emptiness, not assumption.</b> A database with no marker
 * is either a fresh install or a deployment predating this mechanism, and nothing in the data
 * distinguishes them. Treating "no marker" as always-version-0 makes every fresh install replay an
 * accumulating migration list against empty collections; treating it as always-current silently
 * stamps a real unmigrated deployment as done — the failure this class exists to prevent. So the
 * runner probes: no marker plus no documents anywhere is a fresh install, stamped at the current
 * version; no marker plus any document is a legacy database, migrated from 0.
 *
 * <p>The probe answers as of the moment it runs, and it is not atomic with the stamp that follows —
 * a write landing in between would be read as a fresh install. Nothing serves requests until this
 * completes, so in practice the only writer is another starting process, and that one is racing to
 * write the same marker rather than data. Worth knowing rather than assuming the probe is a
 * transaction, since the rest of this design leans on it.
 *
 * <p>Every service process runs {@code MongoClientBase.init()} and the documented deployment starts
 * three of them, so concurrent startup is normal. See {@link SchemaVersionMarker#claimForMigration}
 * for how exactly one process is elected to migrate while the others wait.
 *
 * <p><b>A claim whose holder has gone away is taken over, within three bounds (issue #292).</b> The
 * holder increments a heartbeat on the marker; a waiter that sees it stop for
 * {@code schemaMigrationStaleAfterSeconds} takes the claim over and resumes from the recorded
 * version. The bounds are as much the design as the takeover:
 *
 * <ul>
 *   <li>A migration that <b>failed</b> marks its claim, and a marked claim is never taken over. It
 *       waits for an operator, as every failed migration did before the lease.</li>
 *   <li>A takeover waits while an operation of any earlier claim is <b>still running on the
 *       server</b>. A killed client's operation runs to completion there; taking over beside it
 *       would start a second full scan, and a third after the next kill.</li>
 *   <li>A claim is taken over at most {@link #MAX_TAKEOVERS_PER_VERSION} times without the version
 *       advancing. This is the only thing that stops a migration which kills its own process (an
 *       {@code OutOfMemoryError} under {@code -XX:+ExitOnOutOfMemoryError} writes no failure mark)
 *       from being retried without end.</li>
 * </ul>
 */
public class SchemaMigrationRunner {

    private static final Logger logger = LogManager.getLogger();

    public static final String CFG_KEY_RUN_MIGRATIONS_ON_STARTUP =
            "MongoClient.runSchemaMigrationsOnStartup";
    public static final boolean DEFAULT_RUN_MIGRATIONS_ON_STARTUP = true;

    /**
     * Schema version this binary expects. Incremented only when the schema changes, which is why it
     * is not derived from the Maven project version — most releases change no schema, so the two
     * version lines move at different rates and coupling them would mean either bumping this on
     * every release or maintaining a mapping.
     */
    public static final int SCHEMA_VERSION = 5;

    /**
     * Migrations in application order. Must be contiguous from 1 through {@link #SCHEMA_VERSION};
     * {@code SchemaMigrationRunnerTest} asserts that, since a gap or duplicate would otherwise be
     * discovered only against a real database.
     */
    public static final List<Migration> MIGRATIONS = List.of(
            new V1AnnotationCommentToDescription(),
            new V2NormalizeAnnotationTags(),
            new V3CanonicalizeAnnotationReferenceIds(),
            new V4StampColumnDiscriminators(),
            new V5SeedPvStatsMaxBucketSpan()
    );

    /**
     * Collections the emptiness probe consults, enumerated from the {@code COLLECTION_NAME_*}
     * constants rather than by hand.
     *
     * <p>Deriving this list from the constants is load-bearing. A collection added later and omitted
     * from a hand-copied list would make a populated database look fresh, which stamps it as
     * migrated and skips every migration — precisely the silent skip this class exists to prevent.
     * {@code SchemaMigrationRunnerTest} pins the list against the declared constants.
     *
     * <p>The probe asks "has any build ever used this database?", which is a broader question than
     * "does it hold service data". So it includes {@code bucketSpanVerification} — the marker the
     * startup bucket-span check wrote before #232 removed it. That marker is positive evidence of a
     * prior deployment even if every data collection has since been emptied by a purge, a retention
     * wipe, or a partial restore. Including it can only push a database toward "legacy", never
     * toward "fresh", which is the safe direction: a needless migration run against empty
     * collections is idempotent, while a legacy database mistaken for fresh is silently stamped as
     * migrated. Nothing writes that collection any more and migration v5 drops it; the name stays
     * on {@code MongoClientBase} as a legacy constant so a pre-v5 database is still classified
     * correctly.
     *
     * <p>{@code serviceMetadata} is the one deliberate exclusion: it holds the marker itself, so
     * including it would make every database look populated the moment a marker is written.
     */
    public static final List<String> MANAGED_COLLECTION_NAMES = List.of(
            MongoClientBase.COLLECTION_NAME_PROVIDERS,
            MongoClientBase.COLLECTION_NAME_BUCKETS,
            MongoClientBase.COLLECTION_NAME_REQUEST_STATUS,
            MongoClientBase.COLLECTION_NAME_DATA_SETS,
            MongoClientBase.COLLECTION_NAME_ANNOTATIONS,
            MongoClientBase.COLLECTION_NAME_CALCULATIONS,
            MongoClientBase.COLLECTION_NAME_PV_METADATA,
            MongoClientBase.COLLECTION_NAME_CONFIGURATIONS,
            MongoClientBase.COLLECTION_NAME_CONFIGURATION_ACTIVATIONS,
            MongoClientBase.COLLECTION_NAME_SAMPLE_STATUS_BUCKETS,
            MongoClientBase.COLLECTION_NAME_PV_STATS,
            MongoClientBase.COLLECTION_NAME_BUCKET_SPAN_VERIFICATION_LEGACY
    );

    public static final String CFG_KEY_HEARTBEAT_INTERVAL_SECONDS =
            "MongoClient.schemaMigrationHeartbeatIntervalSeconds";
    public static final int DEFAULT_HEARTBEAT_INTERVAL_SECONDS = 10;

    public static final String CFG_KEY_STALE_AFTER_SECONDS =
            "MongoClient.schemaMigrationStaleAfterSeconds";
    public static final int DEFAULT_STALE_AFTER_SECONDS = 60;

    /**
     * How many times a claim may be taken over without the schema version advancing. A constant,
     * not configuration: the recourse at the cap is an operator reading the log, and a deployment
     * that needs a larger number has a problem the number does not fix.
     */
    public static final int MAX_TAKEOVERS_PER_VERSION = 3;

    // How long a waiting (non-claiming) process will wait for the migrating process to finish.
    static final long CLAIM_WAIT_TIMEOUT_MILLIS = 5 * 60 * 1000L;
    static final long CLAIM_WAIT_POLL_MILLIS = 500L;

    /**
     * How often a waiter repeats the orphaned-operation check while it keeps finding one. Slower
     * than the marker poll on purpose: through mongos {@code $currentOp} is sent to every shard,
     * every waiting process runs its own, and an orphaned scan can run for hours.
     */
    static final long ORPHAN_CHECK_INTERVAL_MILLIS = 5_000L;

    /** The lease timings, in milliseconds. */
    protected record LeaseTimings(
            long heartbeatIntervalMillis, long staleAfterMillis, long orphanCheckIntervalMillis) {
    }

    private final MongoDatabase database;
    private final MongoDatabase adminDatabase;
    private final List<Migration> migrations;
    private final int targetVersion;
    private final long claimWaitTimeoutMillis;
    private final long claimWaitPollMillis;

    // null: read from configuration in run()
    private final LeaseTimings leaseTimingsOverride;
    private LeaseTimings leaseTimings;

    /**
     * @param adminDatabase the same client's {@code admin} database, where the orphaned-operation
     *     check must be run
     */
    public SchemaMigrationRunner(MongoDatabase database, MongoDatabase adminDatabase) {
        this(database, adminDatabase, MIGRATIONS, SCHEMA_VERSION);
    }

    /** Test seam: run an alternative migration list against a target version. */
    public SchemaMigrationRunner(
            MongoDatabase database, MongoDatabase adminDatabase,
            List<Migration> migrations, int targetVersion) {
        this(database, adminDatabase, migrations, targetVersion,
                CLAIM_WAIT_TIMEOUT_MILLIS, CLAIM_WAIT_POLL_MILLIS, null);
    }

    /**
     * Test seam: as above, with a shortened wait and lease so the waiting paths are testable.
     *
     * @param leaseTimings null to read the configured values
     */
    protected SchemaMigrationRunner(
            MongoDatabase database,
            MongoDatabase adminDatabase,
            List<Migration> migrations,
            int targetVersion,
            long claimWaitTimeoutMillis,
            long claimWaitPollMillis,
            LeaseTimings leaseTimings) {
        this.database = database;
        this.adminDatabase = adminDatabase;
        this.migrations = migrations;
        this.targetVersion = targetVersion;
        this.claimWaitTimeoutMillis = claimWaitTimeoutMillis;
        this.claimWaitPollMillis = claimWaitPollMillis;
        this.leaseTimingsOverride = leaseTimings;
    }

    protected static ConfigurationManager configMgr() {
        return ConfigurationManager.getInstance();
    }

    /**
     * Brings the database to {@link #SCHEMA_VERSION}, or throws.
     *
     * <p>Note that disabling migrations by configuration skips <i>applying</i> them, not the version
     * check: an operator who migrates out of band still must not run this binary against a schema it
     * does not match.
     *
     * @throws DpException if the schema cannot be established or a migration fails. The caller must
     *     abort startup.
     */
    public void run() throws DpException {

        // Validated on every startup, not only when a migration is pending: a bad value then fails
        // the deployment that introduced it, not the next schema upgrade months later.
        leaseTimings = leaseTimingsOverride != null ? leaseTimingsOverride : configuredLeaseTimings();
        validateLeaseTimings(leaseTimings, claimWaitTimeoutMillis);

        final String orphanCheckFailure = probeOrphanCheck();
        if (orphanCheckFailure != null) {
            logger.warn(
                    "this service cannot take over an abandoned schema migration: {} Startup "
                            + "continues; nothing is affected unless a process is killed while it "
                            + "migrates, and that migration's claim will then need to be cleared "
                            + "by hand.",
                    orphanCheckFailure);
        }

        final boolean applyMigrations = configMgr().getConfigBoolean(
                CFG_KEY_RUN_MIGRATIONS_ON_STARTUP, DEFAULT_RUN_MIGRATIONS_ON_STARTUP);

        SchemaVersionMarker.MarkerState state = SchemaVersionMarker.read(database);

        if (!state.present()) {
            state = establishMarker();
        }

        if (state.version() == targetVersion) {
            logger.info("schema version {} is current; no migration needed", targetVersion);
            return;
        }

        if (state.version() > targetVersion) {
            // The database was written by a newer service than this binary. Its data may already use
            // fields this binary does not understand, so continuing is the case most likely to
            // corrupt silently. Downgrade migrations are not supported.
            throw new DpException(
                    "database schema version " + state.version() + " is newer than this service "
                            + "supports (" + targetVersion + "); it was written by a newer build. "
                            + "Deploy a service build of at least that schema version, or restore a "
                            + "database at version " + targetVersion + " or lower. Downgrade "
                            + "migrations are not supported.");
        }

        if (!applyMigrations) {
            throw new DpException(
                    "database schema version " + state.version() + " requires migration to "
                            + targetVersion + ", but " + CFG_KEY_RUN_MIGRATIONS_ON_STARTUP
                            + " is disabled. Migrate out of band and restart, or enable the setting.");
        }

        // Whether or not the marker already shows a migration in progress, the path is the same
        // loop: claim if the marker is unclaimed, otherwise wait for, or take over from, the holder.
        migrateOrWait();
    }

    /**
     * Creates the marker for a database that has none, choosing the version from whether the
     * database holds data. See the class comment for why this cannot be assumed either way.
     */
    private SchemaVersionMarker.MarkerState establishMarker() throws DpException {

        if (isEmptyDatabase()) {
            logger.info(
                    "no schema version marker and no data found; recording fresh database at "
                            + "schema version {}",
                    targetVersion);
            SchemaVersionMarker.stampFresh(database, targetVersion);
        } else {
            logger.info(
                    "no schema version marker but existing data found; treating database as "
                            + "schema version {} and migrating to {}",
                    SchemaVersionMarker.VERSION_UNMANAGED,
                    targetVersion);
            SchemaVersionMarker.createUnmanagedMarker(database);
        }

        // Re-read rather than assuming what was written: a concurrent process may have created the
        // marker first, in which case the insert above was a no-op and its value is what governs.
        final SchemaVersionMarker.MarkerState state = SchemaVersionMarker.read(database);
        if (!state.present()) {
            throw new DpException(
                    "schema version marker could not be established; the write reported success but "
                            + "the marker is not readable");
        }
        return state;
    }

    /**
     * Returns whether every managed collection is empty.
     *
     * <p>Uses {@code estimatedDocumentCount()}, which reads collection metadata rather than counting
     * documents, and short-circuits at the first non-empty collection — so the probe is O(number of
     * collections) regardless of archive size.
     */
    private boolean isEmptyDatabase() throws DpException {
        try {
            for (String collectionName : MANAGED_COLLECTION_NAMES) {
                if (database.getCollection(collectionName).estimatedDocumentCount() > 0) {
                    logger.debug("database has existing data in collection: {}", collectionName);
                    return false;
                }
            }
        } catch (MongoException ex) {
            throw new DpException(
                    "error determining whether database is empty: " + ex.getMessage(), ex);
        }
        return true;
    }

    /**
     * Runs the orphaned-operation check once, to find out whether it can be run at all, and returns
     * why not, or null if it can.
     *
     * <p>The check is otherwise run only when a claim has gone stale, so a deployment where it is
     * refused (through mongos it needs the {@code inprog} privilege) would first learn of that in
     * the middle of an incident, from a process declining to take over a stuck claim. Run at every
     * startup, the missing grant is reported on an ordinary day. It never fails startup: without
     * the check the service behaves as it did before the lease, which is not a reason to refuse to
     * serve.
     */
    String probeOrphanCheck() {
        try {
            SchemaVersionMarker.findOrphanedOperations(adminDatabase, database.getName());
            return null;
        } catch (DpException | RuntimeException ex) {
            return ex.getMessage();
        }
    }

    private static LeaseTimings configuredLeaseTimings() {
        return new LeaseTimings(
                TimeUnit.SECONDS.toMillis(configMgr().getConfigInteger(
                        CFG_KEY_HEARTBEAT_INTERVAL_SECONDS, DEFAULT_HEARTBEAT_INTERVAL_SECONDS)),
                TimeUnit.SECONDS.toMillis(configMgr().getConfigInteger(
                        CFG_KEY_STALE_AFTER_SECONDS, DEFAULT_STALE_AFTER_SECONDS)),
                ORPHAN_CHECK_INTERVAL_MILLIS);
    }

    /**
     * Rejects lease timings under which the lease would misbehave without saying so.
     *
     * <p>A stale-after below three heartbeat intervals lets one slow beat read as a dead holder. A
     * stale-after that is not below the claim wait means no waiter ever lives long enough to see a
     * claim go stale, which disables takeover silently.
     */
    static void validateLeaseTimings(LeaseTimings timings, long claimWaitTimeoutMillis)
            throws DpException {

        if (timings.heartbeatIntervalMillis() < 1) {
            throw new DpException(
                    CFG_KEY_HEARTBEAT_INTERVAL_SECONDS + " must be at least 1 (is "
                            + timings.heartbeatIntervalMillis() + " ms)");
        }
        if (timings.staleAfterMillis() < 3 * timings.heartbeatIntervalMillis()) {
            throw new DpException(
                    CFG_KEY_STALE_AFTER_SECONDS + " (" + timings.staleAfterMillis() + " ms) must be "
                            + "at least three times " + CFG_KEY_HEARTBEAT_INTERVAL_SECONDS + " ("
                            + timings.heartbeatIntervalMillis() + " ms); otherwise one slow "
                            + "heartbeat reads as a migration whose process has gone away");
        }
        if (timings.staleAfterMillis() >= claimWaitTimeoutMillis) {
            throw new DpException(
                    CFG_KEY_STALE_AFTER_SECONDS + " (" + timings.staleAfterMillis() + " ms) must be "
                            + "below the schema migration claim wait (" + claimWaitTimeoutMillis
                            + " ms); otherwise no waiting process lasts long enough to take over "
                            + "an abandoned migration");
        }
    }

    /**
     * Brings the database to the target version: by claiming and migrating it, by waiting for the
     * process that holds the claim, or by taking the claim over from a holder that has gone away.
     *
     * <p><b>One deadline governs the whole call, across every claim attempt.</b> This is a single
     * loop rather than a pair of claim-and-wait methods calling each other. In the recursive form
     * each retry allocated a fresh timeout and added a stack frame, so the bound this class
     * documents — wait a bounded time, then refuse to start — was not actually the bound in force.
     * An absolute deadline computed once, here, is what makes the documented bound true however
     * many attempts occur. Watching for a stale claim, the orphaned-operation check and the
     * takeover attempts all happen inside this loop and against that deadline; a process that wins
     * a claim or a takeover stops being bound by it, like any claimant.
     *
     * <p><b>Staleness is what this process observes, never a comparison of timestamps.</b> It
     * remembers the {@code (claimToken, heartbeatSeq)} pair it last read and when, on its own
     * monotonic clock, it first read it. The same pair for {@code staleAfter} is a stale claim. No
     * clock is compared across hosts, so skew can neither cause nor prevent a takeover, and a claim
     * with no token (a 1.16.0 holder) has nothing to watch and is never stale. The cost is that a
     * waiter always waits one full {@code staleAfter}, even for a holder that died days ago.
     */
    private void migrateOrWait() throws DpException {

        final String host = processIdentity();
        final long deadline = System.currentTimeMillis() + claimWaitTimeoutMillis;
        final long staleAfterNanos = TimeUnit.MILLISECONDS.toNanos(leaseTimings.staleAfterMillis());
        final long orphanCheckIntervalNanos =
                TimeUnit.MILLISECONDS.toNanos(leaseTimings.orphanCheckIntervalMillis());

        // The heartbeat pair being watched, and when it was first seen.
        String watchedToken = null;
        long watchedSeq = 0;
        long watchedSinceNanos = 0;

        // The orphaned-operation check, once the watched claim is stale.
        boolean orphanCheckDue = true;
        long lastOrphanCheckNanos = 0;
        List<SchemaVersionMarker.OrphanedOperation> orphans = List.of();
        String orphanCheckFailure = null;

        SchemaVersionMarker.MarkerState state = SchemaVersionMarker.read(database);

        while (true) {

            if (!state.migrating() && state.version() == targetVersion) {
                logger.info(
                        "schema migration by another process completed; database is at version {}",
                        targetVersion);
                return;
            }

            if (state.version() > targetVersion) {
                // A process running a newer binary migrated past what this one supports. Same
                // reasoning as the equivalent check in run(): continuing risks silent corruption.
                throw new DpException(
                        "database schema version " + state.version() + " is newer than this service "
                                + "supports (" + targetVersion + "); another process running a newer "
                                + "build migrated it while this one was waiting. Deploy a service "
                                + "build of at least that schema version. Downgrade migrations are "
                                + "not supported.");
            }

            boolean stale = false;

            if (!state.migrating()) {

                // Unclaimed and below the target: nobody has started, or a holder released without
                // completing. Claim at whatever version it actually reached.
                final SchemaVersionMarker.Claim claim =
                        SchemaVersionMarker.claimForMigration(database, state.version(), host);
                if (claim != null) {
                    logger.info(
                            "claimed schema migration from version {} to {} as {}",
                            claim.fromVersion(), targetVersion, host);
                    migrateWithClaim(claim);
                    return;
                }
                // Refused: another process claimed first, or the version moved. Wait and re-read.
                watchedToken = null;

            } else if (state.migrationFailed()) {

                // Never taken over, and not worth waiting out the deadline for: the mark is only
                // ever removed by an operator.
                throw new DpException(
                        "schema migration version " + state.migrationFailedVersion() + " failed in "
                                + describeHolder(state) + " and its claim is held for an operator: "
                                + state.migrationFailedMessage() + ". The service will not start "
                                + "until the failure is resolved and the claim cleared; see "
                                + "doc/runbooks/schema-migration.md.");

            } else if (state.claimToken() != null) {

                final long now = System.nanoTime();
                if (!state.claimToken().equals(watchedToken) || state.heartbeatSeq() != watchedSeq) {
                    watchedToken = state.claimToken();
                    watchedSeq = state.heartbeatSeq();
                    watchedSinceNanos = now;
                    orphanCheckDue = true;
                    orphans = List.of();
                    orphanCheckFailure = null;
                }
                stale = now - watchedSinceNanos >= staleAfterNanos;

                if (stale && state.takeoverCount() >= MAX_TAKEOVERS_PER_VERSION) {
                    throw new DpException(
                            "the schema migration from version " + state.version() + " has been "
                                    + "abandoned by " + (MAX_TAKEOVERS_PER_VERSION + 1)
                                    + " processes in a row without the version advancing (last "
                                    + "held by " + describeHolder(state) + "), and will not be "
                                    + "taken over again. Something is ending the migrating process "
                                    + "each time: a liveness probe, an out-of-memory kill, or the "
                                    + "migration itself. Find out what before clearing the claim; "
                                    + "see doc/runbooks/schema-migration.md.");
                }

                if (stale && (orphanCheckDue
                        || now - lastOrphanCheckNanos >= orphanCheckIntervalNanos)) {

                    // A takeover always follows a check made now that found nothing, never a
                    // remembered result. If the check cannot be run the takeover is refused:
                    // proceeding would restore the pile-up of scans on exactly the deployments
                    // where nobody can see it.
                    orphanCheckDue = false;
                    lastOrphanCheckNanos = now;
                    try {
                        orphans = SchemaVersionMarker.findOrphanedOperations(
                                adminDatabase, database.getName());
                        orphanCheckFailure = null;
                    } catch (DpException ex) {
                        // the check repeats every few seconds: log the trace once per claim
                        final boolean firstFailure = orphanCheckFailure == null;
                        orphans = List.of();
                        orphanCheckFailure = ex.getMessage();
                        logger.warn(
                                "the schema migration held by {} appears abandoned, but it is not "
                                        + "being taken over because the check for its operations "
                                        + "still running on the server failed: {}",
                                describeHolder(state), ex.getMessage(), firstFailure ? ex : null);
                    }

                    if (orphanCheckFailure == null && orphans.isEmpty()) {
                        final SchemaVersionMarker.Claim claim = SchemaVersionMarker.takeOverClaim(
                                database, watchedToken, watchedSeq, host,
                                MAX_TAKEOVERS_PER_VERSION);
                        if (claim != null) {
                            logger.warn(
                                    "took over the schema migration abandoned by {} (no heartbeat "
                                            + "for {} ms); resuming from version {} as {}, "
                                            + "takeover {} of {} for this version",
                                    describeHolder(state), leaseTimings.staleAfterMillis(),
                                    claim.fromVersion(), host, state.takeoverCount() + 1,
                                    MAX_TAKEOVERS_PER_VERSION);
                            migrateWithClaim(claim);
                            return;
                        }
                        // Refused: another waiter won, or the holder was alive after all and its
                        // heartbeat moved. Start watching again from the next read.
                        watchedToken = null;
                        stale = false;
                    } else if (!orphans.isEmpty()) {
                        logger.info(
                                "the schema migration held by {} appears abandoned, but {} of its "
                                        + "operations are still running on the server ({}); not "
                                        + "taking over until they end",
                                describeHolder(state), orphans.size(), describeOrphan(orphans.get(0)));
                    }
                }

            } else {
                // A claim with no token: a holder that predates the lease. Nothing to watch.
                watchedToken = null;
            }

            if (System.currentTimeMillis() >= deadline) {
                throw new DpException(
                        describeTimeout(state, stale, orphans, orphanCheckFailure));
            }

            try {
                Thread.sleep(claimWaitPollMillis);
            } catch (InterruptedException ex) {
                Thread.currentThread().interrupt();
                throw new DpException("interrupted while waiting for schema migration to complete");
            }

            state = SchemaVersionMarker.read(database);
        }
    }

    /**
     * Migrates under a claim this process has just won, by an ordinary claim or by a takeover, and
     * releases it.
     *
     * <p><b>The heartbeat is stopped in a {@code finally}.</b> If this thread ends with an
     * {@code Error} while another non-daemon thread keeps the process alive, a heartbeat stopped
     * only on the normal paths would keep a dead claim fresh indefinitely. It is also stopped
     * before the release on the success path, so that no beat finds the token already gone and
     * reports a lost claim.
     *
     * <p>A takeover can hand back a version this method has nothing to apply for, since the
     * takeover's filter does not include the version. At the target, the previous holder recorded
     * its last migration and stopped before releasing: apply nothing and release. Above the
     * target, a newer build's migrator stopped partway and this older binary took its claim:
     * release it, so that a process of the newer build can claim afresh, and refuse to start.
     */
    private void migrateWithClaim(SchemaVersionMarker.Claim claim) throws DpException {

        final ClaimHeartbeat heartbeat =
                new ClaimHeartbeat(database, claim, leaseTimings.heartbeatIntervalMillis());
        heartbeat.start();
        try {
            if (claim.fromVersion() > targetVersion) {
                heartbeat.stop();
                SchemaVersionMarker.releaseClaim(database, claim);
                throw new DpException(
                        "database schema version " + claim.fromVersion() + " is newer than this "
                                + "service supports (" + targetVersion + "); a newer build's "
                                + "migration was abandoned partway and this process took its claim "
                                + "over. The claim has been released so that a process of that "
                                + "build can resume it. Deploy a service build of at least that "
                                + "schema version. Downgrade migrations are not supported.");
            }

            applyPending(claim, heartbeat);

            heartbeat.stop();
            SchemaVersionMarker.releaseClaim(database, claim);
            logger.info("schema migration complete; database is at version {}", targetVersion);

        } finally {
            heartbeat.stop();
        }
    }

    /**
     * Applies each pending migration in order, recording the version after each.
     *
     * <p>The claim is deliberately <b>not</b> released when a migration fails. The database is then
     * in a state no automatic rule can characterize — partway through an ordered set of changes —
     * and every subsequent startup blocks until an operator has looked at it. That is the intended
     * outcome: an outage rather than silent service from an unknown schema. With the lease, keeping
     * the claim is no longer enough to get that outcome, because a claim whose holder exits is
     * taken over; the failure is therefore <b>marked on the claim</b>, and a marked claim is never
     * taken over.
     *
     * <p>Only a migration that threw {@code DpException} or {@code RuntimeException} is marked.
     * Three failures leave no mark, and the takeover cap is what bounds them:
     * <ul>
     *   <li>an {@code Error}: a fatal one usually ends the process before a catch could write
     *       anything, and one rule is easier to hold than a classification of errors here;</li>
     *   <li>a mark that could not be written, most likely in the outage that failed the migration.
     *       It is not retried: that would keep a failed process alive for the length of an outage
     *       to prevent at most three idempotent re-runs;</li>
     *   <li>a marker write that failed for a reason other than a lost claim. The migration did not
     *       fail, so there is nothing to mark.</li>
     * </ul>
     * A lost claim ({@link ClaimLostException}) is never marked: the claim is another process's.
     */
    private void applyPending(SchemaVersionMarker.Claim claim, ClaimHeartbeat heartbeat)
            throws DpException {

        final MigrationContext context = MigrationContext.forClaimToken(claim.token());

        for (Migration migration : migrations) {

            if (migration.version() <= claim.fromVersion()) {
                continue;
            }

            if (heartbeat.isClaimLost()) {
                throw new ClaimLostException(
                        "schema migration stopped before version " + migration.version() + ": the "
                                + "migration claim is no longer held by this process. Another "
                                + "process took the migration over, or the claim was cleared. "
                                + "This process has written nothing further to the marker.");
            }

            logger.info(
                    "applying schema migration version {}: {}",
                    migration.version(), migration.description());

            try {
                migration.apply(database, context);
            } catch (DpException | RuntimeException ex) {
                // RuntimeException as well: a migration that throws unchecked would otherwise
                // escape init() and be reported as something other than a migration failure,
                // losing the recovery instructions.
                final String outcome = markFailed(claim, migration, ex);
                throw new DpException(
                        "schema migration version " + migration.version() + " ("
                                + migration.description() + ") failed"
                                + (ex instanceof RuntimeException ? " unexpectedly" : "") + ": "
                                + ex.getMessage() + ". " + outcome, ex);
            }

            SchemaVersionMarker.recordApplied(
                    database, claim, migration.version(), migration.description());
        }
    }

    /**
     * Marks the claim failed and returns the sentence that tells the operator what state the claim
     * is in. Never throws: the migration's own failure is what the caller must report.
     */
    private String markFailed(SchemaVersionMarker.Claim claim, Migration migration, Exception cause) {
        try {
            SchemaVersionMarker.markFailed(database, claim, migration.version(), cause.getMessage());
            return "The migration claim has been left in place and marked failed, so the service "
                    + "will not start until an operator resolves this; see "
                    + "doc/runbooks/schema-migration.md.";
        } catch (ClaimLostException ex) {
            logger.error(
                    "schema migration version {} failed, and the failure was not recorded because "
                            + "the claim is no longer held by this process: {}",
                    migration.version(), ex.getMessage(), ex);
            return "The failure was not recorded on the marker, because the migration claim is no "
                    + "longer held by this process: another process took the migration over, or "
                    + "the claim was cleared.";
        } catch (DpException | RuntimeException ex) {
            logger.error(
                    "schema migration version {} failed, and recording the failure on the marker "
                            + "failed too: {}",
                    migration.version(), ex.getMessage(), ex);
            return "The failure could NOT be recorded on the marker (" + ex.getMessage() + "), so "
                    + "the claim will read as abandoned: another starting process may take it "
                    + "over and run this migration again, up to " + MAX_TAKEOVERS_PER_VERSION
                    + " times. See doc/runbooks/schema-migration.md.";
        }
    }

    private String describeTimeout(
            SchemaVersionMarker.MarkerState state,
            boolean stale,
            List<SchemaVersionMarker.OrphanedOperation> orphans,
            String orphanCheckFailure) {

        final String prefix = "timed out after " + (claimWaitTimeoutMillis / 1000) + "s waiting for "
                + "a schema migration held by " + describeHolder(state) + ". ";

        if (!state.migrating()) {
            return "timed out after " + (claimWaitTimeoutMillis / 1000) + "s trying to claim the "
                    + "schema migration from version " + state.version() + ": other processes kept "
                    + "claiming it without completing it. See doc/runbooks/schema-migration.md.";
        }
        if (state.claimToken() == null) {
            return prefix + "The claim has no heartbeat: it was written by a service build that "
                    + "predates migration takeover (1.16.0 or earlier), so this process cannot tell "
                    + "whether its holder is alive and will not take it over. If no migration is "
                    + "actually running, a previous process crashed while holding the claim; see "
                    + "doc/runbooks/schema-migration.md for how to clear it.";
        }
        if (stale && orphanCheckFailure != null) {
            return prefix + "Its heartbeat has stopped, but the claim was not taken over because "
                    + "the check for its operations still running on the server failed: "
                    + orphanCheckFailure + ". See doc/runbooks/schema-migration.md.";
        }
        if (stale && !orphans.isEmpty()) {
            final SchemaVersionMarker.OrphanedOperation orphan = orphans.get(0);
            return prefix + "Its heartbeat has stopped, so the process that held it is gone, but "
                    + orphans.size() + " operation(s) it or an earlier holder started are still "
                    + "running on the server: " + describeOrphan(orphan) + ". The claim will be "
                    + "taken over by a starting process once they end; restarting this service "
                    + "is enough. To end one early: db.killOp(" + killOpArgument(orphan) + "). See "
                    + "doc/runbooks/schema-migration.md.";
        }
        return prefix + "Its heartbeat is current, so the migration is still running; a long "
                + "migration outlasts this wait, and restarting this service once it completes is "
                + "enough. See doc/runbooks/schema-migration.md.";
    }

    private static String describeOrphan(SchemaVersionMarker.OrphanedOperation orphan) {
        return "opid " + orphan.opid() + " on " + orphan.ns() + ", running for "
                + orphan.runningSeconds() + "s, comment '" + orphan.comment() + "'";
    }

    // A numeric opid on a standalone or replica set; "shard:number", which killOp takes as a
    // string, through mongos.
    private static String killOpArgument(SchemaVersionMarker.OrphanedOperation orphan) {
        return orphan.opid().chars().allMatch(Character::isDigit)
                ? orphan.opid() : "\"" + orphan.opid() + "\"";
    }

    private static String describeHolder(SchemaVersionMarker.MarkerState state) {
        final StringBuilder holder = new StringBuilder(
                state.migratingHost() == null ? "an unknown process" : state.migratingHost());
        if (state.migratingSince() != null) {
            holder.append(" since ").append(state.migratingSince());
        }
        if (state.claimToken() != null) {
            holder.append(" (last heartbeat ").append(state.migratingHeartbeat())
                    .append(", taken over ").append(state.takeoverCount()).append(" time(s))");
        }
        return holder.toString();
    }

    /** Identifies this process in the marker, so an operator can tell who left a claim behind. */
    private static String processIdentity() {
        try {
            return ManagementFactory.getRuntimeMXBean().getName();
        } catch (RuntimeException ex) {
            return "unknown";
        }
    }
}
