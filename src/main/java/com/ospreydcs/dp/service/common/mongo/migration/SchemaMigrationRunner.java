package com.ospreydcs.dp.service.common.mongo.migration;

import com.mongodb.MongoException;
import com.mongodb.client.MongoDatabase;
import com.ospreydcs.dp.service.common.bson.bucket.BucketSpanVerifier;
import com.ospreydcs.dp.service.common.config.ConfigurationManager;
import com.ospreydcs.dp.service.common.exception.DpException;
import com.ospreydcs.dp.service.common.mongo.MongoClientBase;
import com.ospreydcs.dp.service.common.mongo.migration.migrations.V1AnnotationCommentToDescription;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.lang.management.ManagementFactory;
import java.util.List;

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
    public static final int SCHEMA_VERSION = 1;

    /**
     * Migrations in application order. Must be contiguous from 1 through {@link #SCHEMA_VERSION};
     * {@code SchemaMigrationRunnerTest} asserts that, since a gap or duplicate would otherwise be
     * discovered only against a real database.
     */
    public static final List<Migration> MIGRATIONS = List.of(
            new V1AnnotationCommentToDescription()
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
     * "does it hold service data". So it includes {@code bucketSpanVerification} — a marker written
     * by {@code BucketSpanVerifier} on a previous startup is positive evidence of a prior deployment
     * even if every data collection has since been emptied by a purge, a retention wipe, or a
     * partial restore. Including it can only push a database toward "legacy", never toward "fresh",
     * which is the safe direction: a needless migration run against empty collections is idempotent,
     * while a legacy database mistaken for fresh is silently stamped as migrated.
     *
     * <p>Note that this constant is declared on {@code BucketSpanVerifier} rather than
     * {@code MongoClientBase}, so the reflection test that pins this list consults both classes.
     * {@code serviceMetadata} is the one deliberate exclusion: it holds the marker itself, so
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
            BucketSpanVerifier.COLLECTION_NAME_BUCKET_SPAN_VERIFICATION
    );

    // How long a waiting (non-claiming) process will wait for the migrating process to finish.
    static final long CLAIM_WAIT_TIMEOUT_MILLIS = 5 * 60 * 1000L;
    static final long CLAIM_WAIT_POLL_MILLIS = 500L;

    private final MongoDatabase database;
    private final List<Migration> migrations;
    private final int targetVersion;
    private final long claimWaitTimeoutMillis;
    private final long claimWaitPollMillis;

    public SchemaMigrationRunner(MongoDatabase database) {
        this(database, MIGRATIONS, SCHEMA_VERSION);
    }

    /** Test seam: run an alternative migration list against a target version. */
    public SchemaMigrationRunner(
            MongoDatabase database, List<Migration> migrations, int targetVersion) {
        this(database, migrations, targetVersion,
                CLAIM_WAIT_TIMEOUT_MILLIS, CLAIM_WAIT_POLL_MILLIS);
    }

    /** Test seam: as above, with a shortened wait so the stuck-claim path is testable. */
    protected SchemaMigrationRunner(
            MongoDatabase database,
            List<Migration> migrations,
            int targetVersion,
            long claimWaitTimeoutMillis,
            long claimWaitPollMillis) {
        this.database = database;
        this.migrations = migrations;
        this.targetVersion = targetVersion;
        this.claimWaitTimeoutMillis = claimWaitTimeoutMillis;
        this.claimWaitPollMillis = claimWaitPollMillis;
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

        // Whether or not the marker already shows a migration in progress, the path is the same:
        // attempt the claim, and wait if it is refused. A marker showing migrating=true simply means
        // the first claim attempt is certain to fail.
        migrateOrWait(state.version());
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
     * Brings the database to the target version, either by migrating it or by waiting for whichever
     * process won the claim.
     *
     * <p><b>One deadline governs the whole call, across every claim attempt.</b> This is a single
     * loop rather than a pair of claim-and-wait methods calling each other. In the recursive form
     * each takeover allocated a fresh timeout and added a stack frame, so the bound this class
     * documents — wait a bounded time, then refuse to start — was not actually the bound in force.
     * Repeatedly losing the claim race is rare enough that the recursive form was not observed to
     * fail in practice, since the loser of one race usually wins the next; this is a structural
     * correction rather than a fix for a reproduced hang. An absolute deadline computed once, here,
     * is what makes the documented bound true regardless of how many takeovers occur.
     */
    private void migrateOrWait(int fromVersion) throws DpException {

        final String host = processIdentity();
        final long deadline = System.currentTimeMillis() + claimWaitTimeoutMillis;

        int observedVersion = fromVersion;
        SchemaVersionMarker.MarkerState observed = SchemaVersionMarker.read(database);

        while (true) {

            if (SchemaVersionMarker.claimForMigration(database, observedVersion, host)) {

                logger.info(
                        "claimed schema migration from version {} to {} as {}",
                        observedVersion, targetVersion, host);

                applyPending(observedVersion);

                SchemaVersionMarker.releaseClaim(database);
                logger.info(
                        "schema migration complete; database is at version {}", targetVersion);
                return;
            }

            // The claim was refused: another process holds it, or the version moved under us
            // between our read and our claim.
            if (System.currentTimeMillis() >= deadline) {
                throw new DpException(
                        "timed out after " + (claimWaitTimeoutMillis / 1000) + "s waiting for a "
                                + "schema migration held by " + describeHolder(observed) + ". If no "
                                + "migration is actually running, a previous process crashed while "
                                + "holding the claim; see doc/schema-migration.md for how to clear "
                                + "it.");
            }

            try {
                Thread.sleep(claimWaitPollMillis);
            } catch (InterruptedException ex) {
                Thread.currentThread().interrupt();
                throw new DpException("interrupted while waiting for schema migration to complete");
            }

            final SchemaVersionMarker.MarkerState state = SchemaVersionMarker.read(database);

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

            if (!state.migrating()) {
                // The claim cleared but the version did not reach the target, so the holder released
                // without completing. Retry the claim at whatever version it actually reached,
                // against the same deadline rather than a fresh one.
                logger.info(
                        "migration claim released at version {}; attempting to take over migration "
                                + "to {}",
                        state.version(), targetVersion);
            }

            observedVersion = state.version();
            observed = state;
        }
    }

    /**
     * Applies each pending migration in order, recording the version after each.
     *
     * <p>The claim is deliberately <b>not</b> released when a migration fails. The database is then
     * in a state no automatic rule can characterize — partway through an ordered set of changes —
     * and every subsequent startup blocks until an operator has looked at it. That is the intended
     * outcome: an outage rather than silent service from an unknown schema.
     */
    private void applyPending(int fromVersion) throws DpException {

        for (Migration migration : migrations) {

            if (migration.version() <= fromVersion) {
                continue;
            }

            logger.info(
                    "applying schema migration version {}: {}",
                    migration.version(), migration.description());

            try {
                migration.apply(database);
            } catch (DpException ex) {
                throw new DpException(
                        "schema migration version " + migration.version() + " ("
                                + migration.description() + ") failed: " + ex.getMessage()
                                + ". The migration claim has been left in place, so the service will "
                                + "not start until an operator resolves this; see "
                                + "doc/schema-migration.md.", ex);
            } catch (RuntimeException ex) {
                // A migration that throws unchecked would otherwise escape init() and be reported as
                // something other than a migration failure, losing the recovery instructions.
                throw new DpException(
                        "schema migration version " + migration.version() + " ("
                                + migration.description() + ") failed unexpectedly: "
                                + ex.getMessage()
                                + ". The migration claim has been left in place, so the service will "
                                + "not start until an operator resolves this; see "
                                + "doc/schema-migration.md.", ex);
            }

            SchemaVersionMarker.recordApplied(
                    database, migration.version(), migration.description());
        }
    }

    private static String describeHolder(SchemaVersionMarker.MarkerState state) {
        final String host = state.migratingHost() == null ? "an unknown process" : state.migratingHost();
        if (state.migratingSince() == null) {
            return host;
        }
        return host + " since " + state.migratingSince();
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
