package com.ospreydcs.dp.service.common.mongo.migration;

import com.mongodb.MongoException;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.FindOneAndUpdateOptions;
import com.mongodb.client.model.ReturnDocument;
import com.mongodb.client.model.Updates;
import com.mongodb.client.result.UpdateResult;
import com.ospreydcs.dp.service.common.config.SecretRedactor;
import com.ospreydcs.dp.service.common.exception.DpException;
import org.bson.Document;
import org.bson.conversions.Bson;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.regex.Pattern;

/**
 * Reads and writes the single marker document recording which schema version a database is at.
 *
 * <p>Structurally this is one document with a fixed {@code _id} in a dedicated collection, so the
 * collection holds exactly one record — the shape the startup bucket-span marker had before #232
 * removed that check. What it deliberately does <b>not</b> share with that marker is an
 * unsynchronized read-check-write. That was safe there because the work was idempotent and
 * read-only and its failure mode a lost optimization; a migration is neither. Every service
 * process runs {@code MongoClientBase.init()}, and the
 * documented deployment starts three of them, so two processes reaching the runner at once is the
 * normal case rather than an edge case.
 *
 * <p>Coordination therefore uses {@link #claimForMigration}, whose conditional
 * {@code findOneAndUpdate} is atomic at the single-document level. That is the only primitive
 * available: multi-document transactions require a replica set, which the deployment does not
 * guarantee.
 *
 * <p><b>The claim is a lease (issue #292).</b> A claim carries a random {@code claimToken} and a
 * {@code heartbeatSeq} its holder increments. A waiting process that sees the same pair for long
 * enough concludes the holder is gone and takes the claim over with {@link #takeOverClaim}, a
 * compare-and-swap on that pair. <b>Every marker write a holder makes filters on its token</b>
 * ({@link #recordApplied}, {@link #releaseClaim}, {@link #heartbeat}, {@link #markFailed}), so a
 * holder that was displaced while stalled cannot overwrite the new holder's state: its write
 * matches nothing and is reported as {@link ClaimLostException}. A new marker write by a holder
 * must take the {@link Claim} and filter the same way.
 *
 * <p>No code compares {@code migratingHeartbeat} with a clock. It is there for an operator to
 * read. A filter comparing it with server time matches a marker that has no such field at all,
 * since a missing value sorts below any date, and that marker is a live 1.16.0 migrator.
 *
 * <p>All operations work with raw {@link Document} rather than the POJO codec, for the reason given
 * on {@link Migration} — and here additionally because the marker has no corresponding domain class.
 */
public class SchemaVersionMarker {

    public static final String COLLECTION_NAME_SERVICE_METADATA = "serviceMetadata";

    // Single-document marker; the fixed _id keeps the collection to one record.
    public static final String MARKER_ID = "schemaVersion";

    static final String FIELD_ID = "_id";
    static final String FIELD_VERSION = "version";
    static final String FIELD_UPDATED_AT = "updatedAt";
    static final String FIELD_MIGRATING = "migrating";
    static final String FIELD_MIGRATING_SINCE = "migratingSince";
    static final String FIELD_MIGRATING_HOST = "migratingHost";
    static final String FIELD_APPLIED_MIGRATIONS = "appliedMigrations";

    // The claim lease (#292). Present while a claim is held, removed by releaseClaim().
    static final String FIELD_CLAIM_TOKEN = "claimToken";
    static final String FIELD_HEARTBEAT_SEQ = "heartbeatSeq";
    static final String FIELD_MIGRATING_HEARTBEAT = "migratingHeartbeat";
    static final String FIELD_TAKEOVER_COUNT = "takeoverCount";

    // Written when the holder's migration threw; removed by an operator or by the next fresh claim.
    static final String FIELD_MIGRATION_FAILED = "migrationFailed";
    static final String FIELD_MIGRATION_FAILED_VERSION = "migrationFailedVersion";
    static final String FIELD_MIGRATION_FAILED_MESSAGE = "migrationFailedMessage";

    static final int MIGRATION_FAILED_MESSAGE_MAX_LENGTH = 1000;

    static final String FIELD_APPLIED_VERSION = "version";
    static final String FIELD_APPLIED_DESCRIPTION = "description";
    static final String FIELD_APPLIED_AT = "appliedAt";

    /**
     * The version a database is at when no marker exists and the database holds data — i.e. a
     * deployment that predates this mechanism. Migration versions start at 1.
     */
    public static final int VERSION_UNMANAGED = 0;

    /**
     * State of the marker as read from the database.
     *
     * <p>{@code claimToken} is null on a marker that is not claimed and on a claim written by a
     * build that predates the lease (1.16.0); such a claim has no heartbeat to watch and is never
     * taken over.
     */
    public record MarkerState(boolean present, int version, boolean migrating,
                              Instant migratingSince, String migratingHost,
                              String claimToken, long heartbeatSeq, Instant migratingHeartbeat,
                              int takeoverCount, boolean migrationFailed,
                              Integer migrationFailedVersion, String migrationFailedMessage) {

        public static MarkerState absent() {
            return new MarkerState(
                    false, VERSION_UNMANAGED, false, null, null,
                    null, 0L, null, 0, false, null, null);
        }
    }

    /**
     * A held claim: the token every marker write by its holder filters on, and the recorded version
     * the claim was taken at, which is where the holder starts migrating from.
     */
    public record Claim(String token, int fromVersion) {
    }

    /** An operation of some migration claim that is still running on the server. */
    public record OrphanedOperation(String comment, String ns, String opid, long runningSeconds) {
    }

    private SchemaVersionMarker() {
    }

    public static MongoCollection<Document> collection(MongoDatabase database) {
        return database.getCollection(COLLECTION_NAME_SERVICE_METADATA);
    }

    /**
     * Reads the marker. A missing marker is reported as {@link MarkerState#absent()} rather than an
     * assumed version, because the caller must distinguish a fresh install from a legacy database
     * and cannot do so from the marker alone.
     *
     * @throws DpException if the read fails. Checked deliberately: the caller's only sound response
     *     to not knowing the schema version is to refuse to start, and a checked exception forces
     *     that decision rather than allowing a failure to be read as "no marker".
     */
    public static MarkerState read(MongoDatabase database) throws DpException {

        final Document marker;
        try {
            marker = collection(database).find(Filters.eq(FIELD_ID, MARKER_ID)).first();
        } catch (MongoException ex) {
            throw new DpException("error reading schema version marker: " + ex.getMessage(), ex);
        }

        if (marker == null) {
            return MarkerState.absent();
        }

        final Integer version = marker.getInteger(FIELD_VERSION);
        if (version == null) {
            throw new DpException(
                    "schema version marker exists but has no '" + FIELD_VERSION + "' field; "
                            + "the marker document is corrupt and must be repaired manually");
        }

        final Number failedVersion = readNumber(marker, FIELD_MIGRATION_FAILED_VERSION);
        final Number heartbeatSeq = readNumber(marker, FIELD_HEARTBEAT_SEQ);
        final Number takeoverCount = readNumber(marker, FIELD_TAKEOVER_COUNT);
        return new MarkerState(
                true,
                version,
                Boolean.TRUE.equals(marker.get(FIELD_MIGRATING)),
                readInstant(marker, FIELD_MIGRATING_SINCE),
                readString(marker, FIELD_MIGRATING_HOST),
                readString(marker, FIELD_CLAIM_TOKEN),
                heartbeatSeq == null ? 0L : heartbeatSeq.longValue(),
                readInstant(marker, FIELD_MIGRATING_HEARTBEAT),
                takeoverCount == null ? 0 : takeoverCount.intValue(),
                Boolean.TRUE.equals(marker.get(FIELD_MIGRATION_FAILED)),
                failedVersion == null ? null : failedVersion.intValue(),
                readString(marker, FIELD_MIGRATION_FAILED_MESSAGE));
    }

    // The lease fields are read leniently: a value of an unexpected type reads as absent rather
    // than throwing ClassCastException out of read(), where it would be reported as something
    // other than a marker problem. An unreadable token is then a claim with nothing to watch,
    // which is never taken over.
    private static Number readNumber(Document document, String key) {
        return document.get(key) instanceof Number number ? number : null;
    }

    private static String readString(Document document, String key) {
        return document.get(key) instanceof String string ? string : null;
    }

    /**
     * Reads a stored timestamp back as an {@link Instant}.
     *
     * <p>An {@code Instant} written through the default codec registry comes back as a
     * {@link java.util.Date} — BSON has one date type, and the driver's default decoding target for
     * it is {@code Date}. Asking {@code Document.get(key, Instant.class)} therefore throws
     * {@link ClassCastException} rather than returning null, which would escape {@code read()}
     * unchecked and be reported as something other than a marker problem.
     */
    static Instant readInstant(Document document, String key) {
        final Object value = document.get(key);
        if (value instanceof Instant instant) {
            return instant;
        }
        if (value instanceof java.util.Date date) {
            return date.toInstant();
        }
        return null;
    }

    /**
     * Records a fresh (empty) database as already being at {@code version}, without running
     * anything. Uses an insert rather than an upsert so that a marker created concurrently by
     * another starting process is not overwritten — the loser of that race sees a duplicate-key
     * error, which the runner treats as "someone else stamped it", re-reads, and proceeds.
     *
     * @return true if this call created the marker, false if one already existed
     */
    public static boolean stampFresh(MongoDatabase database, int version) throws DpException {

        final Document marker = new Document(FIELD_ID, MARKER_ID)
                .append(FIELD_VERSION, version)
                .append(FIELD_UPDATED_AT, Instant.now())
                .append(FIELD_MIGRATING, false)
                .append(FIELD_APPLIED_MIGRATIONS, new ArrayList<Document>());

        try {
            collection(database).insertOne(marker);
            return true;
        } catch (MongoException ex) {
            // Duplicate key means another process stamped it first, which is a benign race.
            if (isDuplicateKey(ex)) {
                return false;
            }
            throw new DpException("error creating schema version marker: " + ex.getMessage(), ex);
        }
    }

    /**
     * Attempts to claim the exclusive right to migrate from {@code observedVersion}.
     *
     * <p>The update matches only a marker that is still at the observed version and not already
     * being migrated, so exactly one of any number of concurrent callers succeeds. A caller that
     * fails must not proceed to migrate — see {@code SchemaMigrationRunner} for the wait path.
     *
     * <p>{@code migratingSince} and {@code migratingHost} are recorded so that an operator facing a
     * stuck claim can tell a genuinely hung migration from a merely slow one, and can identify which
     * process left it behind.
     *
     * <p><b>A fresh claim removes the three failure fields.</b> This filter matches only
     * {@code migrating != true}, so a claim that failed can be claimed afresh only after an operator
     * cleared it, and the flag then describes a failure that has been dealt with. The runbook clear
     * shipped with 1.16.0 unsets only {@code migratingSince} and {@code migratingHost}; an operator
     * following it leaves {@code migrationFailed: true} behind. Kept here, that flag would make
     * every waiter read this healthy claim as failed and exit at once, on this migration and every
     * later one, since nothing else removes it.
     *
     * @return the claim, or null if it was refused
     */
    public static Claim claimForMigration(
            MongoDatabase database, int observedVersion, String host) throws DpException {

        final String token = newClaimToken();
        final Document claimed;
        try {
            claimed = collection(database).findOneAndUpdate(
                    Filters.and(
                            Filters.eq(FIELD_ID, MARKER_ID),
                            Filters.eq(FIELD_VERSION, observedVersion),
                            Filters.ne(FIELD_MIGRATING, true)),
                    Updates.combine(
                            Updates.set(FIELD_MIGRATING, true),
                            Updates.set(FIELD_MIGRATING_SINCE, Instant.now()),
                            Updates.set(FIELD_MIGRATING_HOST, host),
                            Updates.set(FIELD_CLAIM_TOKEN, token),
                            Updates.set(FIELD_HEARTBEAT_SEQ, 0L),
                            Updates.currentDate(FIELD_MIGRATING_HEARTBEAT),
                            Updates.set(FIELD_TAKEOVER_COUNT, 0),
                            Updates.unset(FIELD_MIGRATION_FAILED),
                            Updates.unset(FIELD_MIGRATION_FAILED_VERSION),
                            Updates.unset(FIELD_MIGRATION_FAILED_MESSAGE)),
                    new FindOneAndUpdateOptions().returnDocument(ReturnDocument.AFTER));
        } catch (MongoException ex) {
            throw new DpException("error claiming schema migration: " + ex.getMessage(), ex);
        }

        return claimed == null ? null : new Claim(token, observedVersion);
    }

    /**
     * Attempts to take over a claim whose holder has stopped, as a compare-and-swap on the
     * {@code (claimToken, heartbeatSeq)} pair the caller has been watching.
     *
     * <p>The filter requires that pair unchanged, so of any number of waiters that reached the same
     * conclusion exactly one wins, and a holder that was alive after all (its heartbeat moved)
     * defeats every one of them. It also requires that the claim is not marked failed and that
     * {@code takeoverCount} is below {@code maxTakeovers}; a missing count is 0.
     *
     * <p><b>The returned claim's version comes from the document this write returned</b>, not from
     * the caller's earlier read. The filter does not include the version and {@link #recordApplied}
     * does not move the heartbeat, so the holder may have recorded a migration between the two. The
     * caller must handle a returned version equal to, or above, its target.
     *
     * <p>"Exactly one wins" assumes the write is majority-acknowledged. Through mongos the service
     * runs with {@code WriteConcern.ACKNOWLEDGED} until #291 is fixed, and a takeover acknowledged
     * by a primary that is then rolled back could be followed by a second one. Two migrators
     * running at once is the case every migration is required to tolerate (see {@link Migration}).
     *
     * @return the new claim, or null if it was refused
     */
    public static Claim takeOverClaim(
            MongoDatabase database, String observedToken, long observedSeq, String host,
            int maxTakeovers) throws DpException {

        final String token = newClaimToken();
        final Document claimed;
        try {
            claimed = collection(database).findOneAndUpdate(
                    Filters.and(
                            Filters.eq(FIELD_ID, MARKER_ID),
                            Filters.eq(FIELD_MIGRATING, true),
                            Filters.eq(FIELD_CLAIM_TOKEN, observedToken),
                            Filters.eq(FIELD_HEARTBEAT_SEQ, observedSeq),
                            Filters.ne(FIELD_MIGRATION_FAILED, true),
                            // not(gte) rather than lt: it also matches a missing count
                            Filters.not(Filters.gte(FIELD_TAKEOVER_COUNT, maxTakeovers))),
                    Updates.combine(
                            Updates.set(FIELD_CLAIM_TOKEN, token),
                            Updates.set(FIELD_HEARTBEAT_SEQ, 0L),
                            Updates.currentDate(FIELD_MIGRATING_HEARTBEAT),
                            Updates.set(FIELD_MIGRATING_SINCE, Instant.now()),
                            Updates.set(FIELD_MIGRATING_HOST, host),
                            Updates.inc(FIELD_TAKEOVER_COUNT, 1)),
                    new FindOneAndUpdateOptions().returnDocument(ReturnDocument.AFTER));
        } catch (MongoException ex) {
            throw new DpException("error taking over schema migration claim: " + ex.getMessage(), ex);
        }

        if (claimed == null) {
            return null;
        }
        final Number version = readNumber(claimed, FIELD_VERSION);
        if (version == null) {
            throw new DpException(
                    "schema version marker has no '" + FIELD_VERSION + "' field; the marker "
                            + "document is corrupt and must be repaired manually");
        }
        return new Claim(token, version.intValue());
    }

    private static String newClaimToken() {
        return UUID.randomUUID().toString();
    }

    private static Bson heldBy(Claim claim) {
        return Filters.and(
                Filters.eq(FIELD_ID, MARKER_ID),
                Filters.eq(FIELD_CLAIM_TOKEN, claim.token()));
    }

    /**
     * Says why a marker write filtered on {@code claim}'s token matched nothing, as the exception
     * to throw.
     *
     * <p>Three states are possible and they call for different responses. The marker is gone:
     * {@code goneMessage}, the pre-lease error. The marker is held under another token: this
     * process was displaced by a takeover. The marker has no token: the claim was cleared or
     * released while this process held it, which is an operator running the manual clear against a
     * holder that was alive.
     */
    private static DpException claimWriteMatchedNothing(
            MongoDatabase database, Claim claim, String action, String goneMessage) {

        final Document marker;
        try {
            marker = collection(database).find(Filters.eq(FIELD_ID, MARKER_ID)).first();
        } catch (MongoException ex) {
            return new DpException(
                    "could not " + action + ": the marker write matched nothing, and reading the "
                            + "marker to find out why failed: " + ex.getMessage(), ex);
        }

        if (marker == null) {
            return new DpException(goneMessage);
        }

        final String currentToken = readString(marker, FIELD_CLAIM_TOKEN);
        if (currentToken == null) {
            return new ClaimLostException(
                    "could not " + action + ": the migration claim was cleared or released while "
                            + "this process held it, most likely by the manual clear in "
                            + "doc/runbooks/schema-migration.md. This process has stopped migrating "
                            + "and has written nothing further to the marker. An operation it "
                            + "already sent to the server may still be running; a restart resumes "
                            + "from the recorded schema version.");
        }
        if (currentToken.equals(claim.token())) {
            return new DpException(
                    "could not " + action + ": the marker write matched nothing although the "
                            + "marker still carries this process's claim token; retry by restarting");
        }
        return new ClaimLostException(
                "could not " + action + ": another process ("
                        + readString(marker, FIELD_MIGRATING_HOST) + ") took the schema migration "
                        + "over from this one, whose heartbeat it had stopped seeing. This process "
                        + "has stopped migrating and has written nothing further to the marker. An "
                        + "operation it already sent to the server may still be running; the "
                        + "process that took over is responsible for the migration now.");
    }

    /**
     * Creates the marker for a legacy database — one holding data but predating this mechanism — at
     * {@link #VERSION_UNMANAGED}, so that the claim protocol has a document to operate on.
     *
     * @return true if this call created the marker, false if one already existed
     */
    public static boolean createUnmanagedMarker(MongoDatabase database) throws DpException {
        return stampFresh(database, VERSION_UNMANAGED);
    }

    /**
     * Records one successfully applied migration, advancing the version and appending to the audit
     * list, while retaining the migration claim.
     *
     * <p>The version advances per migration rather than once at the end of the run, so a crash
     * partway through a multi-migration upgrade leaves the version at the last migration that
     * actually completed. Recording only at the end would make a crash re-run migrations that had
     * already succeeded — safe only because {@link Migration} requires idempotency, and not worth
     * depending on when the narrower record is this cheap.
     *
     * <p>An advanced version is progress, so this also resets {@code takeoverCount}: the cap on
     * takeovers is per version.
     *
     * @throws ClaimLostException if the claim is no longer this process's
     */
    public static void recordApplied(
            MongoDatabase database, Claim claim, int version, String description) throws DpException {

        final Document applied = new Document(FIELD_APPLIED_VERSION, version)
                .append(FIELD_APPLIED_DESCRIPTION, description)
                .append(FIELD_APPLIED_AT, Instant.now());

        final UpdateResult result;
        try {
            result = collection(database).updateOne(
                    heldBy(claim),
                    Updates.combine(
                            Updates.set(FIELD_VERSION, version),
                            Updates.set(FIELD_UPDATED_AT, Instant.now()),
                            Updates.set(FIELD_TAKEOVER_COUNT, 0),
                            Updates.push(FIELD_APPLIED_MIGRATIONS, applied)));
        } catch (MongoException ex) {
            throw new DpException(
                    "error recording applied migration version " + version + ": " + ex.getMessage(), ex);
        }

        // Test matchedCount, per the repo convention for a filtered update that must find its
        // target. Matching nothing means the version advance is silently lost: the migration has
        // been applied to the data but nothing records it. Left unreported, the runner would go on
        // to the next migration and finally report success. Throwing stops the run. Which exception
        // depends on why nothing matched: a marker that is gone is a schema whose recorded state no
        // longer matches its data; a marker under another token is a holder that was displaced.
        if (result.getMatchedCount() == 0) {
            throw claimWriteMatchedNothing(
                    database, claim, "record schema migration version " + version,
                    "schema migration version " + version + " was applied but could not be "
                            + "recorded: the marker document is gone. It was deleted or "
                            + "replaced while this process held the migration claim. The "
                            + "database now holds migrated data with no record of it; inspect "
                            + "the schema and restore the marker manually before restarting. "
                            + "See doc/runbooks/schema-migration.md.");
        }
    }

    /**
     * Releases the migration claim, removing every lease field. Called after a successful run;
     * deliberately <b>not</b> called after a failed one, so that a database left in an unknown
     * state blocks subsequent startups until an operator has looked at it.
     *
     * @throws ClaimLostException if the claim is no longer this process's
     */
    public static void releaseClaim(MongoDatabase database, Claim claim) throws DpException {

        final UpdateResult result;
        try {
            result = collection(database).updateOne(
                    heldBy(claim),
                    Updates.combine(
                            Updates.set(FIELD_MIGRATING, false),
                            Updates.unset(FIELD_MIGRATING_SINCE),
                            Updates.unset(FIELD_MIGRATING_HOST),
                            Updates.unset(FIELD_CLAIM_TOKEN),
                            Updates.unset(FIELD_HEARTBEAT_SEQ),
                            Updates.unset(FIELD_MIGRATING_HEARTBEAT),
                            Updates.unset(FIELD_TAKEOVER_COUNT)));
        } catch (MongoException ex) {
            throw new DpException("error releasing schema migration claim: " + ex.getMessage(), ex);
        }

        // Matching nothing with the marker gone throws rather than merely logging: the migration
        // itself succeeded, but there is now no authoritative marker, so every other starting
        // process is polling for a state that will never appear and this process cannot assert the
        // schema is what it needs. Reporting success would leave the deployment coordinating on
        // nothing.
        if (result.getMatchedCount() == 0) {
            throw claimWriteMatchedNothing(
                    database, claim, "release the schema migration claim",
                    "schema migration completed but the claim could not be released: the marker "
                            + "document is gone. It was deleted or replaced while this process "
                            + "held the claim, so the recorded schema state is lost and other "
                            + "starting processes have nothing to coordinate on. Verify the "
                            + "schema and restore the marker before restarting. See "
                            + "doc/runbooks/schema-migration.md.");
        }
    }

    /**
     * Shows the claim's holder to be alive: increments {@code heartbeatSeq}, which is what a
     * waiting process watches, and sets {@code migratingHeartbeat} to server time for an operator
     * to read.
     *
     * @throws ClaimLostException if the claim is no longer this process's
     */
    public static void heartbeat(MongoDatabase database, Claim claim) throws DpException {

        final UpdateResult result;
        try {
            result = collection(database).updateOne(
                    heldBy(claim),
                    Updates.combine(
                            Updates.inc(FIELD_HEARTBEAT_SEQ, 1L),
                            Updates.currentDate(FIELD_MIGRATING_HEARTBEAT)));
        } catch (MongoException ex) {
            throw new DpException(
                    "error writing schema migration heartbeat: " + ex.getMessage(), ex);
        }

        if (result.getMatchedCount() == 0) {
            throw claimWriteMatchedNothing(
                    database, claim, "write the schema migration heartbeat",
                    "the schema migration heartbeat could not be written: the marker document is "
                            + "gone. It was deleted or replaced while this process held the "
                            + "migration claim. See doc/runbooks/schema-migration.md.");
        }
    }

    /**
     * Marks the held claim as failed, so that no process takes it over: a migration that threw
     * would throw again, and the database is partway through an ordered set of changes that an
     * operator must look at.
     *
     * <p>The message is stored for the operator and for the waiting processes' error text. It is
     * passed through {@link SecretRedactor} (a driver message can quote the connection string) and
     * truncated.
     *
     * @throws ClaimLostException if the claim is no longer this process's
     */
    public static void markFailed(
            MongoDatabase database, Claim claim, int version, String message) throws DpException {

        String stored = SecretRedactor.redact(message == null ? "" : message);
        if (stored.length() > MIGRATION_FAILED_MESSAGE_MAX_LENGTH) {
            stored = stored.substring(0, MIGRATION_FAILED_MESSAGE_MAX_LENGTH);
        }

        final UpdateResult result;
        try {
            result = collection(database).updateOne(
                    heldBy(claim),
                    Updates.combine(
                            Updates.set(FIELD_MIGRATION_FAILED, true),
                            Updates.set(FIELD_MIGRATION_FAILED_VERSION, version),
                            Updates.set(FIELD_MIGRATION_FAILED_MESSAGE, stored)));
        } catch (MongoException ex) {
            throw new DpException(
                    "error recording failure of schema migration version " + version + ": "
                            + ex.getMessage(), ex);
        }

        if (result.getMatchedCount() == 0) {
            throw claimWriteMatchedNothing(
                    database, claim, "record the failure of schema migration version " + version,
                    "the failure of schema migration version " + version + " could not be "
                            + "recorded: the marker document is gone. It was deleted or replaced "
                            + "while this process held the migration claim. See "
                            + "doc/runbooks/schema-migration.md.");
        }
    }

    /**
     * Returns the operations of any migration claim that are running on the server against
     * {@code databaseName}: those whose comment begins with
     * {@link MigrationContext#OPERATION_COMMENT_PREFIX}.
     *
     * <p>A client that is killed does not take its operation with it. Measured on 8.0.6: an
     * {@code updateMany} and a {@code $group}/{@code $merge} aggregate whose clients were killed
     * with {@code kill -9} both ran to completion. A process that took over a claim without looking
     * would start the same scan beside the one still running, once per kill.
     *
     * <p>{@code allUsers: false} reports the operations of the connecting user only, which needs no
     * privilege beyond the service's own ({@code allUsers: true} needs {@code inprog}); every
     * dp-service process connects as the same user, so that is sufficient. {@code $currentOp} must
     * be run on the {@code admin} database, which is why this takes that handle.
     *
     * <p><b>An operation with no comment is invisible here.</b> In driver 5.4.0 {@code dropIndexes},
     * {@code drop} and {@code createIndexes} cannot carry one. The first two finish at once. An
     * index build does not, and survives its client; a migration that needs one needs its own
     * answer to this before it ships.
     *
     * @throws DpException if the check could not be run. The caller must then refuse the takeover,
     *     not proceed without the check.
     */
    public static List<OrphanedOperation> findOrphanedOperations(
            MongoDatabase adminDatabase, String databaseName) throws DpException {

        final List<Bson> pipeline = List.of(
                new Document("$currentOp", new Document("allUsers", false)),
                new Document("$match", new Document()
                        .append("command.comment", new Document(
                                "$regex",
                                "^" + Pattern.quote(MigrationContext.OPERATION_COMMENT_PREFIX)))
                        .append("ns", new Document(
                                "$regex", "^" + Pattern.quote(databaseName + ".")))));

        final List<OrphanedOperation> operations = new ArrayList<>();
        try {
            for (Document operation : adminDatabase.aggregate(pipeline)) {
                final Number runningSeconds = readNumber(operation, "secs_running");
                final Object comment =
                        operation.get("command") instanceof Document command
                                ? command.get("comment") : null;
                operations.add(new OrphanedOperation(
                        String.valueOf(comment),
                        String.valueOf(operation.get("ns")),
                        // a number on a standalone or replica set, "shard:number" through mongos
                        String.valueOf(operation.get("opid")),
                        runningSeconds == null ? 0L : runningSeconds.longValue()));
            }
        } catch (MongoException ex) {
            throw new DpException(
                    "error checking for running schema migration operations ($currentOp): "
                            + ex.getMessage(), ex);
        }
        return operations;
    }

    /** Returns the versions recorded in the marker's audit list, oldest first. */
    public static List<Integer> readAppliedVersions(MongoDatabase database) throws DpException {

        final Document marker;
        try {
            marker = collection(database).find(Filters.eq(FIELD_ID, MARKER_ID)).first();
        } catch (MongoException ex) {
            throw new DpException("error reading schema version marker: " + ex.getMessage(), ex);
        }

        final List<Integer> versions = new ArrayList<>();
        if (marker == null) {
            return versions;
        }

        final List<?> applied = marker.getList(FIELD_APPLIED_MIGRATIONS, Object.class);
        if (applied == null) {
            return versions;
        }
        for (Object entry : applied) {
            if (entry instanceof Document doc) {
                final Integer version = doc.getInteger(FIELD_APPLIED_VERSION);
                if (version != null) {
                    versions.add(version);
                }
            }
        }
        return versions;
    }

    private static boolean isDuplicateKey(MongoException ex) {
        return ex.getCode() == 11000;
    }
}
