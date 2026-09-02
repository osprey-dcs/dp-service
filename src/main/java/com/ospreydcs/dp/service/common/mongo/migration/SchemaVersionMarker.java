package com.ospreydcs.dp.service.common.mongo.migration;

import com.mongodb.MongoException;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.FindOneAndUpdateOptions;
import com.mongodb.client.model.ReturnDocument;
import com.mongodb.client.model.Updates;
import com.ospreydcs.dp.service.common.exception.DpException;
import org.bson.Document;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

/**
 * Reads and writes the single marker document recording which schema version a database is at.
 *
 * <p>Structurally this follows {@code BucketSpanVerifier}'s marker: one document with a fixed
 * {@code _id} in a dedicated collection, so the collection holds exactly one record. What it
 * deliberately does <b>not</b> follow is that class's unsynchronized read-check-write. That is safe
 * there because its work is idempotent and read-only and its failure mode is a lost optimization;
 * a migration is neither. Every service process runs {@code MongoClientBase.init()}, and the
 * documented deployment starts three of them, so two processes reaching the runner at once is the
 * normal case rather than an edge case.
 *
 * <p>Coordination therefore uses {@link #claimForMigration}, whose conditional
 * {@code findOneAndUpdate} is atomic at the single-document level. That is the only primitive
 * available: multi-document transactions require a replica set, which the deployment does not
 * guarantee.
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

    static final String FIELD_APPLIED_VERSION = "version";
    static final String FIELD_APPLIED_DESCRIPTION = "description";
    static final String FIELD_APPLIED_AT = "appliedAt";

    /**
     * The version a database is at when no marker exists and the database holds data — i.e. a
     * deployment that predates this mechanism. Migration versions start at 1.
     */
    public static final int VERSION_UNMANAGED = 0;

    /** State of the marker as read from the database. */
    public record MarkerState(boolean present, int version, boolean migrating,
                              Instant migratingSince, String migratingHost) {

        public static MarkerState absent() {
            return new MarkerState(false, VERSION_UNMANAGED, false, null, null);
        }
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

        final Boolean migrating = marker.getBoolean(FIELD_MIGRATING);
        return new MarkerState(
                true,
                version,
                migrating != null && migrating,
                readInstant(marker, FIELD_MIGRATING_SINCE),
                marker.getString(FIELD_MIGRATING_HOST));
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
     * @return true if the claim was acquired by this caller
     */
    public static boolean claimForMigration(
            MongoDatabase database, int observedVersion, String host) throws DpException {

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
                            Updates.set(FIELD_MIGRATING_HOST, host)),
                    new FindOneAndUpdateOptions().returnDocument(ReturnDocument.AFTER));
        } catch (MongoException ex) {
            throw new DpException("error claiming schema migration: " + ex.getMessage(), ex);
        }

        return claimed != null;
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
     */
    public static void recordApplied(
            MongoDatabase database, int version, String description) throws DpException {

        final Document applied = new Document(FIELD_APPLIED_VERSION, version)
                .append(FIELD_APPLIED_DESCRIPTION, description)
                .append(FIELD_APPLIED_AT, Instant.now());

        try {
            collection(database).updateOne(
                    Filters.eq(FIELD_ID, MARKER_ID),
                    Updates.combine(
                            Updates.set(FIELD_VERSION, version),
                            Updates.set(FIELD_UPDATED_AT, Instant.now()),
                            Updates.push(FIELD_APPLIED_MIGRATIONS, applied)));
        } catch (MongoException ex) {
            throw new DpException(
                    "error recording applied migration version " + version + ": " + ex.getMessage(), ex);
        }
    }

    /**
     * Releases the migration claim. Called after a successful run; deliberately <b>not</b> called
     * after a failed one, so that a database left in an unknown state blocks subsequent startups
     * until an operator has looked at it.
     */
    public static void releaseClaim(MongoDatabase database) throws DpException {
        try {
            collection(database).updateOne(
                    Filters.eq(FIELD_ID, MARKER_ID),
                    Updates.combine(
                            Updates.set(FIELD_MIGRATING, false),
                            Updates.unset(FIELD_MIGRATING_SINCE),
                            Updates.unset(FIELD_MIGRATING_HOST)));
        } catch (MongoException ex) {
            throw new DpException("error releasing schema migration claim: " + ex.getMessage(), ex);
        }
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
