package com.ospreydcs.dp.service.common.mongo.migration;

import com.mongodb.client.MongoDatabase;
import com.ospreydcs.dp.service.common.exception.DpException;

/**
 * One versioned, ordered change to the stored schema.
 *
 * <p><b>Implementations must operate on {@link MongoDatabase} and raw {@code Document}s, never on
 * the POJO document classes.</b> The codec registry is bound to the <i>current</i> shape of those
 * classes, while a migration by definition reads documents written under a <i>previous</i> one —
 * deserializing an unmigrated record through a codec that expects the migrated field is exactly the
 * failure the migration exists to prevent. It also keeps a migration stable over time: one written
 * today must still work after the document classes have moved on, which it can only do if it never
 * references them.
 *
 * <p><b>Implementations must be idempotent.</b> The version marker makes a re-run unlikely but not
 * impossible: a process can crash after applying a migration and before recording the version, and
 * the operator recovery path for a stuck claim can re-run it. State the reason each implementation
 * is idempotent in its class Javadoc — the runner cannot enforce this.
 *
 * <p><b>Idempotent is not enough: an implementation must be safe to run late and concurrently
 * (issue #292).</b> A claim whose holder stops showing a heartbeat is taken over. If that holder
 * was stalled, not dead, its operation and the new holder's run at once; and its migration N can
 * finish after the new holder has applied N+1 and the services have started serving. Only its
 * marker writes are fenced, not its data operations. So every write a migration makes must be
 * harmless against data that has moved on since the migration read it: filter each write on the
 * state it was computed from (an absent field, the value that was read), or make it monotone
 * ({@code $max}). A read-then-write filtered on {@code _id} alone overwrites whatever a save path
 * stored in between, with no error. State why the implementation meets this in its class Javadoc.
 *
 * <p><b>Every operation must carry {@link MigrationContext#operationComment()}</b>, on every driver
 * call that accepts a comment. That is how a process about to take over sees that an operation of
 * a previous holder is still running on the server (a killed client's operation runs to
 * completion), and waits for it. An operation without the comment is invisible to that check.
 * {@code MigrationOperationCommentTest} fails a migration that issues an uncommented command
 * outside a short exempt set; a long-running operation that cannot carry a comment
 * ({@code createIndexes} in this driver) needs its own design before it ships.
 */
public interface Migration {

    /** Schema version this migration produces. Contiguous from 1; see {@code SchemaMigrationRunner}. */
    int version();

    /** Short human-readable summary, recorded in the marker's audit list. */
    String description();

    /**
     * Applies the change.
     *
     * @param context carries the comment every operation must be issued with
     * @throws DpException if the change could not be completed. The runner treats this as fatal and
     *     leaves the migration claim in place, so the database is not silently used in an unknown
     *     state.
     */
    void apply(MongoDatabase database, MigrationContext context) throws DpException;
}
