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
 */
public interface Migration {

    /** Schema version this migration produces. Contiguous from 1; see {@code SchemaMigrationRunner}. */
    int version();

    /** Short human-readable summary, recorded in the marker's audit list. */
    String description();

    /**
     * Applies the change.
     *
     * @throws DpException if the change could not be completed. The runner treats this as fatal and
     *     leaves the migration claim in place, so the database is not silently used in an unknown
     *     state.
     */
    void apply(MongoDatabase database) throws DpException;
}
