package com.ospreydcs.dp.service.common.mongo.migration;

import com.ospreydcs.dp.service.common.exception.DpException;

/**
 * A marker write by a process that believed it held the migration claim matched nothing, and the
 * marker is still there: the claim now belongs to another process, or was cleared by an operator
 * (issue #292).
 *
 * <p>It is a separate type because the caller must treat it differently from a migration failure.
 * A displaced holder stops and writes nothing more to the marker; in particular it must not record
 * {@code migrationFailed}, which would block the process that took the migration over.
 */
public class ClaimLostException extends DpException {

    public ClaimLostException(String message) {
        super(message);
    }
}
