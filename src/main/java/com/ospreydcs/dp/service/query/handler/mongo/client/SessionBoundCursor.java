package com.ospreydcs.dp.service.query.handler.mongo.client;

import com.mongodb.ServerAddress;
import com.mongodb.ServerCursor;
import com.mongodb.client.ClientSession;
import com.mongodb.client.MongoCursor;

/**
 * A cursor that owns the explicit session its finds were issued in, and ends that session when it
 * is closed (issue #314).
 *
 * <p>The time-ordered samples retrieval holds several server cursors open for as long as the
 * response takes, and reads each only when the merge reaches its next bucket, so one of them can
 * go unread for a long time. The server does not apply its idle-cursor timeout to a cursor that
 * belongs to a session; it kills the cursor when the <em>session</em> expires (30 minutes without
 * use by default), and {@code noCursorTimeout} does not prevent that. Measured on MongoDB 8.0.6
 * with the timeouts shortened: an unread cursor in its own implicit session was gone after the
 * session timeout with or without {@code noCursorTimeout}, while an unread cursor sharing a
 * session with one that kept reading survived. Each driver cursor otherwise gets its own implicit
 * session, so the retrieval issues all its finds in one explicit session: any {@code getMore} on
 * any of them keeps every one alive.
 *
 * <p>The session is ended after the cursors are closed, in a {@code finally}, so a failed cursor
 * close cannot leak it.
 */
final class SessionBoundCursor<T> implements MongoCursor<T> {

    private final MongoCursor<T> delegate;
    private final ClientSession session;

    SessionBoundCursor(MongoCursor<T> delegate, ClientSession session) {
        this.delegate = delegate;
        this.session = session;
    }

    @Override
    public void close() {
        try {
            delegate.close();
        } finally {
            session.close();
        }
    }

    @Override
    public boolean hasNext() {
        return delegate.hasNext();
    }

    @Override
    public T next() {
        return delegate.next();
    }

    @Override
    public T tryNext() {
        return delegate.tryNext();
    }

    @Override
    public int available() {
        return delegate.available();
    }

    @Override
    public ServerCursor getServerCursor() {
        return delegate.getServerCursor();
    }

    @Override
    public ServerAddress getServerAddress() {
        return delegate.getServerAddress();
    }

    @Override
    public void remove() {
        delegate.remove();
    }
}
