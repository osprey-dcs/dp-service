package com.ospreydcs.dp.service.common.mongo;

import com.mongodb.ServerAddress;
import com.mongodb.ServerCursor;
import com.mongodb.client.MongoCursor;

import java.util.function.Consumer;

/**
 * A {@link MongoCursor} decorator that accumulates the time spent inside the cursor and counts the
 * documents read (issue #212, D3).
 *
 * <p>This is how the query pipeline's {@code db} stage is measured. Opening a cursor issues the
 * find but retrieves only the first batch; every subsequent batch is a {@code getMore} round-trip
 * that happens inside {@link #hasNext()} or {@link #next()}, interleaved with the assembly work the
 * dispatcher does between documents. Timing only the {@code executeQuery*()} call would therefore
 * attribute all but the first batch of a large result to {@code process} — on a query returning
 * tens of thousands of buckets, nearly all of the database time would land in the wrong stage, and
 * an operator would conclude the service was CPU-bound when it was waiting on Mongo.
 *
 * <p><b>The accumulated time is an upper bound on database time, not a measurement of it.</b> The
 * driver's cursor has a prefetched batch in hand, so most {@code next()} calls return from memory
 * and contribute microseconds; the calls that actually block are the batch boundaries. What is
 * measured here is "time the dispatcher spent inside the cursor", which includes the driver's
 * BSON decode of each returned document. That decode is genuinely attributable to retrieval rather
 * than to assembly, so it belongs in this stage — but it means the {@code db} stage is not purely
 * server time. {@code db.client.operation.duration} (D4) is the server-side view, and the two
 * together separate "the server is slow" from "we are decoding a great deal".
 *
 * <p><b>Thread-safety follows the wrapped cursor's.</b> The counters are plain fields, not atomics,
 * because a {@code MongoCursor} is not safe for concurrent use and every caller here drives one
 * cursor from one thread at a time. {@code QueryDataBidiStreamDispatcher} is the one caller that
 * touches its cursor from more than one thread, and it holds a lock across every cursor operation
 * (its {@code cursorLock}); those same lock acquisitions publish these fields, so the decorator
 * needs no synchronization the cursor does not already have.
 */
public class TimedMongoCursor<T> implements MongoCursor<T> {

    // instance variables
    private final MongoCursor<T> delegate;
    private long elapsedNanos = 0;
    private long documentCount = 0;

    public TimedMongoCursor(MongoCursor<T> delegate) {
        this.delegate = delegate;
    }

    /** Nanos accumulated across every cursor call so far; see the class note on what it includes. */
    public long elapsedNanos() {
        return elapsedNanos;
    }

    /** Documents actually returned by {@link #next()} and {@link #tryNext()}. */
    public long documentCount() {
        return documentCount;
    }

    @Override
    public boolean hasNext() {
        final long startNanos = System.nanoTime();
        try {
            return delegate.hasNext();
        } finally {
            // In a finally so that a MongoException thrown mid-iteration still contributes the time
            // it took to fail. Otherwise a query that times out against the server -- exactly the
            // case an operator is investigating -- would report a db stage of zero.
            elapsedNanos += System.nanoTime() - startNanos;
        }
    }

    @Override
    public T next() {
        final long startNanos = System.nanoTime();
        try {
            final T result = delegate.next();
            documentCount++;
            return result;
        } finally {
            elapsedNanos += System.nanoTime() - startNanos;
        }
    }

    @Override
    public T tryNext() {
        final long startNanos = System.nanoTime();
        try {
            final T result = delegate.tryNext();
            if (result != null) {
                documentCount++;
            }
            return result;
        } finally {
            elapsedNanos += System.nanoTime() - startNanos;
        }
    }

    /**
     * Closes the wrapped cursor, timing it: closing a cursor the caller did not exhaust sends a
     * {@code killCursors} to the server, which is a round-trip and belongs in the db stage. The
     * unary bucket dispatcher stops at its page size and closes a live cursor on every paged query,
     * so this is the common path, not an edge case.
     */
    @Override
    public void close() {
        final long startNanos = System.nanoTime();
        try {
            delegate.close();
        } finally {
            elapsedNanos += System.nanoTime() - startNanos;
        }
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

    /**
     * Delegated rather than inherited. {@code Iterator} supplies a default {@code remove()} that
     * throws {@code UnsupportedOperationException}, and the driver's cursors override it -- so
     * inheriting the default would change behavior for any caller that reaches it, silently, in a
     * class whose whole purpose is to be transparent.
     */
    @Override
    public void remove() {
        delegate.remove();
    }

    /**
     * Delegated to {@code MongoCursor}'s default implementation via the timed {@code hasNext()} and
     * {@code next()} above rather than to the wrapped cursor's, so the documents it consumes are
     * counted and timed. Calling {@code delegate.forEachRemaining} would drain the cursor entirely
     * outside this decorator's view -- the db stage would read as zero for that portion of the
     * result, which is the failure this class exists to prevent.
     */
    @Override
    public void forEachRemaining(Consumer<? super T> action) {
        while (hasNext()) {
            action.accept(next());
        }
    }
}
