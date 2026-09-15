package com.ospreydcs.dp.service.common.mongo;

import com.mongodb.MongoException;
import com.mongodb.ServerAddress;
import com.mongodb.ServerCursor;
import com.mongodb.client.MongoCursor;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Covers the cursor decorator that measures the query pipeline's {@code db} stage (issue #212, D3).
 *
 * <p>Driven against a fake cursor rather than a real one, because what needs pinning is the
 * decorator's arithmetic and its delegation — that it counts every document, accumulates time from
 * every entry point including the ones that fail, and does not quietly bypass itself. A real cursor
 * could not produce the mid-iteration failure, and its timings would be too small to assert on.
 */
@RunWith(JUnit4.class)
public class TimedMongoCursorTest {

    /** A cursor over a list, optionally sleeping per call and optionally throwing at an index. */
    private static class FakeCursor implements MongoCursor<String> {

        private final Iterator<String> iterator;
        private final long sleepMillis;
        private final int throwAtIndex;
        private int index = 0;
        private final AtomicBoolean closed = new AtomicBoolean(false);
        private boolean forEachRemainingCalled = false;

        FakeCursor(List<String> values, long sleepMillis, int throwAtIndex) {
            this.iterator = values.iterator();
            this.sleepMillis = sleepMillis;
            this.throwAtIndex = throwAtIndex;
        }

        private void work() {
            if (sleepMillis > 0) {
                try {
                    Thread.sleep(sleepMillis);
                } catch (InterruptedException ex) {
                    Thread.currentThread().interrupt();
                }
            }
            if (throwAtIndex >= 0 && index == throwAtIndex) {
                throw new MongoException("deliberate mid-iteration failure");
            }
            index++;
        }

        @Override
        public boolean hasNext() {
            work();
            return iterator.hasNext();
        }

        @Override
        public String next() {
            work();
            if (!iterator.hasNext()) {
                throw new NoSuchElementException();
            }
            return iterator.next();
        }

        @Override
        public String tryNext() {
            work();
            return iterator.hasNext() ? iterator.next() : null;
        }

        @Override
        public void close() {
            work();
            closed.set(true);
        }

        @Override
        public int available() {
            return 0;
        }

        @Override
        public ServerCursor getServerCursor() {
            return null;
        }

        @Override
        public ServerAddress getServerAddress() {
            return new ServerAddress("localhost", 27017);
        }

        @Override
        public void forEachRemaining(java.util.function.Consumer<? super String> action) {
            // Records the call so the decorator can be proven not to delegate here; a delegated
            // forEachRemaining would drain the cursor outside the decorator's view and the db stage
            // would read as zero for that portion of the result.
            forEachRemainingCalled = true;
            MongoCursor.super.forEachRemaining(action);
        }

        boolean isClosed() {
            return closed.get();
        }
    }

    private static final List<String> VALUES = List.of("a", "b", "c");

    /** Documents returned by {@code next()} are counted and the time is accumulated. */
    @Test
    public void testCountsDocumentsAndAccumulatesTime() {

        final FakeCursor delegate = new FakeCursor(VALUES, 10, -1);
        final TimedMongoCursor<String> cursor = new TimedMongoCursor<>(delegate);

        assertEquals(0, cursor.documentCount());
        assertEquals(0, cursor.elapsedNanos());

        final List<String> read = new ArrayList<>();
        while (cursor.hasNext()) {
            read.add(cursor.next());
        }

        assertEquals(VALUES, read);
        assertEquals(3, cursor.documentCount());
        // 3 next() + 4 hasNext() calls at 10ms each, so comfortably above 50ms and below a second
        assertTrue(
                "elapsed " + cursor.elapsedNanos() + "ns is implausibly small",
                cursor.elapsedNanos() > 50_000_000L);
        assertTrue(
                "elapsed " + cursor.elapsedNanos() + "ns is implausibly large",
                cursor.elapsedNanos() < 5_000_000_000L);
    }

    /** {@code tryNext()} counts a returned document and does not count a null. */
    @Test
    public void testTryNextCountsOnlyReturnedDocuments() {

        final TimedMongoCursor<String> cursor =
                new TimedMongoCursor<>(new FakeCursor(List.of("only"), 0, -1));

        assertNotNull(cursor.tryNext());
        assertEquals(1, cursor.documentCount());

        assertNull(cursor.tryNext());
        assertEquals("a null tryNext was counted as a document", 1, cursor.documentCount());
    }

    /**
     * A failure mid-iteration still contributes the time it took to fail. Without the
     * {@code finally}, a query that times out against the server — exactly the case an operator is
     * investigating — would report a db stage of zero.
     */
    @Test
    public void testFailureStillContributesItsTime() {

        final FakeCursor delegate = new FakeCursor(VALUES, 20, 1);
        final TimedMongoCursor<String> cursor = new TimedMongoCursor<>(delegate);

        assertTrue(cursor.hasNext());
        final long afterFirstCall = cursor.elapsedNanos();
        assertTrue(afterFirstCall > 0);

        try {
            cursor.next();
            fail("the fake cursor did not throw");
        } catch (MongoException expected) {
            // the throw is the point
        }

        assertTrue(
                "the failing call contributed no time",
                cursor.elapsedNanos() > afterFirstCall);
        assertEquals("a failed next() must not count a document", 0, cursor.documentCount());
    }

    /**
     * Closing is timed: closing a cursor the caller did not exhaust sends {@code killCursors},
     * which is a round-trip and belongs in the db stage. The unary bucket dispatcher closes a live
     * cursor on every paged query, so this is the common path.
     */
    @Test
    public void testCloseIsTimedAndDelegated() {

        final FakeCursor delegate = new FakeCursor(VALUES, 15, -1);
        final TimedMongoCursor<String> cursor = new TimedMongoCursor<>(delegate);

        assertEquals(0, cursor.elapsedNanos());
        cursor.close();

        assertTrue(delegate.isClosed());
        assertTrue("close contributed no time", cursor.elapsedNanos() > 0);
    }

    /**
     * {@code forEachRemaining} must run through this decorator's own {@code hasNext}/{@code next},
     * not the wrapped cursor's. Delegating it would drain the cursor entirely outside the
     * decorator's view: the documents would be uncounted and the db stage would read as zero for
     * that portion of the result — the failure the class exists to prevent, and one that produces a
     * plausible-looking number rather than an error.
     */
    @Test
    public void testForEachRemainingIsCountedAndTimed() {

        final FakeCursor delegate = new FakeCursor(VALUES, 5, -1);
        final TimedMongoCursor<String> cursor = new TimedMongoCursor<>(delegate);

        final List<String> read = new ArrayList<>();
        cursor.forEachRemaining(read::add);

        assertEquals(VALUES, read);
        assertEquals("documents drained by forEachRemaining were not counted", 3, cursor.documentCount());
        assertTrue(cursor.elapsedNanos() > 0);
        assertFalse(
                "forEachRemaining was delegated to the wrapped cursor",
                delegate.forEachRemainingCalled);
    }

    /** The pass-through accessors reach the wrapped cursor. */
    @Test
    public void testAccessorsDelegate() {

        final FakeCursor delegate = new FakeCursor(VALUES, 0, -1);
        final TimedMongoCursor<String> cursor = new TimedMongoCursor<>(delegate);

        assertEquals(0, cursor.available());
        assertNull(cursor.getServerCursor());
        assertEquals(new ServerAddress("localhost", 27017), cursor.getServerAddress());
    }
}
