package com.ospreydcs.dp.service.query.handler.mongo.client;

import com.mongodb.MongoException;
import com.mongodb.ServerAddress;
import com.mongodb.ServerCursor;
import com.mongodb.client.MongoCursor;
import com.ospreydcs.dp.service.common.bson.DataTimestampsDocument;
import com.ospreydcs.dp.service.common.bson.TimestampDocument;
import com.ospreydcs.dp.service.common.bson.bucket.BucketDocument;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

/**
 * Pins the merge order and lifecycle of {@link MergedBucketCursor} (issue #274, plan D11) with
 * list-backed fake cursors: interleaved PV names across inputs come out in
 * {@code (pvName, firstTime.seconds, firstTime.nanos)} order, empty inputs are harmless, nothing
 * is read before the first {@code hasNext()}, and {@code close()} closes every input.
 */
public class MergedBucketCursorTest {

    /** A cursor over a fixed list that records whether it was closed and how far it was read. */
    private static final class ListCursor implements MongoCursor<BucketDocument> {
        private final Iterator<BucketDocument> iterator;
        int reads = 0;
        boolean closed = false;

        ListCursor(List<BucketDocument> documents) {
            this.iterator = documents.iterator();
        }

        @Override public boolean hasNext() { return iterator.hasNext(); }
        @Override public BucketDocument next() { reads++; return iterator.next(); }
        @Override public BucketDocument tryNext() { return hasNext() ? next() : null; }
        @Override public void close() { closed = true; }
        @Override public int available() { return 0; }
        @Override public ServerCursor getServerCursor() { return null; }
        @Override public ServerAddress getServerAddress() { return new ServerAddress(); }
    }

    private static BucketDocument bucket(String pvName, long seconds, long nanos) {
        final BucketDocument bucket = new BucketDocument();
        bucket.setPvName(pvName);
        final TimestampDocument first = new TimestampDocument();
        first.setSeconds(seconds);
        first.setNanos(nanos);
        final DataTimestampsDocument timestamps = new DataTimestampsDocument();
        timestamps.setFirstTime(first);
        bucket.setDataTimestamps(timestamps);
        return bucket;
    }

    private static String key(BucketDocument bucket) {
        return bucket.getPvName() + "@" + bucket.getDataTimestamps().getFirstTime().getSeconds()
                + "." + bucket.getDataTimestamps().getFirstTime().getNanos();
    }

    private static List<String> drain(MongoCursor<BucketDocument> cursor) {
        final List<String> keys = new ArrayList<>();
        while (cursor.hasNext()) {
            keys.add(key(cursor.next()));
        }
        return keys;
    }

    @Test
    public void testMergesInterleavedPvsInSortOrder() {
        // classes hold disjoint PV sets; names interleave alphabetically across the inputs
        final ListCursor classA = new ListCursor(List.of(
                bucket("pv_a", 10, 0), bucket("pv_a", 11, 0), bucket("pv_c", 10, 0)));
        final ListCursor classB = new ListCursor(List.of(
                bucket("pv_b", 9, 500), bucket("pv_b", 9, 900), bucket("pv_d", 1, 0)));
        final ListCursor classC = new ListCursor(List.of(
                bucket("pv_ab", 10, 0)));

        final List<String> merged = drain(new MergedBucketCursor(List.of(classA, classB, classC)));
        assertEquals(List.of(
                "pv_a@10.0", "pv_a@11.0", "pv_ab@10.0",
                "pv_b@9.500", "pv_b@9.900", "pv_c@10.0", "pv_d@1.0"), merged);
    }

    @Test
    public void testEmptyInputsAndSingleInput() {
        final ListCursor empty = new ListCursor(List.of());
        final ListCursor only = new ListCursor(List.of(bucket("pv", 1, 0), bucket("pv", 2, 0)));
        assertEquals(List.of("pv@1.0", "pv@2.0"), drain(new MergedBucketCursor(List.of(empty, only, new ListCursor(List.of())))));

        final MergedBucketCursor allEmpty = new MergedBucketCursor(List.of(new ListCursor(List.of())));
        assertFalse(allEmpty.hasNext());
        assertThrows(NoSuchElementException.class, allEmpty::next);
    }

    @Test
    public void testNothingIsReadBeforeFirstHasNext() {
        final ListCursor input = new ListCursor(List.of(bucket("pv", 1, 0)));
        final MergedBucketCursor merged = new MergedBucketCursor(List.of(input));
        assertEquals(0, input.reads);
        assertTrue(merged.hasNext());
        assertEquals(1, input.reads); // the head
    }

    @Test
    public void testCloseClosesEveryInputAndEndsIteration() {
        final ListCursor a = new ListCursor(List.of(bucket("a", 1, 0)));
        final ListCursor b = new ListCursor(List.of(bucket("b", 1, 0)));
        final MergedBucketCursor merged = new MergedBucketCursor(List.of(a, b));
        assertTrue(merged.hasNext());
        merged.close();
        assertTrue(a.closed);
        assertTrue(b.closed);
        assertFalse(merged.hasNext());
        merged.close(); // idempotent
    }

    @Test
    public void testMissingTimestampsSortFirstWithinPvRatherThanThrowing() {
        final BucketDocument malformed = new BucketDocument();
        malformed.setPvName("pv");
        final ListCursor a = new ListCursor(List.of(malformed));
        final ListCursor b = new ListCursor(List.of(bucket("pv", 0, 0)));
        final MergedBucketCursor merged = new MergedBucketCursor(List.of(b, a));
        assertTrue(merged.hasNext());
        assertEquals(malformed, merged.next());
        assertEquals("pv@0.0", key(merged.next()));
        assertFalse(merged.hasNext());
    }

    @Test
    public void testRequiresAtLeastOneInput() {
        assertThrows(IllegalArgumentException.class, () -> new MergedBucketCursor(List.of()));
    }

    /**
     * When one span class's cursor fails to open, the classes already opened must be closed before
     * the exception propagates. The caller converts that exception into the null cursor either way,
     * so a dropped cleanup loop is invisible from outside -- it just leaks a server cursor per
     * failed multi-class query. Drives {@code openSpanClassCursors} directly with a finder that
     * throws on the second class; the function never touches the database.
     */
    @Test
    public void testPartialOpenFailureClosesAlreadyOpenedCursors() {
        final MongoSyncQueryClient client = new MongoSyncQueryClient();
        final ListCursor first = new ListCursor(List.of(bucket("pv_a", 0, 0)));
        final List<SpanClass> classes = List.of(
                new SpanClass(List.of("pv_a"), 0L),
                new SpanClass(List.of("pv_b"), 4L));

        final MongoException failure = new MongoException("hint provided does not correspond to an existing index");
        final RuntimeException thrown = assertThrows(MongoException.class, () ->
                client.openSpanClassCursors(classes, spanClass -> {
                    if (spanClass.pvNames().contains("pv_a")) {
                        // a FindIterable whose cursor() hands back the tracked fake
                        return findIterableReturning(first);
                    }
                    throw failure;
                }));

        assertEquals(failure, thrown);
        assertTrue("the cursor opened for the first span class must be closed", first.closed);
    }

    /** A FindIterable stub whose only usable method is cursor(); the rest are unreachable here. */
    @SuppressWarnings("unchecked")
    private static com.mongodb.client.FindIterable<BucketDocument> findIterableReturning(
            MongoCursor<BucketDocument> cursor) {
        return (com.mongodb.client.FindIterable<BucketDocument>) java.lang.reflect.Proxy.newProxyInstance(
                MergedBucketCursorTest.class.getClassLoader(),
                new Class<?>[]{com.mongodb.client.FindIterable.class},
                (proxy, method, args) -> {
                    if ("cursor".equals(method.getName()) || "iterator".equals(method.getName())) {
                        return cursor;
                    }
                    throw new UnsupportedOperationException(method.getName());
                });
    }
}
