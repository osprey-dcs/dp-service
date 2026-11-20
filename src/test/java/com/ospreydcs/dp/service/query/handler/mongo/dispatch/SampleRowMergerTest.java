package com.ospreydcs.dp.service.query.handler.mongo.dispatch;

import com.mongodb.MongoException;
import com.mongodb.ServerAddress;
import com.mongodb.ServerCursor;
import com.mongodb.client.MongoCursor;
import com.ospreydcs.dp.grpc.v1.common.ArrayDimensions;
import com.ospreydcs.dp.grpc.v1.common.DataColumn;
import com.ospreydcs.dp.grpc.v1.common.DataTimestamps;
import com.ospreydcs.dp.grpc.v1.common.DataValue;
import com.ospreydcs.dp.grpc.v1.common.DoubleArrayColumn;
import com.ospreydcs.dp.grpc.v1.common.SamplingClock;
import com.ospreydcs.dp.grpc.v1.common.Timestamp;
import com.ospreydcs.dp.grpc.v1.common.TimestampList;
import com.ospreydcs.dp.service.common.bson.DataTimestampsDocument;
import com.ospreydcs.dp.service.common.bson.TimestampDocument;
import com.ospreydcs.dp.service.common.bson.bucket.BucketDocument;
import com.ospreydcs.dp.service.common.bson.column.DataColumnDocument;
import com.ospreydcs.dp.service.common.bson.column.DoubleArrayColumnDocument;
import com.ospreydcs.dp.service.common.exception.DpException;
import com.ospreydcs.dp.service.common.exception.NonScalarColumnException;
import com.ospreydcs.dp.service.common.utility.TabularDataUtility;
import com.ospreydcs.dp.service.query.handler.model.TimeInterval;
import org.junit.Test;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * {@link SampleRowMerger} driven directly from in-memory cursors (#314): the row order, the
 * completeness rule, how little it reads ahead and holds open, and each condition it must report
 * as an error instead of a wrong answer. No database; {@code MongoSyncQuerySamplesV2Test} covers
 * the same merge end to end through the dispatchers.
 */
public class SampleRowMergerTest {

    private static final long B = 1_700_000_000L;
    private static final long SECOND = 1_000_000_000L;
    private static final String PV_A = "pv_a";
    private static final String PV_B = "pv_b";
    private static final List<TimeInterval> EVERYTHING = List.of(new TimeInterval(0L, 0L, Long.MAX_VALUE, 0L));

    // ---- fixtures --------------------------------------------------------------------------------

    /** A bucket of {@code count} samples from {@code startSecond}, valued {@code base + i}. */
    private static BucketDocument clockBucket(String pvName, long startSecond, long periodNanos, int count, double base) {
        final BucketDocument bucket = new BucketDocument();
        bucket.setPvName(pvName);
        final DataColumn.Builder column = DataColumn.newBuilder().setName(pvName);
        for (int i = 0; i < count; i++) {
            column.addDataValues(DataValue.newBuilder().setDoubleValue(base + i).build());
        }
        bucket.setDataColumn(DataColumnDocument.fromDataColumn(column.build()));
        bucket.setDataTimestamps(DataTimestampsDocument.fromDataTimestamps(DataTimestamps.newBuilder()
                .setSamplingClock(SamplingClock.newBuilder()
                        .setStartTime(Timestamp.newBuilder().setEpochSeconds(startSecond).build())
                        .setPeriodNanos(periodNanos)
                        .setCount(count))
                .build()));
        return bucket;
    }

    /** A bucket with an explicit timestamp list (seconds offsets from B), valued {@code base + i}. */
    private static BucketDocument listBucket(String pvName, double base, long... secondOffsets) {
        final BucketDocument bucket = new BucketDocument();
        bucket.setPvName(pvName);
        final DataColumn.Builder column = DataColumn.newBuilder().setName(pvName);
        final TimestampList.Builder timestamps = TimestampList.newBuilder();
        for (int i = 0; i < secondOffsets.length; i++) {
            column.addDataValues(DataValue.newBuilder().setDoubleValue(base + i).build());
            timestamps.addTimestamps(Timestamp.newBuilder().setEpochSeconds(B + secondOffsets[i]).build());
        }
        bucket.setDataColumn(DataColumnDocument.fromDataColumn(column.build()));
        bucket.setDataTimestamps(DataTimestampsDocument.fromDataTimestamps(
                DataTimestamps.newBuilder().setTimestampList(timestamps).build()));
        return bucket;
    }

    /** An in-memory cursor that counts what it hands out. */
    private static class ListCursor implements MongoCursor<BucketDocument> {
        private final Iterator<BucketDocument> iterator;
        int served = 0;

        ListCursor(List<BucketDocument> buckets) {
            this.iterator = buckets.iterator();
        }

        @Override public void close() { }
        @Override public boolean hasNext() { return iterator.hasNext(); }
        @Override public BucketDocument next() { served++; return iterator.next(); }
        @Override public int available() { return 0; }
        @Override public BucketDocument tryNext() { return hasNext() ? next() : null; }
        @Override public ServerCursor getServerCursor() { return null; }
        @Override public ServerAddress getServerAddress() { return null; }
    }

    private static SampleRowMerger merger(MongoCursor<BucketDocument> cursor, List<String> pvNames) {
        return new SampleRowMerger(cursor, pvNames, EVERYTHING, null, () -> false);
    }

    /** One collected row: the offset in whole seconds from B, and each column's value or null. */
    private record CollectedRow(long offsetSeconds, long nanos, List<Double> values) { }

    private static List<CollectedRow> drain(SampleRowMerger merger) throws DpException {
        final List<CollectedRow> rows = new ArrayList<>();
        SampleRowMerger.Row row;
        while ((row = merger.nextRow()) != null) {
            final List<Double> values = new ArrayList<>();
            for (DataValue value : row.values) {
                values.add(value == null ? null : value.getDoubleValue());
            }
            rows.add(new CollectedRow(row.seconds - B, row.nanos, values));
        }
        return rows;
    }

    private static List<Long> offsets(List<CollectedRow> rows) {
        final List<Long> offsets = new ArrayList<>();
        for (CollectedRow row : rows) {
            offsets.add(row.offsetSeconds());
        }
        return offsets;
    }

    // ---- row order and completeness --------------------------------------------------------------

    @Test
    public void testRowsAreTheUnionOfTimestampsWithMissingValuesUnset() throws DpException {
        // pv_a at 2 Hz and pv_b at 1 Hz over two seconds, as the time-ordered cursor yields them
        final SampleRowMerger merger = merger(new ListCursor(List.of(
                clockBucket(PV_A, B, SECOND / 2, 2, 100),
                clockBucket(PV_B, B, SECOND, 1, 200),
                clockBucket(PV_A, B + 1, SECOND / 2, 2, 110),
                clockBucket(PV_B, B + 1, SECOND, 1, 210))), List.of(PV_A, PV_B));

        final List<CollectedRow> rows = drain(merger);
        assertEquals(4, rows.size());
        assertEquals(new CollectedRow(0, 0, List.of(100.0, 200.0)), rows.get(0));
        assertEquals(0, rows.get(1).offsetSeconds());
        assertEquals(SECOND / 2, rows.get(1).nanos());
        assertEquals(100.0 + 1, rows.get(1).values().get(0), 0);
        assertNull("pv_b has no sample at the half second", rows.get(1).values().get(1));
        assertEquals(new CollectedRow(1, 0, List.of(110.0, 210.0)), rows.get(2));
        assertNull(rows.get(3).values().get(1));
    }

    /**
     * A row is emitted only once the next unread bucket starts after it. Here pv_b's bucket
     * starts at the same instant as pv_a's first sample, so it must be opened before that row.
     */
    @Test
    public void testABucketStartingAtTheRowTimestampIsOpenedBeforeTheRow() throws DpException {
        final ListCursor cursor = new ListCursor(List.of(
                clockBucket(PV_A, B, SECOND, 3, 100),
                clockBucket(PV_B, B, SECOND, 3, 200)));
        final SampleRowMerger merger = merger(cursor, List.of(PV_A, PV_B));

        final SampleRowMerger.Row first = merger.nextRow();
        assertEquals(B, first.seconds);
        assertEquals(100.0, first.values[0].getDoubleValue(), 0);
        assertEquals("the row must be complete across PVs", 200.0, first.values[1].getDoubleValue(), 0);
    }

    /**
     * A wide bucket beside narrow ones: pv_a is one 100-second bucket, pv_b a hundred one-second
     * buckets. The merge holds the wide bucket open and one narrow one at a time, and reads the
     * narrow ones as the position reaches them.
     */
    @Test
    public void testWideBucketBesideNarrowOnesHoldsFewBucketsOpen() throws DpException {
        final List<BucketDocument> buckets = new ArrayList<>();
        buckets.add(clockBucket(PV_A, B, SECOND, 100, 0));
        for (int second = 0; second < 100; second++) {
            buckets.add(clockBucket(PV_B, B + second, SECOND, 1, 1000 + second));
        }
        final ListCursor cursor = new ListCursor(buckets);
        final SampleRowMerger merger = merger(cursor, List.of(PV_A, PV_B));

        int rows = 0;
        SampleRowMerger.Row row;
        while ((row = merger.nextRow()) != null) {
            assertEquals(B + rows, row.seconds);
            assertEquals((double) rows, row.values[0].getDoubleValue(), 0);
            assertEquals(1000.0 + rows, row.values[1].getDoubleValue(), 0);
            rows++;
            assertTrue("open buckets: " + merger.openBucketCount(), merger.openBucketCount() <= 2);
            // the wide bucket, this row's narrow bucket, and the one read ahead
            assertTrue("read " + cursor.served + " buckets after " + rows + " rows", cursor.served <= rows + 2);
        }
        assertEquals(100, rows);
        assertEquals(101, merger.bucketsRead());
    }

    /**
     * The #314 shape: a quiet stretch, then dense data. The first row needs only the first
     * bucket and the one read ahead of it, however much follows.
     */
    @Test
    public void testQuietThenDenseReadsOnlyWhatTheNextRowNeeds() throws DpException {
        final List<BucketDocument> buckets = new ArrayList<>();
        buckets.add(clockBucket(PV_A, B, SECOND, 1, 0));
        final long denseStart = B + 2L * 365 * 24 * 3600;
        for (int i = 0; i < 500; i++) {
            buckets.add(clockBucket(PV_A, denseStart + i * 100L, SECOND, 100, i * 100));
        }
        final ListCursor cursor = new ListCursor(buckets);
        final SampleRowMerger merger = merger(cursor, List.of(PV_A));

        assertEquals(B, merger.nextRow().seconds);
        assertEquals(2, cursor.served);
        assertEquals(denseStart, merger.nextRow().seconds);
        assertTrue("read " + cursor.served, cursor.served <= 3);

        long rows = 2;
        while (merger.nextRow() != null) {
            rows++;
            assertTrue(merger.openBucketCount() <= 1);
        }
        assertEquals(1 + 500 * 100L, rows);
        assertEquals("each bucket is read once", 501, cursor.served);
    }

    // ---- duplicates --------------------------------------------------------------------------------

    /** Two buckets of one PV overlapping in time: at a shared timestamp the later bucket wins. */
    @Test
    public void testOverlappingBucketsOfOnePvLaterBucketWins() throws DpException {
        final SampleRowMerger merger = merger(new ListCursor(List.of(
                listBucket(PV_A, 100, 0, 1, 2, 3),
                listBucket(PV_A, 200, 2, 3, 4))), List.of(PV_A));

        final List<CollectedRow> rows = drain(merger);
        assertEquals(List.of(0L, 1L, 2L, 3L, 4L), offsets(rows));
        assertEquals(100.0, rows.get(0).values().get(0), 0);
        assertEquals(101.0, rows.get(1).values().get(0), 0);
        assertEquals("later bucket's value at the shared timestamp", 200.0, rows.get(2).values().get(0), 0);
        assertEquals(201.0, rows.get(3).values().get(0), 0);
        assertEquals(202.0, rows.get(4).values().get(0), 0);
    }

    /** A timestamp repeated inside one bucket yields one row holding the last of its values. */
    @Test
    public void testRepeatedTimestampWithinABucketLastValueWins() throws DpException {
        final SampleRowMerger merger = merger(new ListCursor(List.of(
                listBucket(PV_A, 100, 0, 1, 1, 2))), List.of(PV_A));

        final List<CollectedRow> rows = drain(merger);
        assertEquals(List.of(0L, 1L, 2L), offsets(rows));
        assertEquals(102.0, rows.get(1).values().get(0), 0);
        assertEquals(103.0, rows.get(2).values().get(0), 0);
    }

    // ---- fragments (#207) and the status selector --------------------------------------------------

    @Test
    public void testSamplesOutsideEveryFragmentAreDropped() throws DpException {
        // one bucket spanning ten seconds; fragments [0, 2) and [8, 10) with the gap between
        final List<TimeInterval> fragments = List.of(
                new TimeInterval(B, 0L, B + 2, 0L), new TimeInterval(B + 8, 0L, B + 10, 0L));
        final SampleRowMerger merger = new SampleRowMerger(
                new ListCursor(List.of(clockBucket(PV_A, B, SECOND, 12, 0))),
                List.of(PV_A), fragments, null, () -> false);

        assertEquals(List.of(0L, 1L, 8L, 9L), offsets(drain(merger)));
        assertEquals("a bucket past the last fragment is not kept open", 0, merger.openBucketCount());
    }

    /** A fragment end is exclusive and a begin inclusive, at nanosecond resolution. */
    @Test
    public void testFragmentBoundsAreHalfOpen() throws DpException {
        final List<TimeInterval> fragments = List.of(new TimeInterval(B + 1, 0L, B + 3, 0L));
        final SampleRowMerger merger = new SampleRowMerger(
                new ListCursor(List.of(clockBucket(PV_A, B, SECOND, 5, 0))),
                List.of(PV_A), fragments, null, () -> false);
        assertEquals(List.of(1L, 2L), offsets(drain(merger)));
    }

    /**
     * The status filter is asked for windows that only move forward, each starting at the row
     * that needed it. A sample it removes becomes a missing value, and a row with nothing left
     * is not produced.
     */
    @Test
    public void testStatusWindowsAdvanceWithTheMergeAndFilterPerSample() throws DpException {
        // EXCLUDE: pv_a labeled at +1 (pv_b keeps the row), both labeled at +3 (row omitted),
        // and pv_a labeled at +150, which falls in a later window
        final Map<String, Set<Long>> labeled = Map.of(
                PV_A, Set.of((B + 1) * SECOND, (B + 3) * SECOND, (B + 150) * SECOND),
                PV_B, Set.of((B + 3) * SECOND));
        final List<long[]> windows = new ArrayList<>();
        final SampleRowMerger.StatusWindowLoader loader = (beginSecs, beginNanos, endSecs, endNanos) -> {
            windows.add(new long[]{beginSecs, endSecs});
            final Map<String, Set<Long>> inWindow = new java.util.HashMap<>();
            labeled.forEach((pv, timestamps) -> {
                final Set<Long> kept = new HashSet<>();
                for (long timestamp : timestamps) {
                    if (timestamp >= beginSecs * SECOND + beginNanos && timestamp < endSecs * SECOND + endNanos) {
                        kept.add(timestamp);
                    }
                }
                inWindow.put(pv, kept);
            });
            return new TabularDataUtility.SampleStatusFilter(false, inWindow);
        };
        final List<TimeInterval> range = List.of(new TimeInterval(B, 0L, B + 200, 0L));
        final SampleRowMerger merger = new SampleRowMerger(
                new ListCursor(List.of(
                        clockBucket(PV_A, B, SECOND, 200, 0),
                        clockBucket(PV_B, B, SECOND, 200, 1000))),
                List.of(PV_A, PV_B), range, loader, () -> false);

        final List<CollectedRow> rows = drain(merger);
        assertEquals("the row at +3 is omitted", 199, rows.size());
        assertNull("pv_a filtered at +1", rows.get(1).values().get(0));
        assertEquals(1001.0, rows.get(1).values().get(1), 0);
        assertEquals(4, rows.get(3).offsetSeconds());
        final CollectedRow at150 = rows.get(149);
        assertEquals(150, at150.offsetSeconds());
        assertNull("pv_a filtered at +150, in a later window", at150.values().get(0));

        assertTrue("expected more than one window, got " + windows.size(), windows.size() > 1);
        assertEquals("the first window starts at the first row", B, windows.get(0)[0]);
        for (int i = 1; i < windows.size(); i++) {
            assertEquals("each window starts where the previous one ended",
                    windows.get(i - 1)[1], windows.get(i)[0]);
        }
        assertEquals("the last window is capped at the range end", B + 200, windows.get(windows.size() - 1)[1]);
    }

    // ---- cancellation and the empty merger -----------------------------------------------------------

    @Test
    public void testCancellationIsNoticedBeforeTheNextBucketRead() throws DpException {
        final List<BucketDocument> buckets = new ArrayList<>();
        for (int second = 0; second < 50; second++) {
            buckets.add(clockBucket(PV_A, B + second, SECOND, 1, second));
        }
        final ListCursor cursor = new ListCursor(buckets);
        final boolean[] cancelled = {false};
        final SampleRowMerger merger =
                new SampleRowMerger(cursor, List.of(PV_A), EVERYTHING, null, () -> cancelled[0]);

        assertEquals(B, merger.nextRow().seconds);
        final int servedBeforeCancel = cursor.served;
        cancelled[0] = true;

        // the bucket already read ahead may still yield its row; no further bucket is read
        int rowsAfterCancel = 0;
        while (merger.nextRow() != null) {
            rowsAfterCancel++;
        }
        assertTrue(merger.wasCancelled());
        assertTrue("rows after cancel: " + rowsAfterCancel, rowsAfterCancel <= 1);
        assertEquals("no bucket read after the cancel", servedBeforeCancel, cursor.served);
    }

    @Test
    public void testNoCursorMeansNoRowsAndNotCancelled() throws DpException {
        final SampleRowMerger merger = merger(null, List.of(PV_A));
        assertNull(merger.nextRow());
        assertFalse(merger.wasCancelled());
    }

    // ---- conditions reported as errors, never as a wrong answer -----------------------------------------

    private static DpException expectFailure(SampleRowMerger merger) {
        try {
            drain(merger);
        } catch (DpException expected) {
            return expected;
        }
        fail("expected the merge to fail");
        return null;
    }

    /** An unchecked failure from the driver mid-read must not escape as unchecked (see #281). */
    @Test
    public void testCursorFailureIsWrappedAsDpException() {
        final MongoCursor<BucketDocument> failing = new ListCursor(List.of(clockBucket(PV_A, B, SECOND, 1, 0))) {
            @Override public boolean hasNext() { return true; }
            @Override public BucketDocument next() {
                if (served == 0) {
                    return super.next();
                }
                throw new MongoException("simulated getMore failure");
            }
        };
        final DpException failure = expectFailure(merger(failing, List.of(PV_A)));
        assertTrue(failure.getCause() instanceof MongoException);
    }

    /** The scalar-only reject (Q4) relies on the exception type surviving the merge. */
    @Test
    public void testNonScalarBucketKeepsItsExceptionType() throws DpException {
        final BucketDocument bucket = new BucketDocument();
        bucket.setPvName(PV_A);
        bucket.setDataColumn(DoubleArrayColumnDocument.fromDoubleArrayColumn(DoubleArrayColumn.newBuilder()
                .setName(PV_A)
                .setDimensions(ArrayDimensions.newBuilder().addDims(2))
                .addValues(1.0).addValues(2.0)
                .build()));
        bucket.setDataTimestamps(DataTimestampsDocument.fromDataTimestamps(DataTimestamps.newBuilder()
                .setSamplingClock(SamplingClock.newBuilder()
                        .setStartTime(Timestamp.newBuilder().setEpochSeconds(B).build())
                        .setPeriodNanos(SECOND).setCount(1))
                .build()));
        final DpException failure = expectFailure(merger(new ListCursor(List.of(bucket)), List.of(PV_A)));
        assertTrue(failure.getClass().getName(), failure instanceof NonScalarColumnException);
    }

    /**
     * The completeness rule trusts each bucket's stored firstTime. A bucket whose firstTime is
     * later than its first sample would put a row out of order; that must be an error.
     */
    @Test
    public void testFirstTimeLaterThanFirstSampleIsAnError() {
        final BucketDocument honest = listBucket(PV_A, 100, 10);
        final BucketDocument lying = listBucket(PV_B, 200, 5);
        final TimestampDocument wrongFirstTime = new TimestampDocument();
        wrongFirstTime.setSeconds(B + 20);
        wrongFirstTime.setNanos(0);
        lying.getDataTimestamps().setFirstTime(wrongFirstTime);

        final DpException failure = expectFailure(merger(new ListCursor(List.of(honest, lying)), List.of(PV_A, PV_B)));
        assertTrue(failure.getMessage(), failure.getMessage().contains("firstTime is later than its first sample"));
    }

    @Test
    public void testDecreasingTimestampsWithinABucketIsAnError() {
        final DpException failure = expectFailure(
                merger(new ListCursor(List.of(listBucket(PV_A, 100, 0, 2, 1))), List.of(PV_A)));
        assertTrue(failure.getMessage(), failure.getMessage().contains("decreasing timestamps"));
    }

    @Test
    public void testBucketForAnUnresolvedPvIsAnError() {
        final DpException failure = expectFailure(
                merger(new ListCursor(List.of(clockBucket("other_pv", B, SECOND, 1, 0))), List.of(PV_A)));
        assertTrue(failure.getMessage(), failure.getMessage().contains("did not resolve"));
    }
}
