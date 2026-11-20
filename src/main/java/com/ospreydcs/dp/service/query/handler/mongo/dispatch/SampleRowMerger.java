package com.ospreydcs.dp.service.query.handler.mongo.dispatch;

import com.mongodb.client.MongoCursor;
import com.ospreydcs.dp.grpc.v1.common.DataValue;
import com.ospreydcs.dp.grpc.v1.common.Timestamp;
import com.ospreydcs.dp.service.common.bson.TimestampDocument;
import com.ospreydcs.dp.service.common.bson.bucket.BucketDocument;
import com.ospreydcs.dp.service.common.exception.DpException;
import com.ospreydcs.dp.service.common.protobuf.DataTimestampsUtility;
import com.ospreydcs.dp.service.common.utility.TabularDataUtility;
import com.ospreydcs.dp.service.query.handler.model.TimeInterval;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.function.BooleanSupplier;

/**
 * Merges a time-ordered bucket cursor into sample rows, one row per distinct timestamp, in
 * timestamp order (issue #314). It replaces the time-sliced retrieval and its pivot map on the
 * samples paths: buckets are read once, and a row is produced as soon as it is known to be
 * complete, so memory is the buckets overlapping the current position and nothing else.
 *
 * <h2>How a row is known to be complete</h2>
 *
 * <p>The cursor yields buckets ordered by {@code firstTime} across every resolved PV
 * ({@code MongoQueryClientInterface.executeQuerySamplesTimeOrdered}). Each bucket read becomes an
 * <em>open bucket</em>: its decoded column and a position. A min-heap over the open buckets gives
 * the earliest pending sample. One bucket is read ahead of need; its {@code firstTime} is the
 * <em>watermark</em>. No unread bucket can hold a sample earlier than the watermark, so every
 * pending sample strictly before it is complete across PVs and its row can be emitted. When the
 * earliest pending sample is at or after the watermark, the read-ahead bucket is opened first.
 *
 * <p>That argument needs three things of the stored data, all of which ingestion guarantees:
 * a bucket's {@code firstTime} is its earliest sample, the timestamps within a bucket do not
 * decrease, and the cursor is in {@code firstTime} order. A violation would produce rows out of
 * order, which is a wrong answer, so each is checked and reported as a {@link DpException}.
 *
 * <h2>Invariants carried over from the sliced retrieval</h2>
 *
 * <ul>
 * <li><b>Fragment trim (#207).</b> A sample is kept only when it lies inside one of
 *     {@code fragments}, which the caller derives from {@code TimeInterval.clampToWindowBegin},
 *     the same call the database filter uses. The fragments must be sorted and disjoint, as the
 *     resolver emits them.</li>
 * <li><b>Duplicate timestamps.</b> When two buckets of one PV (or two samples of one bucket) hold
 *     the same timestamp, the one read later wins, as it did when a later insert overwrote the
 *     pivot map's cell. "Later" is cursor order, which within a PV is index order.</li>
 * <li><b>Status selector.</b> Matching timestamps are loaded in windows as the merge advances
 *     (rows are produced in time order, so the windows only move forward), and the test is made
 *     when a row is emitted. A row whose every sample is filtered out is not produced. A failed
 *     load is a {@link DpException}, never "no statuses".</li>
 * <li><b>Failure classification.</b> Any unchecked exception out of the cursor (a {@code getMore}
 *     against a failed-over server, a decode error) is wrapped as {@link DpException}, so it
 *     reaches the dispatcher's error response instead of escaping into the worker.</li>
 * </ul>
 *
 * <p>Not thread-safe; one instance serves one request on one worker. The caller owns the cursor
 * and closes it.
 */
final class SampleRowMerger {

    /** Loads the status filter for one window; null means the request has no status selector. */
    @FunctionalInterface
    interface StatusWindowLoader {
        TabularDataUtility.SampleStatusFilter load(
                long beginSecs, long beginNanos, long endSecs, long endNanos) throws DpException;
    }

    /**
     * One output row. The merger reuses a single instance: a row is valid until the next
     * {@link #nextRow()} call, and {@code values} holds null where a PV has no sample.
     */
    static final class Row {
        long seconds;
        long nanos;
        final DataValue[] values;

        private Row(int columnCount) {
            this.values = new DataValue[columnCount];
        }
    }

    /** First status window length; it then adapts to the density of matching statuses. */
    static final long INITIAL_STATUS_WINDOW_NANOS = 60L * 1_000_000_000L;
    /** A window holding fewer matching timestamps than this doubles the next window. */
    static final int STATUS_WINDOW_GROW_BELOW = 10_000;
    /** A window holding more matching timestamps than this halves the next window. */
    static final int STATUS_WINDOW_SHRINK_ABOVE = 100_000;
    private static final long MIN_STATUS_WINDOW_NANOS = 1_000_000_000L;

    private final MongoCursor<BucketDocument> cursor;
    private final List<TimeInterval> fragments;
    private final Map<String, Integer> columnIndexByPv;
    private final StatusWindowLoader statusWindowLoader;
    private final BooleanSupplier cancelled;
    private final Row row;

    private final PriorityQueue<OpenBucket> openBuckets = new PriorityQueue<>();
    private BucketDocument readAhead = null;
    private boolean cursorExhausted;
    private boolean wasCancelled = false;
    private long bucketsRead = 0;

    private boolean anyRowEmitted = false;
    private long lastRowSeconds;
    private long lastRowNanos;

    private TabularDataUtility.SampleStatusFilter statusWindow = null;
    private long statusWindowEndSecs;
    private long statusWindowEndNanos;
    private long statusWindowNanos = INITIAL_STATUS_WINDOW_NANOS;

    /**
     * @param cursor buckets in {@code firstTime} order, or null when no fragment remains (the
     *               merger then produces no rows and issues no reads)
     * @param pvNames the resolved PV list; a PV's position is its column index
     * @param fragments the retention fragments, sorted and disjoint
     * @param statusWindowLoader the status-window loader, or null for no status selector
     * @param cancelled asked before each bucket read; true abandons the merge
     */
    SampleRowMerger(
            MongoCursor<BucketDocument> cursor,
            List<String> pvNames,
            List<TimeInterval> fragments,
            StatusWindowLoader statusWindowLoader,
            BooleanSupplier cancelled) {
        this.cursor = cursor;
        this.cursorExhausted = (cursor == null);
        this.fragments = fragments;
        this.statusWindowLoader = statusWindowLoader;
        this.cancelled = cancelled;
        this.columnIndexByPv = new HashMap<>(pvNames.size() * 2);
        for (int i = 0; i < pvNames.size(); i++) {
            columnIndexByPv.put(pvNames.get(i), i);
        }
        this.row = new Row(pvNames.size());
    }

    /** The cursor this merger reads, for the caller to close and time; may be null. */
    MongoCursor<BucketDocument> cursor() {
        return cursor;
    }

    /** True when {@link #nextRow()} returned null because the call was cancelled. */
    boolean wasCancelled() {
        return wasCancelled;
    }

    /** Buckets read from the cursor so far (for tests and tracing). */
    long bucketsRead() {
        return bucketsRead;
    }

    /** Buckets currently open (for tests: the merge's memory is proportional to this). */
    int openBucketCount() {
        return openBuckets.size();
    }

    /**
     * The next row in timestamp order, or null when the result is exhausted or the call was
     * cancelled ({@link #wasCancelled()} tells which).
     */
    Row nextRow() throws DpException {
        try {
            while (true) {
                if (readAhead == null && !cursorExhausted) {
                    if (cancelled.getAsBoolean()) {
                        wasCancelled = true;
                        return null;
                    }
                    if (cursor.hasNext()) {
                        readAhead = cursor.next();
                        bucketsRead++;
                    } else {
                        cursorExhausted = true;
                    }
                }

                final OpenBucket earliest = openBuckets.peek();
                if (earliest == null) {
                    if (readAhead == null) {
                        return null;
                    }
                    openReadAhead();
                    continue;
                }
                if (readAhead != null && !firstTimeIsAfter(readAhead, earliest.seconds, earliest.nanos)) {
                    // the unread bucket may hold a sample at or before the earliest pending one
                    openReadAhead();
                    continue;
                }

                if (emitRowAt(earliest.seconds, earliest.nanos)) {
                    return row;
                }
                // every sample at that timestamp was removed by the status filter; keep going
            }
        } catch (RuntimeException ex) {
            throw new DpException("querySamples retrieval failed: " + ex.getMessage(), ex);
        }
    }

    /**
     * Collects every pending sample at {@code (seconds, nanos)} into the row and advances the
     * buckets that supplied them. Returns false when the status filter removed all of them.
     */
    private boolean emitRowAt(long seconds, long nanos) throws DpException {
        if (anyRowEmitted && TimeInterval.compareInstant(seconds, nanos, lastRowSeconds, lastRowNanos) <= 0) {
            throw new DpException("querySamples merge produced timestamp " + seconds + "." + nanos
                    + " after " + lastRowSeconds + "." + lastRowNanos
                    + ": a stored bucket's firstTime is later than its first sample");
        }

        Arrays.fill(row.values, null);
        row.seconds = seconds;
        row.nanos = nanos;
        boolean anyValue = false;

        OpenBucket bucket;
        while ((bucket = openBuckets.peek()) != null && bucket.seconds == seconds && bucket.nanos == nanos) {
            openBuckets.poll();
            if (statusRetains(bucket.pvName, seconds, nanos)) {
                // heap order at equal timestamps is read order, so a later bucket overwrites
                row.values[bucket.columnIndex] = bucket.currentValue();
                anyValue = true;
            }
            if (bucket.advance()) {
                openBuckets.add(bucket);
            }
        }

        if (anyValue) {
            anyRowEmitted = true;
            lastRowSeconds = seconds;
            lastRowNanos = nanos;
        }
        return anyValue;
    }

    private void openReadAhead() throws DpException {
        final BucketDocument document = readAhead;
        readAhead = null;

        final Integer columnIndex = columnIndexByPv.get(document.getPvName());
        if (columnIndex == null) {
            throw new DpException("querySamples retrieved a bucket for PV '" + document.getPvName()
                    + "', which the request did not resolve");
        }
        if (document.getDataTimestamps() == null) {
            throw new DpException("bucket for PV '" + document.getPvName() + "' has no dataTimestamps");
        }
        final OpenBucket bucket = new OpenBucket(
                document.getPvName(),
                columnIndex,
                bucketsRead,
                DataTimestampsUtility.dataTimestampsIterator(document.getDataTimestamps().toDataTimestamps()),
                TabularDataUtility.tabularDataColumn(document).getDataValuesList());
        if (bucket.advance()) {
            openBuckets.add(bucket);
        }
    }

    private static boolean firstTimeIsAfter(BucketDocument document, long seconds, long nanos) throws DpException {
        final TimestampDocument firstTime =
                document.getDataTimestamps() == null ? null : document.getDataTimestamps().getFirstTime();
        if (firstTime == null) {
            throw new DpException("bucket for PV '" + document.getPvName() + "' has no firstTime");
        }
        return TimeInterval.compareInstant(firstTime.getSeconds(), firstTime.getNanos(), seconds, nanos) > 0;
    }

    private boolean statusRetains(String pvName, long seconds, long nanos) throws DpException {
        if (statusWindowLoader == null) {
            return true;
        }
        if (statusWindow == null
                || TimeInterval.compareInstant(seconds, nanos, statusWindowEndSecs, statusWindowEndNanos) >= 0) {
            loadStatusWindow(seconds, nanos);
        }
        return statusWindow.retains(pvName, seconds, nanos);
    }

    /**
     * Loads the matching status timestamps for a window starting at the row being emitted. Rows
     * come in time order, so each window starts where it is first needed and the previous one is
     * dropped. The length adapts to how many timestamps a window held, which is what bounds the
     * memory: sparse labeling gets long windows (few reads over a long range), dense labeling
     * short ones.
     */
    private void loadStatusWindow(long seconds, long nanos) throws DpException {
        if (statusWindow != null) {
            int held = 0;
            for (java.util.Set<Long> timestamps : statusWindow.matchingTimestampsByPv().values()) {
                held += timestamps.size();
            }
            if (held < STATUS_WINDOW_GROW_BELOW) {
                statusWindowNanos = (statusWindowNanos > Long.MAX_VALUE / 2) ? Long.MAX_VALUE : statusWindowNanos * 2;
            } else if (held > STATUS_WINDOW_SHRINK_ABOVE) {
                statusWindowNanos = Math.max(MIN_STATUS_WINDOW_NANOS, statusWindowNanos / 2);
            }
        }
        final TimeInterval last = fragments.get(fragments.size() - 1);
        final long[] end = AbstractQuerySamplesDispatcher.addNanos(
                seconds, nanos, statusWindowNanos, last.getEndSeconds(), last.getEndNanos());
        statusWindow = statusWindowLoader.load(seconds, nanos, end[0], end[1]);
        if (statusWindow == null) {
            throw new DpException("sample status selector resolution returned no filter");
        }
        statusWindowEndSecs = end[0];
        statusWindowEndNanos = end[1];
    }

    /** A bucket being merged: its decoded column and the position of its next retained sample. */
    private final class OpenBucket implements Comparable<OpenBucket> {

        private final String pvName;
        private final int columnIndex;
        private final long readOrder;
        private final Iterator<Timestamp> timestamps;
        private final List<DataValue> values;

        private int valueIndex = -1;
        private int fragmentIndex = 0;
        private boolean positioned = false;
        private long seconds;
        private long nanos;

        private OpenBucket(
                String pvName, int columnIndex, long readOrder,
                Iterator<Timestamp> timestamps, List<DataValue> values) {
            this.pvName = pvName;
            this.columnIndex = columnIndex;
            this.readOrder = readOrder;
            this.timestamps = timestamps;
            this.values = values;
        }

        private DataValue currentValue() throws DpException {
            if (valueIndex >= values.size()) {
                throw new DpException("bucket for PV '" + pvName + "' has " + values.size()
                        + " values but more timestamps");
            }
            return values.get(valueIndex);
        }

        /**
         * Moves to the next sample inside a fragment. Returns false when the bucket has none
         * left, including when its remaining samples lie past the last fragment.
         */
        private boolean advance() throws DpException {
            while (timestamps.hasNext()) {
                final Timestamp timestamp = timestamps.next();
                valueIndex++;
                final long nextSeconds = timestamp.getEpochSeconds();
                final long nextNanos = timestamp.getNanoseconds();
                if (positioned && TimeInterval.compareInstant(nextSeconds, nextNanos, seconds, nanos) < 0) {
                    throw new DpException("bucket for PV '" + pvName + "' has decreasing timestamps at "
                            + nextSeconds + "." + nextNanos);
                }
                positioned = true;
                seconds = nextSeconds;
                nanos = nextNanos;

                // fragments are sorted and disjoint and this bucket's timestamps do not decrease,
                // so the fragment pointer only moves forward
                while (fragmentIndex < fragments.size()) {
                    final TimeInterval fragment = fragments.get(fragmentIndex);
                    if (TimeInterval.compareInstant(seconds, nanos, fragment.getEndSeconds(), fragment.getEndNanos()) < 0) {
                        break;
                    }
                    fragmentIndex++;
                }
                if (fragmentIndex >= fragments.size()) {
                    return false; // past the last fragment: nothing further can be retained
                }
                final TimeInterval fragment = fragments.get(fragmentIndex);
                if (TimeInterval.compareInstant(seconds, nanos, fragment.getBeginSeconds(), fragment.getBeginNanos()) >= 0) {
                    return true;
                }
                // before this fragment's begin (in a gap, or before the window): skip
            }
            return false;
        }

        @Override
        public int compareTo(OpenBucket other) {
            final int byTime = TimeInterval.compareInstant(seconds, nanos, other.seconds, other.nanos);
            return byTime != 0 ? byTime : Long.compare(readOrder, other.readOrder);
        }
    }
}
