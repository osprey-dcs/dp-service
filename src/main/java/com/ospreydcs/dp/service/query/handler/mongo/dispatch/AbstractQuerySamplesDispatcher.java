package com.ospreydcs.dp.service.query.handler.mongo.dispatch;

import com.mongodb.client.MongoCursor;
import com.ospreydcs.dp.grpc.v1.common.DataColumn;
import com.ospreydcs.dp.grpc.v1.common.DataValue;
import com.ospreydcs.dp.grpc.v1.common.SerializedDataColumn;
import com.ospreydcs.dp.grpc.v1.common.Timestamp;
import com.ospreydcs.dp.grpc.v1.common.TimestampList;
import com.ospreydcs.dp.grpc.v1.query.ColumnTable;
import com.ospreydcs.dp.service.common.bson.bucket.BucketDocument;
import com.ospreydcs.dp.service.common.exception.DpException;
import com.ospreydcs.dp.service.common.model.TimestampDataMap;
import com.ospreydcs.dp.service.common.utility.TabularDataUtility;
import com.ospreydcs.dp.service.query.handler.QueryTelemetry;
import com.ospreydcs.dp.service.query.handler.model.KeysetPosition;
import com.ospreydcs.dp.service.query.handler.model.ResolvedQuery;
import com.ospreydcs.dp.service.query.handler.model.TimeInterval;
import com.ospreydcs.dp.service.query.handler.mongo.client.MongoQueryClientInterface;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Shared base for the Query API V2 sample dispatchers (unary {@link QuerySamplesUnaryDispatcher} and
 * streaming {@link QuerySamplesStreamDispatcher}). Holds the outgoing message-size budget, the
 * time-sliced retrieval loop ({@link SliceDrain}, issue #274), and the column-table assembly
 * building blocks — page-window computation, column seeding from the resolved PV list (Q9),
 * distinct-timestamp collection, and the V2 {@link ColumnTable} builder over a row range (with the
 * useSerializedColumns handling, Q5) — so the two dispatchers differ only in what they do with an
 * accepted slice (accumulate toward one truncated page vs. emit successive row-chunks).
 *
 * <h2>Why retrieval is sliced in time (issue #274)</h2>
 *
 * <p>Every bucket query is sorted {@code (pvName, firstTime)}, so its cursor is <b>PV-major</b>:
 * all of one PV's buckets in the window, then all of the next PV's. Before #274 a page was
 * assembled by draining that cursor until the outgoing byte budget tripped, on the assumption that
 * only the last assembled timestamp could then be incomplete. Under PV-major order that assumption
 * is false: when the budget trips partway through the first PV, every later PV has contributed
 * nothing, so every timestamp is incomplete and the page went out with those columns silently
 * all-unset -- and the resume token, a timestamp, put the next page in the same position. The
 * later PVs were never returned.
 *
 * <p>The retrieval is therefore made in consecutive time slices, each a single query over
 * <em>all</em> resolved PVs, and a slice is either drained completely or discarded (plan D1). A
 * timestamp inside an accepted slice is complete across every PV by construction. The slice
 * length adapts toward the page size (plan D2), so a page costs two or three retrievals at any
 * steady sample rate rather than one retrieval per fixed slice -- which matters because each
 * retrieval pays the #232 span scan (a document fetch per index key in {@code [begin - span, end]}).
 */
public abstract class AbstractQuerySamplesDispatcher extends QueryV2Dispatcher {

    private static final Logger logger = LogManager.getLogger();

    /** Largest single-step growth of the slice length (plan D2). */
    static final long MAX_SLICE_GROWTH_FACTOR = 16L;

    private static final long NANOS_PER_SECOND = 1_000_000_000L;

    protected final long byteBudget;
    protected final long initialSliceNanos;

    protected AbstractQuerySamplesDispatcher(long byteBudget, long initialSliceNanos, QueryTelemetry telemetry) {
        super(telemetry);
        this.byteBudget = byteBudget;
        this.initialSliceNanos = Math.max(1L, initialSliceNanos);
    }

    /**
     * The page/stream window <em>begin</em> for the resolved query: the resume timestamp (from a
     * continuation token) or, on the first page, the earliest fragment begin. Returns
     * {@code {beginSecs, beginNanos}}.
     *
     * <p>Deliberately begin-only. There is no corresponding window <em>end</em> for retention,
     * because there is no single upper bound that is correct to filter samples on: the resolved
     * fragments may be disjoint, and a collapsed {@code [min begin, max end)} window spans the gaps
     * between them. Filtering samples against such a window is precisely the #207 defect. The
     * slice end that {@link SliceDrain} applies is a <em>retrieval</em> bound intersected with each
     * fragment by {@link TimeInterval#clampToWindow}, not a retention window; do not turn it into one.
     */
    protected static long[] computeWindowBegin(ResolvedQuery resolvedQuery) {
        final List<TimeInterval> intervals = resolvedQuery.getRetrievalIntervals();
        final KeysetPosition pageStart = resolvedQuery.getPageStart();

        if (pageStart != null) {
            return new long[]{pageStart.getSeconds(), pageStart.getNanos()};
        }
        return new long[]{intervals.get(0).getBeginSeconds(), intervals.get(0).getBeginNanos()};
    }

    /**
     * The sample-retention windows for one slice: one {@link TabularDataUtility.RetentionInterval}
     * per resolved retrieval fragment that overlaps {@code [windowBegin, windowEnd)}, each clamped
     * to the slice.
     *
     * <p>Assembly must trim against this full list rather than a single collapsed window (issue
     * #207). The database filters fragments only at <em>bucket</em> granularity, so a bucket
     * spanning the gap between two fragments is retrieved with its in-gap samples intact; trimming
     * against a collapsed window would leave them in the result.
     *
     * <p>The clamp itself comes from {@link TimeInterval#clampToWindow}, the same call
     * {@code MongoSyncQueryClient.executeQuerySamplesV2} uses to build its per-fragment database
     * filters — so the retrieval filter and this trim cannot drift apart.
     */
    protected static List<TabularDataUtility.RetentionInterval> retentionIntervals(
            ResolvedQuery resolvedQuery,
            long windowBeginSecs, long windowBeginNanos,
            long windowEndSecs, long windowEndNanos) {

        final List<TabularDataUtility.RetentionInterval> intervals = new ArrayList<>();
        for (TimeInterval fragment : TimeInterval.clampToWindow(
                resolvedQuery.getRetrievalIntervals(),
                windowBeginSecs, windowBeginNanos, windowEndSecs, windowEndNanos)) {
            intervals.add(new TabularDataUtility.RetentionInterval(
                    fragment.getBeginSeconds(), fragment.getBeginNanos(),
                    fragment.getEndSeconds(), fragment.getEndNanos()));
        }
        return intervals;
    }

    /**
     * Resolves the query's sampleStatusSelector to a {@link TabularDataUtility.SampleStatusFilter}
     * for assembly-time per-sample filtering, or {@code null} when the request carries no selector.
     * The per-PV matching-timestamp sets come from the sampleStatusBuckets collection over the same
     * clamped slice the bucket retrieval uses, so the join input covers exactly the samples that
     * can appear in this slice. Composition with the configurationSelector is by intersection:
     * this filter and the fragment retention test are both applied in the same per-sample retention
     * decision.
     *
     * @throws DpException on a database error or malformed stored status document — never silently
     *     degraded to "no statuses", which in EXCLUDE mode would return filtered-out samples
     */
    protected static TabularDataUtility.SampleStatusFilter statusRetentionFilter(
            ResolvedQuery resolvedQuery,
            MongoQueryClientInterface mongoClient,
            long windowBeginSecs, long windowBeginNanos,
            long windowEndSecs, long windowEndNanos) throws DpException {

        if (resolvedQuery.getStatusFilter() == null) {
            return null;
        }
        final Map<String, Set<Long>> matchingTimestampsByPv = mongoClient.resolveSampleStatusTimestamps(
                resolvedQuery, windowBeginSecs, windowBeginNanos, windowEndSecs, windowEndNanos);
        if (matchingTimestampsByPv == null) {
            throw new DpException("sample status selector resolution failed (database error)");
        }
        return new TabularDataUtility.SampleStatusFilter(
                resolvedQuery.getStatusFilter().includeMode(), matchingTimestampsByPv);
    }

    /**
     * Creates a {@link TimestampDataMap} with its column index map pre-seeded from the resolved PV
     * list (sorted), so every resolved PV gets a stable column even with no data in the window (Q9).
     */
    protected static TimestampDataMap seededTable(ResolvedQuery resolvedQuery) {
        final TimestampDataMap tableValueMap = new TimestampDataMap();
        for (String pvName : resolvedQuery.getPvNames()) {
            tableValueMap.getColumnIndex(pvName);
        }
        return tableValueMap;
    }

    /** Collects the map's distinct {@code (second, nano)} timestamps in sorted order. */
    protected static List<long[]> collectTimestamps(TimestampDataMap tableValueMap) {
        final List<long[]> timestamps = new ArrayList<>();
        for (Map.Entry<Long, Map<Long, Map<Integer, DataValue>>> secondEntry : tableValueMap.entrySet()) {
            final long second = secondEntry.getKey();
            for (Long nano : secondEntry.getValue().keySet()) {
                timestamps.add(new long[]{second, nano});
            }
        }
        return timestamps;
    }

    // ---- time-sliced retrieval (issue #274) ------------------------------------------------------

    /** What one {@link SliceDrain#drainNext()} call did. */
    enum SliceOutcome {
        /** The slice was drained completely for every PV and its rows are in the map. */
        ACCEPTED,
        /**
         * The slice tripped the byte budget while the map already held rows from earlier slices:
         * the slice's rows were discarded and the position was not advanced. The caller must
         * consume the accumulated rows (end the page, or emit and {@link SliceDrain#markEmitted()})
         * before calling again; the resume point is {@link SliceDrain#resumeSecs()}/{@code Nanos()}.
         */
        BUDGET_TRIP,
        /** Every fragment has been retrieved; nothing remains. */
        EXHAUSTED,
        /**
         * The map was empty and a slice one nanosecond wide still tripped the budget: a single
         * timestamp is larger than the whole message budget and cannot be paged.
         */
        OVERSIZED
    }

    /**
     * The retrieval loop for one page or stream (issue #274, plan D1–D4): consecutive time slices
     * over every resolved PV, each intersected with the resolved fragments through
     * {@link TimeInterval#clampToWindow}, drained into the shared map and either accepted whole or
     * discarded whole.
     *
     * <p><b>Slice length</b> (plan D2) starts at the configured initial length and, after each
     * accepted slice of {@code r} distinct timestamps toward a target of {@code pageSize}, is
     * multiplied by {@code clamp(pageSize / max(r, 1), 1, 16)}: proportional rather than doubling,
     * so a page is reached in two or three retrievals at any steady rate and an empty slice grows
     * sixteenfold. It never shrinks except on a budget trip.
     *
     * <p><b>Budget trip</b> (plan D3): the cumulative data size across accepted-but-unconsumed
     * slices is bounded by the outgoing message budget. When a slice trips it, its rows are removed
     * from the map ({@code removeFrom(sliceBegin)}) and, if earlier slices left rows behind, the
     * caller is told to consume them ({@link SliceOutcome#BUDGET_TRIP}); otherwise the slice is
     * halved and retried from the same begin, down to one nanosecond, where a trip means a single
     * timestamp exceeds the whole budget ({@link SliceOutcome#OVERSIZED}). Every non-error page or
     * chunk therefore makes progress, as before.
     *
     * <p><b>Gaps</b> (plan D4): a slice that intersects no fragment is skipped without a database
     * call, and the position jumps to the next fragment's begin.
     *
     * <p>Each slice's retrieval and cursor are timed into the request's {@code db} stage (#212), so
     * a sliced page reports one {@code db} figure spanning all its slices.
     */
    final class SliceDrain {

        private final ResolvedQuery resolvedQuery;
        private final MongoQueryClientInterface mongoClient;
        private final TimestampDataMap tableValueMap;
        private final long windowEndSecs;
        private final long windowEndNanos;

        private long sliceNanos;
        private long cursorSecs;
        private long cursorNanos;
        private int dataSize = 0;
        private boolean exhausted = false;
        private int retrievals = 0;

        SliceDrain(
                ResolvedQuery resolvedQuery,
                MongoQueryClientInterface mongoClient,
                TimestampDataMap tableValueMap,
                long windowBeginSecs,
                long windowBeginNanos) {
            this.resolvedQuery = resolvedQuery;
            this.mongoClient = mongoClient;
            this.tableValueMap = tableValueMap;
            this.sliceNanos = initialSliceNanos;
            this.cursorSecs = windowBeginSecs;
            this.cursorNanos = windowBeginNanos;
            final List<TimeInterval> fragments = resolvedQuery.getRetrievalIntervals();
            final TimeInterval last = fragments.get(fragments.size() - 1);
            this.windowEndSecs = last.getEndSeconds();
            this.windowEndNanos = last.getEndNanos();
            if (TimeInterval.compareInstant(cursorSecs, cursorNanos, windowEndSecs, windowEndNanos) >= 0) {
                exhausted = true;
            }
        }

        /** The first timestamp not yet retrieved: the resume point after a budget trip. */
        long resumeSecs() {
            return cursorSecs;
        }

        long resumeNanos() {
            return cursorNanos;
        }

        /** Number of database retrievals issued so far (for tests and the slow-query log). */
        int retrievals() {
            return retrievals;
        }

        /** True once every fragment has been retrieved; the position is then past the window end. */
        boolean isExhausted() {
            return exhausted;
        }

        /** Resets the cumulative data size after the caller has emitted and drained the map. */
        void markEmitted() {
            dataSize = 0;
        }

        SliceOutcome drainNext() throws DpException {
            while (true) {
                if (exhausted) {
                    return SliceOutcome.EXHAUSTED;
                }

                final long[] sliceEnd = addNanos(cursorSecs, cursorNanos, sliceNanos, windowEndSecs, windowEndNanos);
                final List<TabularDataUtility.RetentionInterval> retention = retentionIntervals(
                        resolvedQuery, cursorSecs, cursorNanos, sliceEnd[0], sliceEnd[1]);

                if (retention.isEmpty()) {
                    // The slice lies in a gap between fragments (or past the last one): jump to the
                    // next fragment begin without a database call.
                    if (!jumpToNextFragmentBegin(sliceEnd[0], sliceEnd[1])) {
                        exhausted = true;
                        return SliceOutcome.EXHAUSTED;
                    }
                    continue;
                }

                final int rowsBefore = tableValueMap.size();
                final TabularDataUtility.TimestampDataMapSizeStats sizeStats =
                        retrieveSlice(cursorSecs, cursorNanos, sliceEnd[0], sliceEnd[1], retention);

                if (sizeStats.sizeLimitExceeded()) {
                    // Discard the slice whole: under PV-major order none of its timestamps is
                    // known to be complete (class javadoc). Rows from earlier slices precede the
                    // slice begin and survive.
                    tableValueMap.removeFrom(cursorSecs, cursorNanos);
                    if (rowsBefore > 0) {
                        return SliceOutcome.BUDGET_TRIP;
                    }
                    if (sliceNanos <= 1L) {
                        return SliceOutcome.OVERSIZED;
                    }
                    sliceNanos = Math.max(1L, sliceNanos / 2);
                    continue;
                }

                dataSize = sizeStats.currentDataSize();
                final int rowsInSlice = tableValueMap.size() - rowsBefore;
                cursorSecs = sliceEnd[0];
                cursorNanos = sliceEnd[1];
                if (TimeInterval.compareInstant(cursorSecs, cursorNanos, windowEndSecs, windowEndNanos) >= 0) {
                    exhausted = true;
                }
                logger.trace("accepted slice ending {}.{} rows: {} dataSize: {} retrievals: {}",
                        cursorSecs, cursorNanos, rowsInSlice, dataSize, retrievals);
                growSlice(rowsInSlice);
                return SliceOutcome.ACCEPTED;
            }
        }

        private TabularDataUtility.TimestampDataMapSizeStats retrieveSlice(
                long beginSecs, long beginNanos, long endSecs, long endNanos,
                List<TabularDataUtility.RetentionInterval> retention) throws DpException {

            retrievals++;
            final long queryStartNanos = System.nanoTime();
            final MongoCursor<BucketDocument> cursor = mongoClient.executeQuerySamplesV2(
                    resolvedQuery, beginSecs, beginNanos, endSecs, endNanos);
            telemetry.addDbNanos(System.nanoTime() - queryStartNanos);

            // The empty-window case was screened by the clamp above, so null is a retrieval
            // failure (a database error, or a failed pvStats span read — #232 plan D8). Report it
            // as an error; treating it as an empty slice would silently return no data.
            if (cursor == null) {
                throw new DpException("executeQuerySamplesV2 returned null cursor");
            }

            try (cursor) {
                // The status filter is its own query against sampleStatusBuckets, invisible to the
                // bucket cursor, so it is timed separately -- and in a finally, so a failed read
                // still contributes the time it took to fail rather than landing in "process".
                final long statusStartNanos = System.nanoTime();
                final TabularDataUtility.SampleStatusFilter statusFilter;
                try {
                    statusFilter = statusRetentionFilter(
                            resolvedQuery, mongoClient, beginSecs, beginNanos, endSecs, endNanos);
                } finally {
                    telemetry.addDbNanos(System.nanoTime() - statusStartNanos);
                }
                // Trim against every resolved fragment intersected with the slice, not a collapsed
                // window (#207): the database filters fragments only per-bucket.
                return TabularDataUtility.addBucketsToTable(
                        tableValueMap,
                        cursor,
                        dataSize,
                        (int) Math.min(Integer.MAX_VALUE, byteBudget),
                        retention,
                        statusFilter);
            } finally {
                // In a finally so the db stage is folded in on the reject and error paths too.
                recordCursorTime(cursor);
            }
        }

        /**
         * Moves the position to the earliest fragment begin at or after {@code (secs, nanos)}.
         * Returns false when there is none, i.e. every fragment is behind the position.
         */
        private boolean jumpToNextFragmentBegin(long secs, long nanos) {
            long bestSecs = 0;
            long bestNanos = 0;
            boolean found = false;
            for (TimeInterval fragment : resolvedQuery.getRetrievalIntervals()) {
                if (TimeInterval.compareInstant(
                        fragment.getBeginSeconds(), fragment.getBeginNanos(), secs, nanos) < 0) {
                    continue;
                }
                if (!found || TimeInterval.compareInstant(
                        fragment.getBeginSeconds(), fragment.getBeginNanos(), bestSecs, bestNanos) < 0) {
                    bestSecs = fragment.getBeginSeconds();
                    bestNanos = fragment.getBeginNanos();
                    found = true;
                }
            }
            if (!found) {
                return false;
            }
            cursorSecs = bestSecs;
            cursorNanos = bestNanos;
            return true;
        }

        private void growSlice(int rowsInSlice) {
            final long target = Math.max(1, resolvedQuery.getPageSize());
            final long factor = Math.max(1L, Math.min(MAX_SLICE_GROWTH_FACTOR, target / Math.max(1, rowsInSlice)));
            try {
                sliceNanos = Math.multiplyExact(sliceNanos, factor);
            } catch (ArithmeticException ex) {
                sliceNanos = Long.MAX_VALUE;
            }
        }
    }

    /**
     * {@code (secs, nanos) + deltaNanos}, capped at {@code (capSecs, capNanos)}; saturates rather
     * than overflowing, since it only ever bounds a retrieval window.
     */
    static long[] addNanos(long secs, long nanos, long deltaNanos, long capSecs, long capNanos) {
        long endSecs;
        long endNanos;
        try {
            final long totalNanos = Math.addExact(nanos, deltaNanos % NANOS_PER_SECOND);
            endSecs = Math.addExact(Math.addExact(secs, deltaNanos / NANOS_PER_SECOND), totalNanos / NANOS_PER_SECOND);
            endNanos = totalNanos % NANOS_PER_SECOND;
        } catch (ArithmeticException ex) {
            endSecs = capSecs;
            endNanos = capNanos;
        }
        if (TimeInterval.compareInstant(endSecs, endNanos, capSecs, capNanos) > 0) {
            endSecs = capSecs;
            endNanos = capNanos;
        }
        return new long[]{endSecs, endNanos};
    }

    /**
     * Builds a V2 {@link ColumnTable} from the map rows {@code [fromRow, toRow)} of the given sorted
     * timestamp list. Every seeded column is emitted (all-unset {@link DataValue} where a PV has no
     * sample at a row, Q9). When {@code useSerializedColumns}, each column is serialized into
     * {@code serializedDataColumns} (exactly one list populated), empty encoding (Q5).
     */
    protected static ColumnTable buildColumnTable(
            TimestampDataMap tableValueMap,
            List<long[]> timestamps,
            int fromRow,
            int toRow,
            boolean useSerializedColumns) {

        final List<String> columnNames = tableValueMap.getColumnNameList();
        final TimestampList.Builder timestampListBuilder = TimestampList.newBuilder();
        final List<DataColumn.Builder> columnBuilders = new ArrayList<>(columnNames.size());
        for (String name : columnNames) {
            columnBuilders.add(DataColumn.newBuilder().setName(name));
        }

        for (int rowIndex = fromRow; rowIndex < toRow; rowIndex++) {
            final long second = timestamps.get(rowIndex)[0];
            final long nano = timestamps.get(rowIndex)[1];
            timestampListBuilder.addTimestamps(
                    Timestamp.newBuilder().setEpochSeconds(second).setNanoseconds(nano).build());
            // Release each row as it is copied into the column builders (#199), so the map shrinks
            // while the builders grow instead of both being held at full size. Safe because the
            // caller discards the map after this call: rows deferred to a later page are re-queried
            // from the resume token rather than read from here.
            //
            // A null here means this row was already drained -- the same row range was built twice,
            // or the streaming path emitted ahead of estimating. Fail loudly: sparse-filling would
            // emit a row of unset values that is indistinguishable from a legitimate all-missing
            // sample row, turning a logic error into a silently wrong query result.
            final Map<Integer, DataValue> rowValues = tableValueMap.remove(second, nano);
            if (rowValues == null) {
                throw new IllegalStateException(
                        "querySamples row at timestamp " + second + "." + nano
                                + " was already drained; each row range must be built exactly once (#199)");
            }
            for (int columnIndex = 0; columnIndex < columnBuilders.size(); columnIndex++) {
                DataValue value = rowValues.get(columnIndex);
                if (value == null) {
                    value = DataValue.newBuilder().build(); // unset => missing sample (Q9)
                }
                columnBuilders.get(columnIndex).addDataValues(value);
            }
        }

        final ColumnTable.Builder columnTableBuilder = ColumnTable.newBuilder()
                .setTimestampList(timestampListBuilder.build());

        if (useSerializedColumns) {
            for (DataColumn.Builder columnBuilder : columnBuilders) {
                final DataColumn column = columnBuilder.build();
                columnTableBuilder.addSerializedDataColumns(SerializedDataColumn.newBuilder()
                        .setName(column.getName())
                        .setPayload(column.toByteString())
                        .build());
            }
        } else {
            for (DataColumn.Builder columnBuilder : columnBuilders) {
                columnTableBuilder.addDataColumns(columnBuilder.build());
            }
        }

        return columnTableBuilder.build();
    }
}
