package com.ospreydcs.dp.service.query.handler.mongo.dispatch;

import com.mongodb.client.MongoCursor;
import com.ospreydcs.dp.grpc.v1.common.DataColumn;
import com.ospreydcs.dp.grpc.v1.common.DataValue;
import com.ospreydcs.dp.grpc.v1.common.SerializedDataColumn;
import com.ospreydcs.dp.grpc.v1.common.Timestamp;
import com.ospreydcs.dp.grpc.v1.common.TimestampList;
import com.ospreydcs.dp.grpc.v1.query.ColumnTable;
import com.ospreydcs.dp.grpc.v1.query.QuerySamplesResponse;
import com.ospreydcs.dp.service.common.bson.bucket.BucketDocument;
import com.ospreydcs.dp.service.common.exception.DpException;
import com.ospreydcs.dp.service.common.exception.NonScalarColumnException;
import com.ospreydcs.dp.service.common.grpc.OutboundReadinessGate;
import com.ospreydcs.dp.service.common.utility.TabularDataUtility;
import com.ospreydcs.dp.service.query.handler.QueryTelemetry;
import com.ospreydcs.dp.service.query.handler.model.KeysetPosition;
import com.ospreydcs.dp.service.query.handler.model.ResolvedQuery;
import com.ospreydcs.dp.service.query.handler.model.TimeInterval;
import com.ospreydcs.dp.service.query.handler.mongo.client.MongoQueryClientInterface;
import com.ospreydcs.dp.service.query.service.QueryServiceImpl;
import io.grpc.stub.StreamObserver;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.function.BooleanSupplier;

/**
 * Shared base for the Query API V2 sample dispatchers (unary {@link QuerySamplesUnaryDispatcher} and
 * streaming {@link QuerySamplesStreamDispatcher}). Holds the outgoing message-size budget, opens
 * the time-ordered retrieval and its {@link SampleRowMerger} (issue #314), and builds the outgoing
 * {@link ColumnTable} row by row ({@link RowChunk}), so the two dispatchers differ only in what
 * they do when a chunk is full: end the page with a token, or send it and start another.
 *
 * <h2>Why rows are merged from a time-ordered read (issue #314)</h2>
 *
 * <p>A row of the result is one timestamp across every resolved PV, so a row can be sent only
 * once every PV's sample at that timestamp has been read. The PV-major bucket cursor the other
 * query paths use cannot tell when that is (#274), and the first fix retrieved the window in time
 * slices, each a separate query over every PV, buffered whole in a pivot map and accepted or
 * discarded whole. The slice length was a guess: a quiet stretch inflated it, the dense data
 * after it was then read at the inflated length up to the full byte budget, and the map held
 * 24 to 34 times the wire size of what it buffered. A few concurrent long-range streams ran the
 * service out of heap.
 *
 * <p>The samples paths now read the buckets once, ordered by start time across PVs, and
 * {@link SampleRowMerger} emits each row as soon as no unread bucket can add to it. Rows go
 * straight into the outgoing message builders. Nothing is discarded or read twice, and what a
 * request holds is the buckets overlapping its current position, one small cursor batch per PV
 * group, and one outgoing chunk.
 */
public abstract class AbstractQuerySamplesDispatcher extends QueryV2Dispatcher {

    private static final Logger logger = LogManager.getLogger();

    private static final long NANOS_PER_SECOND = 1_000_000_000L;

    protected final StreamObserver<QuerySamplesResponse> responseObserver;
    protected final long byteBudget;

    protected AbstractQuerySamplesDispatcher(
            StreamObserver<QuerySamplesResponse> responseObserver, long byteBudget, QueryTelemetry telemetry) {
        super(telemetry);
        this.responseObserver = responseObserver;
        this.byteBudget = byteBudget;
    }

    // ---- exceptional responses, shared so the two dispatchers cannot drift ----------------------

    /**
     * True when the client has cancelled the call, so an exceptional response has no reader
     * (#326). The outcome is still recorded by the caller: the request failed, or was wrong,
     * whether or not anyone is left to be told. Cancellation only, not readiness: one small
     * message to a slow reader is cheaper than holding a worker for it, and a reader that
     * resumes should find its error. Also spares a unary call, which has no cancel handler, the
     * {@code StatusRuntimeException} gRPC throws from {@code onNext} on a cancelled call.
     */
    private boolean clientCancelled() {
        return OutboundReadinessGate.isCancelled(responseObserver);
    }

    /**
     * Sends an error response, recording the outcome; {@code cause} may be null. Nothing is sent
     * and the call is not completed when the client has cancelled; the outcome stays
     * {@code error}.
     */
    protected void sendError(String msg, Throwable cause) {
        if (cause != null) {
            logger.error(msg + " id: " + responseObserver.hashCode(), cause);
        } else {
            logger.error(msg + " id: " + responseObserver.hashCode());
        }
        telemetry.markError();
        if (clientCancelled()) {
            return;
        }
        QueryServiceImpl.sendQuerySamplesResponseError(msg, responseObserver);
    }

    /**
     * Sends a reject response (a client mistake, #235), recording the outcome. Nothing is sent
     * and the call is not completed when the client has cancelled; the outcome stays
     * {@code reject}.
     */
    protected void sendReject(String msg) {
        logger.debug(msg);
        telemetry.markReject();
        if (clientCancelled()) {
            return;
        }
        QueryServiceImpl.sendQuerySamplesResponseReject(msg, responseObserver);
    }

    /**
     * The indivisible-oversized error: a single timestamp's row is larger than the whole outgoing
     * message budget, so neither paging nor chunking can make progress past it. Only reducing the
     * PV count can help -- the row is one instant, so narrowing the time range cannot.
     */
    protected void sendOversizedRowError(long seconds, long nanos, long rowBytes) {
        sendError("single querySamples row at timestamp " + seconds + "." + nanos
                + " exceeds the outgoing message size limit (" + rowBytes + " > " + byteBudget
                + " bytes); narrow the PV set", null);
    }

    /** Classifies an exception out of the merge: scalar-only reject (Q4), otherwise error. */
    protected void sendDrainFailure(DpException e) {
        if (e instanceof NonScalarColumnException nonScalar) {
            sendReject("querySamples supports scalar PVs only: PV '" + nonScalar.getPvName()
                    + "' has non-scalar column type " + nonScalar.getColumnType() + "; use queryBuckets");
        } else {
            sendError("exception building sample result: " + e.getMessage(), e);
        }
    }

    /**
     * The page/stream window <em>begin</em> for the resolved query: the resume timestamp (from a
     * continuation token) or, on the first page, the earliest fragment begin. Returns
     * {@code {beginSecs, beginNanos}}.
     *
     * <p>Deliberately begin-only. There is no corresponding window <em>end</em> for retention,
     * because there is no single upper bound that is correct to filter samples on: the resolved
     * fragments may be disjoint, and a collapsed {@code [min begin, max end)} window spans the gaps
     * between them. Filtering samples against such a window is precisely the #207 defect. Do not
     * reintroduce a window end.
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
     * Resolves the query's sampleStatusSelector to a {@link TabularDataUtility.SampleStatusFilter}
     * for per-sample filtering over one window, or {@code null} when the request carries no
     * selector. The per-PV matching-timestamp sets come from the sampleStatusBuckets collection;
     * {@link SampleRowMerger} asks for consecutive windows as it advances. Returns null as well
     * when the window holds more than {@code maxTimestamps} matching timestamps, which is what
     * bounds the join input; the merger then asks for a shorter window. Composition with the
     * configurationSelector is by intersection: this filter and the fragment retention test are
     * both applied to each sample.
     *
     * @throws DpException on a database error or malformed stored status document — never silently
     *     degraded to "no statuses", which in EXCLUDE mode would return filtered-out samples
     */
    protected static TabularDataUtility.SampleStatusFilter statusRetentionFilter(
            ResolvedQuery resolvedQuery,
            MongoQueryClientInterface mongoClient,
            long windowBeginSecs, long windowBeginNanos,
            long windowEndSecs, long windowEndNanos,
            int maxTimestamps) throws DpException {

        if (resolvedQuery.getStatusFilter() == null) {
            return null;
        }
        final MongoQueryClientInterface.StatusTimestamps resolved = mongoClient.resolveSampleStatusTimestamps(
                resolvedQuery, windowBeginSecs, windowBeginNanos, windowEndSecs, windowEndNanos, maxTimestamps);
        if (resolved == null) {
            throw new DpException("sample status selector resolution failed (database error)");
        }
        if (resolved.overLimit()) {
            return null;
        }
        return new TabularDataUtility.SampleStatusFilter(
                resolvedQuery.getStatusFilter().includeMode(), resolved.matchingTimestampsByPv());
    }

    // ---- time-ordered retrieval and merge (issue #314) -------------------------------------------

    /**
     * Closes a retrieval cursor from a {@code finally} without the suppression bookkeeping of
     * try-with-resources (#314). A {@code RuntimeException} from {@code close()} is logged and
     * dropped, so it cannot replace a throwable already propagating, and after a clean read the
     * rows are already built. An {@code Error} is left to propagate: it supersedes whatever was in
     * flight, which is correct for the {@code OutOfMemoryError} this exists for, and nothing here
     * calls {@code addSuppressed}.
     */
    static void closeQuietly(MongoCursor<?> cursor) {
        try {
            cursor.close();
        } catch (RuntimeException ex) {
            logger.error("error closing querySamples cursor: {}", ex.getMessage(), ex);
        }
    }

    /**
     * Opens the time-ordered retrieval from {@code (windowBeginSecs, windowBeginNanos)} and wraps
     * it in a {@link SampleRowMerger}. The fragments the merger retains samples against come from
     * {@link TimeInterval#clampToWindowBegin}, the call the client builds its database filter
     * from, so the two cannot disagree (#207).
     *
     * <p>When no fragment lies at or after the window begin (a continuation token at the end of
     * the range) the merger has no cursor and produces no rows, at no database cost. Otherwise a
     * null cursor is a retrieval failure -- a database error or a failed pvStats span read -- and
     * is reported as an error; treating it as empty would silently return no data.
     *
     * <p>The caller must pass the result to {@link #closeMerger} in a {@code finally}.
     *
     * @param cancelled asked before each bucket read and each row (plan D6, D9)
     */
    protected SampleRowMerger openMerger(
            ResolvedQuery resolvedQuery,
            MongoQueryClientInterface mongoClient,
            long windowBeginSecs, long windowBeginNanos,
            BooleanSupplier cancelled) throws DpException {

        final List<TimeInterval> fragments = TimeInterval.clampToWindowBegin(
                resolvedQuery.getRetrievalIntervals(), windowBeginSecs, windowBeginNanos);

        MongoCursor<BucketDocument> cursor = null;
        if (!fragments.isEmpty()) {
            final long queryStartNanos = System.nanoTime();
            cursor = mongoClient.executeQuerySamplesTimeOrdered(resolvedQuery, windowBeginSecs, windowBeginNanos);
            telemetry.addDbNanos(System.nanoTime() - queryStartNanos);
            if (cursor == null) {
                throw new DpException("executeQuerySamplesTimeOrdered returned null cursor");
            }
        }

        // The status filter is its own query against sampleStatusBuckets, invisible to the bucket
        // cursor, so each window is timed separately -- in a finally, so a failed read still
        // contributes the time it took to fail rather than landing in "process".
        final SampleRowMerger.StatusWindowLoader statusWindowLoader = (resolvedQuery.getStatusFilter() == null)
                ? null
                : (beginSecs, beginNanos, endSecs, endNanos, maxTimestamps) -> {
                    final long statusStartNanos = System.nanoTime();
                    try {
                        return statusRetentionFilter(
                                resolvedQuery, mongoClient, beginSecs, beginNanos, endSecs, endNanos,
                                maxTimestamps);
                    } finally {
                        telemetry.addDbNanos(System.nanoTime() - statusStartNanos);
                    }
                };

        return new SampleRowMerger(cursor, resolvedQuery.getPvNames(), fragments, statusWindowLoader, cancelled);
    }

    /**
     * Closes the merger's cursor and folds its time into the request's {@code db} stage. For a
     * {@code finally}: safe with a null merger (the open failed) and a merger with no cursor.
     */
    protected void closeMerger(SampleRowMerger merger) {
        if (merger == null || merger.cursor() == null) {
            return;
        }
        try {
            closeQuietly(merger.cursor());
        } finally {
            recordCursorTime(merger.cursor());
        }
    }

    /**
     * The outgoing {@link ColumnTable} under construction: one column per resolved PV, in resolved
     * order, filled a row at a time. The byte figure is a conservative <b>upper bound</b> on the
     * serialized size of the table built so far, so a dispatcher can stop before a row that would
     * take the message over the wire limit; it must not under-count.
     *
     * <p>It has two parts. What a table costs however many rows it holds (every column's name and
     * framing) is charged once, to the empty chunk: {@link #emptyBytes()}. What a row adds (its
     * timestamp and one value per column) is {@link #rowBytes}. Charging the names to every row,
     * as this did before the PR #323 review, counted a 20-character name and a double at about 43
     * bytes against 11 on the wire, so a wide byte-limited message went out about a quarter full
     * and the oversized-row error fired at about a quarter of the PV count that really overflows.
     *
     * <p>The bound covers the {@code ColumnTable} only. The response around it adds the
     * {@code responseTime}, the page token and message framing, on the order of 100 bytes, which
     * the default limit's margin below gRPC's 4 MiB maximum absorbs.
     */
    protected static final class RowChunk {

        private final List<String> columnNames;
        private final long emptyBytes;
        private TimestampList.Builder timestamps;
        private List<DataColumn.Builder> columns;
        private int rowCount;
        private long bytes;

        RowChunk(List<String> columnNames, boolean useSerializedColumns) {
            this.columnNames = columnNames;
            // Each emitted column carries its UTF-8 name and repeated-field framing however many
            // rows it holds; serialized columns pay it twice (the inner DataColumn name and the
            // outer SerializedDataColumn name and payload). The 16 is the timestamp list's own
            // tag and length, with room to spare.
            long overhead = 16;
            for (String name : columnNames) {
                final long nameBytes = name.getBytes(StandardCharsets.UTF_8).length;
                overhead += nameBytes + 8;
                if (useSerializedColumns) {
                    overhead += nameBytes + 8;
                }
            }
            this.emptyBytes = overhead;
            reset();
        }

        private void reset() {
            timestamps = TimestampList.newBuilder();
            columns = new ArrayList<>(columnNames.size());
            for (String name : columnNames) {
                columns.add(DataColumn.newBuilder().setName(name));
            }
            rowCount = 0;
            bytes = emptyBytes;
        }

        int rowCount() {
            return rowCount;
        }

        /** Upper bound on the table built so far; {@link #emptyBytes()} when it has no rows. */
        long bytes() {
            return bytes;
        }

        /** Upper bound on a table of these columns with no rows. */
        long emptyBytes() {
            return emptyBytes;
        }

        /** Upper bound on what {@code row} adds to the table: see the class comment. */
        long rowBytes(SampleRowMerger.Row row) {
            // Timestamp: two int64 fields (up to 10 bytes varint each) plus tags and length framing.
            long rowBytes = 24;
            for (DataValue value : row.values) {
                // the value plus its repeated-field tag and length; an unset value still costs framing
                rowBytes += ((value != null) ? value.getSerializedSize() : 0) + 6;
            }
            return rowBytes;
        }

        /** Appends {@code row}; {@code rowBytes} is the figure {@link #rowBytes} returned for it. */
        void add(SampleRowMerger.Row row, long rowBytes) {
            timestamps.addTimestamps(
                    Timestamp.newBuilder().setEpochSeconds(row.seconds).setNanoseconds(row.nanos).build());
            for (int columnIndex = 0; columnIndex < columns.size(); columnIndex++) {
                final DataValue value = row.values[columnIndex];
                // unset => missing sample (Q9)
                columns.get(columnIndex).addDataValues(value != null ? value : DataValue.getDefaultInstance());
            }
            rowCount++;
            bytes += rowBytes;
        }

        /**
         * Builds the table from the rows added so far and starts an empty one. Every resolved PV
         * gets a column, all-unset where it had no sample (Q9), including when there are no rows.
         * With {@code useSerializedColumns} each column goes into {@code serializedDataColumns}
         * with an empty encoding (Q5); exactly one of the two lists is populated.
         */
        ColumnTable buildAndReset(boolean useSerializedColumns) {
            final ColumnTable.Builder table = ColumnTable.newBuilder().setTimestampList(timestamps.build());
            for (DataColumn.Builder columnBuilder : columns) {
                final DataColumn column = columnBuilder.build();
                if (useSerializedColumns) {
                    table.addSerializedDataColumns(SerializedDataColumn.newBuilder()
                            .setName(column.getName())
                            .setPayload(column.toByteString())
                            .build());
                } else {
                    table.addDataColumns(column);
                }
            }
            reset();
            return table.build();
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
}
