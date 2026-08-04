package com.ospreydcs.dp.service.query.handler.mongo.dispatch;

import com.ospreydcs.dp.grpc.v1.common.DataColumn;
import com.ospreydcs.dp.grpc.v1.common.DataValue;
import com.ospreydcs.dp.grpc.v1.common.SerializedDataColumn;
import com.ospreydcs.dp.grpc.v1.common.Timestamp;
import com.ospreydcs.dp.grpc.v1.common.TimestampList;
import com.ospreydcs.dp.grpc.v1.query.ColumnTable;
import com.ospreydcs.dp.service.common.model.TimestampDataMap;
import com.ospreydcs.dp.service.common.utility.TabularDataUtility;
import com.ospreydcs.dp.service.query.handler.model.KeysetPosition;
import com.ospreydcs.dp.service.query.handler.model.ResolvedQuery;
import com.ospreydcs.dp.service.query.handler.model.TimeInterval;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Shared base for the Query API V2 sample dispatchers (unary {@link QuerySamplesUnaryDispatcher} and
 * streaming {@code QuerySamplesStreamDispatcher}). Holds the outgoing message-size budget and the
 * column-table assembly building blocks — page-window computation, column seeding from the resolved
 * PV list (Q9), distinct-timestamp collection, and the V2 {@link ColumnTable} builder over a row
 * range (with the useSerializedColumns handling, Q5) — so the two dispatchers differ only in how they
 * bound rows (single truncated page vs. successive row-chunks).
 */
public abstract class AbstractQuerySamplesDispatcher extends QueryV2Dispatcher {

    protected final long byteBudget;

    protected AbstractQuerySamplesDispatcher(long byteBudget) {
        this.byteBudget = byteBudget;
    }

    /**
     * Computes the page/stream window {@code [begin, end)} for the resolved query: begin = the resume
     * timestamp (from a continuation token) or the earliest fragment begin; end = the latest fragment
     * end. Returns {@code {beginSecs, beginNanos, endSecs, endNanos}}.
     */
    protected static long[] computeWindow(ResolvedQuery resolvedQuery) {
        final List<TimeInterval> intervals = resolvedQuery.getRetrievalIntervals();
        final KeysetPosition pageStart = resolvedQuery.getPageStart();

        final long beginSecs;
        final long beginNanos;
        if (pageStart != null) {
            beginSecs = pageStart.getSeconds();
            beginNanos = pageStart.getNanos();
        } else {
            beginSecs = intervals.get(0).getBeginSeconds();
            beginNanos = intervals.get(0).getBeginNanos();
        }

        long endSecs = intervals.get(0).getEndSeconds();
        long endNanos = intervals.get(0).getEndNanos();
        for (TimeInterval iv : intervals) {
            if (TimeInterval.compareInstant(iv.getEndSeconds(), iv.getEndNanos(), endSecs, endNanos) > 0) {
                endSecs = iv.getEndSeconds();
                endNanos = iv.getEndNanos();
            }
        }
        return new long[]{beginSecs, beginNanos, endSecs, endNanos};
    }

    /**
     * The sample-retention windows for the resolved query: one {@link TabularDataUtility.RetentionInterval}
     * per resolved retrieval fragment, each clamped on the left to the page window begin (the resume
     * timestamp on a continuation page), mirroring the clamping
     * {@code MongoSyncQueryClient.executeQuerySamplesV2} applies to its per-fragment database filters.
     *
     * <p>Assembly must trim against this full list rather than the single collapsed window returned by
     * {@link #computeWindow} (issue #207). The database filters fragments only at <em>bucket</em>
     * granularity, so a bucket spanning the gap between two fragments is retrieved with its in-gap
     * samples intact; trimming against the collapsed window would leave them in the result.
     */
    protected static List<TabularDataUtility.RetentionInterval> retentionIntervals(
            ResolvedQuery resolvedQuery, long windowBeginSecs, long windowBeginNanos) {

        final List<TabularDataUtility.RetentionInterval> intervals = new ArrayList<>();
        for (TimeInterval fragment : resolvedQuery.getRetrievalIntervals()) {
            final long beginSecs;
            final long beginNanos;
            if (TimeInterval.compareInstant(
                    fragment.getBeginSeconds(), fragment.getBeginNanos(),
                    windowBeginSecs, windowBeginNanos) >= 0) {
                beginSecs = fragment.getBeginSeconds();
                beginNanos = fragment.getBeginNanos();
            } else {
                beginSecs = windowBeginSecs;
                beginNanos = windowBeginNanos;
            }
            // drop fragments entirely at or before the window begin (they contribute nothing here)
            if (TimeInterval.compareInstant(
                    beginSecs, beginNanos, fragment.getEndSeconds(), fragment.getEndNanos()) >= 0) {
                continue;
            }
            intervals.add(new TabularDataUtility.RetentionInterval(
                    beginSecs, beginNanos, fragment.getEndSeconds(), fragment.getEndNanos()));
        }
        return intervals;
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
