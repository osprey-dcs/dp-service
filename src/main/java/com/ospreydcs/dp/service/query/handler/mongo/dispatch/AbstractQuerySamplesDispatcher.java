package com.ospreydcs.dp.service.query.handler.mongo.dispatch;

import com.ospreydcs.dp.grpc.v1.common.DataColumn;
import com.ospreydcs.dp.grpc.v1.common.DataValue;
import com.ospreydcs.dp.grpc.v1.common.SerializedDataColumn;
import com.ospreydcs.dp.grpc.v1.common.Timestamp;
import com.ospreydcs.dp.grpc.v1.common.TimestampList;
import com.ospreydcs.dp.grpc.v1.query.ColumnTable;
import com.ospreydcs.dp.service.common.exception.DpException;
import com.ospreydcs.dp.service.common.model.TimestampDataMap;
import com.ospreydcs.dp.service.common.utility.TabularDataUtility;
import com.ospreydcs.dp.service.query.handler.model.KeysetPosition;
import com.ospreydcs.dp.service.query.handler.model.ResolvedQuery;
import com.ospreydcs.dp.service.query.handler.model.TimeInterval;
import com.ospreydcs.dp.service.query.handler.mongo.client.MongoQueryClientInterface;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

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
     * The page/stream window <em>begin</em> for the resolved query: the resume timestamp (from a
     * continuation token) or, on the first page, the earliest fragment begin. Returns
     * {@code {beginSecs, beginNanos}}.
     *
     * <p>Deliberately begin-only. There is no corresponding window <em>end</em>, because there is no
     * single upper bound that is correct to filter on: the resolved fragments may be disjoint, and a
     * collapsed {@code [min begin, max end)} window spans the gaps between them. Filtering samples
     * against such a window is precisely the #207 defect. The upper bound is applied per fragment, by
     * {@link #retentionIntervals}; do not reintroduce a window end here.
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
     * The sample-retention windows for the resolved query: one {@link TabularDataUtility.RetentionInterval}
     * per resolved retrieval fragment, each clamped on the left to the page window begin (the resume
     * timestamp on a continuation page).
     *
     * <p>Assembly must trim against this full list rather than a single collapsed
     * {@code [min begin, max end)} window (issue #207). The database filters fragments only at
     * <em>bucket</em> granularity, so a bucket spanning the gap between two fragments is retrieved with
     * its in-gap samples intact; trimming against a collapsed window would leave them in the result.
     *
     * <p>The clamp itself comes from {@link TimeInterval#clampToWindowBegin}, the same call
     * {@code MongoSyncQueryClient.executeQuerySamplesV2} uses to build its per-fragment database
     * filters — so the retrieval filter and this trim cannot drift apart.
     */
    protected static List<TabularDataUtility.RetentionInterval> retentionIntervals(
            ResolvedQuery resolvedQuery, long windowBeginSecs, long windowBeginNanos) {

        final List<TabularDataUtility.RetentionInterval> intervals = new ArrayList<>();
        for (TimeInterval fragment : TimeInterval.clampToWindowBegin(
                resolvedQuery.getRetrievalIntervals(), windowBeginSecs, windowBeginNanos)) {
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
     * clamped page window the bucket retrieval uses, so the join input covers exactly the samples
     * that can appear on this page. Composition with the configurationSelector is by intersection:
     * this filter and the fragment retention test are both applied in the same per-sample retention
     * decision.
     *
     * @throws DpException on a database error or malformed stored status document — never silently
     *     degraded to "no statuses", which in EXCLUDE mode would return filtered-out samples
     */
    protected static TabularDataUtility.SampleStatusFilter statusRetentionFilter(
            ResolvedQuery resolvedQuery,
            MongoQueryClientInterface mongoClient,
            long windowBeginSecs,
            long windowBeginNanos) throws DpException {

        if (resolvedQuery.getStatusFilter() == null) {
            return null;
        }
        final Map<String, Set<Long>> matchingTimestampsByPv =
                mongoClient.resolveSampleStatusTimestamps(resolvedQuery, windowBeginSecs, windowBeginNanos);
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
