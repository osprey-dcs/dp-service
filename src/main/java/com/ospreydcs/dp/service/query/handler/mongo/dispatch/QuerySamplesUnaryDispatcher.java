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
import com.ospreydcs.dp.service.common.model.TimestampDataMap;
import com.ospreydcs.dp.service.common.utility.TabularDataUtility;
import com.ospreydcs.dp.service.query.handler.model.KeysetPosition;
import com.ospreydcs.dp.service.query.handler.model.ResolvedQuery;
import com.ospreydcs.dp.service.query.handler.model.TimeInterval;
import com.ospreydcs.dp.service.query.handler.mongo.MongoQueryHandler;
import com.ospreydcs.dp.service.query.handler.mongo.client.MongoQueryClientInterface;
import com.ospreydcs.dp.service.query.handler.paging.PageToken;
import com.ospreydcs.dp.service.query.service.QueryServiceImpl;
import io.grpc.stub.StreamObserver;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Unary {@code querySamples} formatter (Q1/Q4/Q5/Q7/Q8/Q9). Assembles one bounded page of aligned
 * sample data — a column table over the resolved PV set, trimmed to the half-open query window — and
 * emits a {@code SampleQueryResult} with a timestamp-advanced {@code nextPageToken} when more rows
 * follow.
 *
 * <p><b>Paging (Q1, drain-then-truncate):</b> query the page window {@code [windowBegin, end)},
 * assemble into a timestamp-ordered map, keep the first {@code pageSize} distinct timestamps (soft
 * cap — a timestamp is never split across pages), and set the token to the timestamp immediately
 * after the last kept row. The byte budget is a second stop condition (Q7): if assembly hits it the
 * last (possibly incomplete) timestamp is dropped and becomes the resume point, so no partial row is
 * emitted.
 *
 * <p><b>Column seeding (Q9):</b> every resolved PV gets a column (sorted by name), all-unset where it
 * has no sample at a given row, by pre-seeding the column index map from the resolved PV list.
 *
 * <p><b>Non-scalar reject (Q4):</b> a {@link NonScalarColumnException} during assembly becomes a
 * clean {@code querySamples}-specific reject naming the PV and pointing at {@code queryBuckets}.
 *
 * <p><b>excludeColumnMetadata (Q8)</b> is inert — the tabular path carries no column metadata.
 */
public class QuerySamplesUnaryDispatcher extends QueryV2Dispatcher {

    private static final Logger logger = LogManager.getLogger();

    private final StreamObserver<QuerySamplesResponse> responseObserver;
    private final long byteBudget;

    public QuerySamplesUnaryDispatcher(StreamObserver<QuerySamplesResponse> responseObserver) {
        this(responseObserver, MongoQueryHandler.getOutgoingMessageSizeLimitBytes());
    }

    /** Package/test constructor allowing the outgoing message-size budget to be injected. */
    public QuerySamplesUnaryDispatcher(StreamObserver<QuerySamplesResponse> responseObserver, long byteBudget) {
        this.responseObserver = responseObserver;
        this.byteBudget = byteBudget;
    }

    @Override
    public void executeAndDispatch(ResolvedQuery resolvedQuery, MongoQueryClientInterface mongoClient) {

        if (resolvedQuery.isEmptyResult()) {
            QueryServiceImpl.sendQuerySamplesResponseEmpty(responseObserver);
            return;
        }

        // Page window: begin = resume timestamp (from token) or the earliest fragment begin; end =
        // the latest fragment end. Fragments are disjoint and sorted, so the union window is
        // [min begin, max end); executeQuerySamplesV2 re-applies the exact per-fragment overlap.
        final List<TimeInterval> intervals = resolvedQuery.getRetrievalIntervals();
        final KeysetPosition pageStart = resolvedQuery.getPageStart();
        final long windowBeginSecs;
        final long windowBeginNanos;
        if (pageStart != null) {
            windowBeginSecs = pageStart.getSeconds();
            windowBeginNanos = pageStart.getNanos();
        } else {
            windowBeginSecs = intervals.get(0).getBeginSeconds();
            windowBeginNanos = intervals.get(0).getBeginNanos();
        }
        long windowEndSecs = intervals.get(0).getEndSeconds();
        long windowEndNanos = intervals.get(0).getEndNanos();
        for (TimeInterval iv : intervals) {
            if (TimeInterval.compareInstant(iv.getEndSeconds(), iv.getEndNanos(), windowEndSecs, windowEndNanos) > 0) {
                windowEndSecs = iv.getEndSeconds();
                windowEndNanos = iv.getEndNanos();
            }
        }

        final MongoCursor<BucketDocument> cursor =
                mongoClient.executeQuerySamplesV2(resolvedQuery, windowBeginSecs, windowBeginNanos);

        if (cursor == null) {
            // no buckets overlap the window → empty page (last page)
            QueryServiceImpl.sendQuerySamplesResponseEmpty(responseObserver);
            return;
        }

        // Seed the column set from the resolved PV list (sorted) so every resolved PV gets a column
        // in a stable order, even if it has no sample in this page (Q9).
        final TimestampDataMap tableValueMap = new TimestampDataMap();
        for (String pvName : resolvedQuery.getPvNames()) {
            tableValueMap.getColumnIndex(pvName);
        }

        final boolean byteBudgetHit;
        try (cursor) {
            final TabularDataUtility.TimestampDataMapSizeStats sizeStats = TabularDataUtility.addBucketsToTable(
                    tableValueMap,
                    cursor,
                    0,
                    (int) Math.min(Integer.MAX_VALUE, byteBudget),
                    windowBeginSecs,
                    windowBeginNanos,
                    windowEndSecs,
                    windowEndNanos);
            byteBudgetHit = sizeStats.sizeLimitExceeded();
        } catch (NonScalarColumnException e) {
            // Q4: scalar-only. Translate the neutral shared exception into querySamples guidance.
            final String msg = "querySamples supports scalar PVs only: PV '" + e.getPvName()
                    + "' has non-scalar column type " + e.getColumnType() + "; use queryBuckets";
            logger.debug(msg);
            QueryServiceImpl.sendQuerySamplesResponseReject(msg, responseObserver);
            return;
        } catch (DpException e) {
            final String msg = "exception building sample result: " + e.getMessage();
            logger.error(msg, e);
            QueryServiceImpl.sendQuerySamplesResponseError(msg, responseObserver);
            return;
        }

        emitPage(resolvedQuery, tableValueMap, byteBudgetHit);
    }

    /**
     * Truncates the assembled map to at most {@code pageSize} distinct timestamps and emits the V2
     * ColumnTable. Sets the {@code nextPageToken} to the first dropped timestamp (empty if none).
     */
    private void emitPage(ResolvedQuery resolvedQuery, TimestampDataMap tableValueMap, boolean byteBudgetHit) {

        final int pageSize = resolvedQuery.getPageSize();

        // Collect distinct (second, nano) timestamps in sorted order.
        final List<long[]> allTimestamps = new ArrayList<>();
        for (Map.Entry<Long, Map<Long, Map<Integer, DataValue>>> secondEntry : tableValueMap.entrySet()) {
            final long second = secondEntry.getKey();
            for (Long nano : secondEntry.getValue().keySet()) {
                allTimestamps.add(new long[]{second, nano});
            }
        }

        // Determine how many rows to keep and the resume point.
        int keepCount = allTimestamps.size();
        long[] resumeAt = null;

        if (allTimestamps.size() > pageSize) {
            // count-driven page boundary: keep pageSize rows, resume at the next timestamp
            keepCount = pageSize;
            resumeAt = allTimestamps.get(pageSize);
        } else if (byteBudgetHit && !allTimestamps.isEmpty()) {
            // byte-driven boundary: the last assembled timestamp may be incomplete (later buckets
            // that would contribute to it were not drained), so drop it and resume there. Keep >= 1
            // row (zero-progress guard): if only one timestamp assembled under the byte cap, we still
            // drop it and resume — the next page re-queries from it with a fresh budget.
            keepCount = allTimestamps.size() - 1;
            resumeAt = allTimestamps.get(allTimestamps.size() - 1);
        }

        // Build the V2 ColumnTable from the kept rows.
        final List<String> columnNames = tableValueMap.getColumnNameList();
        final TimestampList.Builder timestampListBuilder = TimestampList.newBuilder();
        final List<DataColumn.Builder> columnBuilders = new ArrayList<>(columnNames.size());
        for (String name : columnNames) {
            columnBuilders.add(DataColumn.newBuilder().setName(name));
        }

        for (int rowIndex = 0; rowIndex < keepCount; rowIndex++) {
            final long second = allTimestamps.get(rowIndex)[0];
            final long nano = allTimestamps.get(rowIndex)[1];
            timestampListBuilder.addTimestamps(
                    Timestamp.newBuilder().setEpochSeconds(second).setNanoseconds(nano).build());
            final Map<Integer, DataValue> rowValues = tableValueMap.get(second, nano);
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

        if (resolvedQuery.isUseSerializedColumns()) {
            // Q5: serialize each assembled column into serializedDataColumns; populate exactly one of
            // the two lists. Empty encoding — no meaningful per-column encoding for a synthesized
            // sample column; present for API symmetry with the bucket path, not as a perf optimization.
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

        final String nextPageToken = (resumeAt != null)
                ? PageToken.encode(KeysetPosition.ofSample(resumeAt[0], resumeAt[1]))
                : "";

        final QuerySamplesResponse.SampleQueryResult result =
                QuerySamplesResponse.SampleQueryResult.newBuilder()
                        .setColumnTable(columnTableBuilder.build())
                        .setNextPageToken(nextPageToken)
                        .build();

        QueryServiceImpl.sendQuerySamplesResponse(result, responseObserver);
    }
}
