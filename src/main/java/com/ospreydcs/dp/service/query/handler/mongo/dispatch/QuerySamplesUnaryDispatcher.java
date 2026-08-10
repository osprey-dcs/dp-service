package com.ospreydcs.dp.service.query.handler.mongo.dispatch;

import com.mongodb.client.MongoCursor;
import com.ospreydcs.dp.grpc.v1.query.ColumnTable;
import com.ospreydcs.dp.grpc.v1.query.QuerySamplesResponse;
import com.ospreydcs.dp.service.common.bson.bucket.BucketDocument;
import com.ospreydcs.dp.service.common.exception.DpException;
import com.ospreydcs.dp.service.common.exception.NonScalarColumnException;
import com.ospreydcs.dp.service.common.model.TimestampDataMap;
import com.ospreydcs.dp.service.common.utility.TabularDataUtility;
import com.ospreydcs.dp.service.query.handler.model.KeysetPosition;
import com.ospreydcs.dp.service.query.handler.model.ResolvedQuery;
import com.ospreydcs.dp.service.query.handler.mongo.MongoQueryHandler;
import com.ospreydcs.dp.service.query.handler.mongo.client.MongoQueryClientInterface;
import com.ospreydcs.dp.service.query.handler.paging.PageToken;
import com.ospreydcs.dp.service.query.service.QueryServiceImpl;
import io.grpc.stub.StreamObserver;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;

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
 * <p><b>Column seeding (Q9)</b> and the V2 {@link ColumnTable} build (incl. useSerializedColumns, Q5)
 * are shared with the streaming dispatcher via {@link AbstractQuerySamplesDispatcher}.
 *
 * <p><b>Non-scalar reject (Q4):</b> a {@link NonScalarColumnException} during assembly becomes a
 * clean {@code querySamples}-specific reject naming the PV and pointing at {@code queryBuckets}.
 *
 * <p><b>excludeColumnMetadata (Q8)</b> is inert — the tabular path carries no column metadata.
 */
public class QuerySamplesUnaryDispatcher extends AbstractQuerySamplesDispatcher {

    private static final Logger logger = LogManager.getLogger();

    private final StreamObserver<QuerySamplesResponse> responseObserver;

    public QuerySamplesUnaryDispatcher(StreamObserver<QuerySamplesResponse> responseObserver) {
        this(responseObserver, MongoQueryHandler.getOutgoingMessageSizeLimitBytes());
    }

    /** Package/test constructor allowing the outgoing message-size budget to be injected. */
    public QuerySamplesUnaryDispatcher(StreamObserver<QuerySamplesResponse> responseObserver, long byteBudget) {
        super(byteBudget);
        this.responseObserver = responseObserver;
    }

    @Override
    public void executeAndDispatch(ResolvedQuery resolvedQuery, MongoQueryClientInterface mongoClient) {

        if (resolvedQuery.isEmptyResult()) {
            QueryServiceImpl.sendQuerySamplesResponseEmpty(responseObserver);
            return;
        }

        // Only the window begin exists: it bounds the database retrieval (and is the resume point on a
        // continuation page). The upper bound is per-fragment, applied by retentionIntervals() (#207).
        final long[] windowBegin = computeWindowBegin(resolvedQuery);
        final long windowBeginSecs = windowBegin[0];
        final long windowBeginNanos = windowBegin[1];

        final MongoCursor<BucketDocument> cursor =
                mongoClient.executeQuerySamplesV2(resolvedQuery, windowBeginSecs, windowBeginNanos);

        if (cursor == null) {
            // no buckets overlap the window → empty page (last page)
            QueryServiceImpl.sendQuerySamplesResponseEmpty(responseObserver);
            return;
        }

        final TimestampDataMap tableValueMap = seededTable(resolvedQuery);

        final boolean byteBudgetHit;
        try (cursor) {
            // Trim against every resolved fragment, not the collapsed window (#207): the database
            // filters fragments only per-bucket, so a bucket spanning a gap arrives with its in-gap
            // samples intact.
            final TabularDataUtility.TimestampDataMapSizeStats sizeStats = TabularDataUtility.addBucketsToTable(
                    tableValueMap,
                    cursor,
                    0,
                    (int) Math.min(Integer.MAX_VALUE, byteBudget),
                    retentionIntervals(resolvedQuery, windowBeginSecs, windowBeginNanos));
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
        final List<long[]> allTimestamps = collectTimestamps(tableValueMap);

        int keepCount = allTimestamps.size();
        long[] resumeAt = null;

        if (allTimestamps.size() > pageSize) {
            // count-driven page boundary: keep pageSize rows, resume at the next timestamp
            keepCount = pageSize;
            resumeAt = allTimestamps.get(pageSize);
        } else if (byteBudgetHit && !allTimestamps.isEmpty()) {
            // byte-driven boundary: the last assembled timestamp may be incomplete (later buckets
            // that would contribute to it were not drained), so drop it and resume there.
            keepCount = allTimestamps.size() - 1;
            resumeAt = allTimestamps.get(allTimestamps.size() - 1);

            // Indivisible-oversized guard (mirrors the buckets dispatchers' isIndivisibleOversized):
            // if dropping the last timestamp leaves nothing to emit, this single row is larger than
            // the whole byte budget. Dropping it and resuming there would re-assemble the identical
            // oversized row on the next page and hit the same boundary forever (zero forward
            // progress). Error out instead, naming the timestamp, rather than loop empty pages.
            if (keepCount == 0) {
                final String msg = "single querySamples row at timestamp "
                        + resumeAt[0] + "." + resumeAt[1]
                        + " exceeds the outgoing message size limit (" + byteBudget
                        + " bytes); narrow the PV set or time range";
                logger.error(msg);
                QueryServiceImpl.sendQuerySamplesResponseError(msg, responseObserver);
                return;
            }
        }

        final ColumnTable columnTable = buildColumnTable(
                tableValueMap, allTimestamps, 0, keepCount, resolvedQuery.isUseSerializedColumns());

        final String nextPageToken = (resumeAt != null)
                ? PageToken.encode(KeysetPosition.ofSample(resumeAt[0], resumeAt[1]))
                : "";

        final QuerySamplesResponse.SampleQueryResult result =
                QuerySamplesResponse.SampleQueryResult.newBuilder()
                        .setColumnTable(columnTable)
                        .setNextPageToken(nextPageToken)
                        .build();

        QueryServiceImpl.sendQuerySamplesResponse(result, responseObserver);
    }
}
