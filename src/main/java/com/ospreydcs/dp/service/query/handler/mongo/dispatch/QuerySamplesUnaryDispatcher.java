package com.ospreydcs.dp.service.query.handler.mongo.dispatch;

import com.ospreydcs.dp.grpc.v1.query.ColumnTable;
import com.ospreydcs.dp.grpc.v1.query.QuerySamplesResponse;
import com.ospreydcs.dp.service.common.exception.DpException;
import com.ospreydcs.dp.service.common.exception.NonScalarColumnException;
import com.ospreydcs.dp.service.common.model.TimestampDataMap;
import com.ospreydcs.dp.service.query.handler.QueryTelemetry;
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
 * <p><b>Paging (issue #274, plan D1–D3):</b> the page window {@code [windowBegin, end)} is
 * retrieved in consecutive time slices, every slice over every resolved PV, through the shared
 * {@link SliceDrain}. Slices are accepted until the map holds {@code pageSize} distinct timestamps
 * (a slice is never split, so the map may overshoot; the page is then truncated to {@code pageSize}
 * rows and the token is the first dropped timestamp), or until a slice trips the outgoing byte
 * budget (the slice is discarded whole, the page ends with the rows accumulated so far, and the
 * token is the slice's begin), or until the window is exhausted (no token). Every timestamp on a
 * page is therefore complete across every PV; before #274 a budget trip partway through the
 * PV-major cursor silently emitted the remaining PVs as all-unset.
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

    public QuerySamplesUnaryDispatcher(
            StreamObserver<QuerySamplesResponse> responseObserver, QueryTelemetry telemetry) {
        this(responseObserver, MongoQueryHandler.getOutgoingMessageSizeLimitBytes(),
                MongoQueryHandler.getQuerySamplesInitialSliceNanos(), telemetry);
    }

    /** Package/test constructor allowing the outgoing message-size budget to be injected. */
    public QuerySamplesUnaryDispatcher(
            StreamObserver<QuerySamplesResponse> responseObserver, long byteBudget,
            QueryTelemetry telemetry) {
        this(responseObserver, byteBudget, MongoQueryHandler.getQuerySamplesInitialSliceNanos(), telemetry);
    }

    /** Test constructor allowing the byte budget and the initial slice length to be injected. */
    public QuerySamplesUnaryDispatcher(
            StreamObserver<QuerySamplesResponse> responseObserver, long byteBudget,
            long initialSliceNanos, QueryTelemetry telemetry) {
        super(byteBudget, initialSliceNanos, telemetry);
        this.responseObserver = responseObserver;
    }

    @Override
    public void executeAndDispatch(ResolvedQuery resolvedQuery, MongoQueryClientInterface mongoClient) {

        if (resolvedQuery.isEmptyResult()) {
            telemetry.markEmpty();
            QueryServiceImpl.sendQuerySamplesResponseEmpty(responseObserver);
            return;
        }

        // Only the window begin exists: it is where the first slice starts (and the resume point
        // on a continuation page). The upper bound is per fragment and per slice (#207, #274).
        final long[] windowBegin = computeWindowBegin(resolvedQuery);
        final long windowBeginSecs = windowBegin[0];
        final long windowBeginNanos = windowBegin[1];

        final TimestampDataMap tableValueMap = seededTable(resolvedQuery);
        final SliceDrain drain = new SliceDrain(
                resolvedQuery, mongoClient, tableValueMap, windowBeginSecs, windowBeginNanos);
        final int pageSize = resolvedQuery.getPageSize();

        long[] resumeAt = null;
        try {
            drainLoop:
            while (true) {
                switch (drain.drainNext()) {
                    case ACCEPTED -> {
                        if (tableValueMap.size() >= pageSize) {
                            // Page full. On overshoot emitPage truncates and the token is the first
                            // dropped row; on an exact fill nothing is dropped, so the token is the
                            // drain's position -- the next slice begin -- unless the window is
                            // exhausted. Without this an exactly-filled page ended the traversal
                            // with rows still unread.
                            if (tableValueMap.size() == pageSize && !drain.isExhausted()) {
                                resumeAt = new long[]{drain.resumeSecs(), drain.resumeNanos()};
                            }
                            break drainLoop;
                        }
                    }
                    case BUDGET_TRIP -> {
                        resumeAt = new long[]{drain.resumeSecs(), drain.resumeNanos()};
                        break drainLoop;
                    }
                    case EXHAUSTED -> {
                        break drainLoop;
                    }
                    case OVERSIZED -> {
                        // A single timestamp exceeds the whole byte budget: paging cannot make
                        // progress past it (the next page would re-assemble the same row and hit
                        // the same boundary forever). Error out naming the timestamp.
                        final String msg = "single querySamples row at timestamp "
                                + drain.resumeSecs() + "." + drain.resumeNanos()
                                + " exceeds the outgoing message size limit (" + byteBudget
                                + " bytes); narrow the PV set";
                        logger.error(msg);
                        telemetry.markError();
                        QueryServiceImpl.sendQuerySamplesResponseError(msg, responseObserver);
                        return;
                    }
                }
            }
        } catch (NonScalarColumnException e) {
            // Q4: scalar-only. Translate the neutral shared exception into querySamples guidance.
            final String msg = "querySamples supports scalar PVs only: PV '" + e.getPvName()
                    + "' has non-scalar column type " + e.getColumnType() + "; use queryBuckets";
            logger.debug(msg);
            telemetry.markReject();
            QueryServiceImpl.sendQuerySamplesResponseReject(msg, responseObserver);
            return;
        } catch (DpException e) {
            final String msg = "exception building sample result: " + e.getMessage();
            logger.error(msg + " id: " + responseObserver.hashCode(), e);
            telemetry.markError();
            QueryServiceImpl.sendQuerySamplesResponseError(msg, responseObserver);
            return;
        }

        emitPage(resolvedQuery, tableValueMap, resumeAt);
    }

    /**
     * Truncates the assembled map to at most {@code pageSize} distinct timestamps and emits the V2
     * ColumnTable. The {@code nextPageToken} is the first dropped timestamp when truncating,
     * otherwise {@code resumeAt} (the begin of a slice discarded on a budget trip, or the drain's
     * position after an exactly-filled page), otherwise empty.
     */
    private void emitPage(ResolvedQuery resolvedQuery, TimestampDataMap tableValueMap, long[] resumeAt) {

        final int pageSize = resolvedQuery.getPageSize();
        final List<long[]> allTimestamps = collectTimestamps(tableValueMap);

        int keepCount = allTimestamps.size();
        if (allTimestamps.size() > pageSize) {
            // count-driven page boundary: keep pageSize rows, resume at the next timestamp. Every
            // row in the map is complete across PVs (accepted slices only), so truncating at any
            // row is safe.
            keepCount = pageSize;
            resumeAt = allTimestamps.get(pageSize);
        }

        if (allTimestamps.isEmpty()) {
            // Retrieval produced no rows -- an empty window, or a status filter that excluded every
            // sample. QuerySamplesStreamDispatcher classifies the identical condition as empty, and
            // without this the unary path recorded it as success: the same operational state would
            // read as two different outcomes depending on which method the client called, and the
            // near-zero latencies would dilute the success distribution the outcome exists to keep
            // clean. Still a normal (empty) payload, not an ExceptionalResult.
            telemetry.markEmpty();
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

        // Size the response actually sent, not the nested result; see QueryBucketsUnaryDispatcher.
        telemetry.recordResponse(
                QueryServiceImpl.sendQuerySamplesResponse(result, responseObserver)
                        .getSerializedSize());
    }
}
