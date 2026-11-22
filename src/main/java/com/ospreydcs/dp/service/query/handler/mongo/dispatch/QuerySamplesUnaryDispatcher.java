package com.ospreydcs.dp.service.query.handler.mongo.dispatch;

import com.ospreydcs.dp.grpc.v1.query.ColumnTable;
import com.ospreydcs.dp.grpc.v1.query.QuerySamplesResponse;
import com.ospreydcs.dp.service.common.exception.DpException;
import com.ospreydcs.dp.service.common.grpc.OutboundReadinessGate;
import com.ospreydcs.dp.service.query.handler.QueryTelemetry;
import com.ospreydcs.dp.service.query.handler.model.KeysetPosition;
import com.ospreydcs.dp.service.query.handler.model.ResolvedQuery;
import com.ospreydcs.dp.service.query.handler.mongo.MongoQueryHandler;
import com.ospreydcs.dp.service.query.handler.mongo.client.MongoQueryClientInterface;
import com.ospreydcs.dp.service.query.handler.paging.PageToken;
import com.ospreydcs.dp.service.query.service.QueryServiceImpl;
import io.grpc.stub.StreamObserver;


/**
 * Unary {@code querySamples} formatter (Q1/Q4/Q5/Q7/Q8/Q9). Assembles one bounded page of aligned
 * sample data — a column table over the resolved PV set, trimmed to the half-open query window — and
 * emits a {@code SampleQueryResult} with a timestamp-advanced {@code nextPageToken} when more rows
 * follow.
 *
 * <p><b>Paging (issue #314):</b> rows come from the shared {@link SampleRowMerger} in timestamp
 * order, each complete across every PV, and are added to the page until it holds {@code pageSize}
 * rows or the next row would take it over the outgoing byte budget (which bounds the
 * {@code ColumnTable}; see {@code RowChunk}). The token is the timestamp of
 * the first row not included, so one row is read past the page to learn whether there is one. A
 * page that ends because the result is exhausted has no token.
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

    public QuerySamplesUnaryDispatcher(
            StreamObserver<QuerySamplesResponse> responseObserver, QueryTelemetry telemetry) {
        this(responseObserver, MongoQueryHandler.getOutgoingMessageSizeLimitBytes(), telemetry);
    }

    /** Package/test constructor allowing the outgoing message-size budget to be injected. */
    public QuerySamplesUnaryDispatcher(
            StreamObserver<QuerySamplesResponse> responseObserver, long byteBudget,
            QueryTelemetry telemetry) {
        super(responseObserver, byteBudget, telemetry);
    }

    @Override
    public void executeAndDispatch(ResolvedQuery resolvedQuery, MongoQueryClientInterface mongoClient) {

        if (resolvedQuery.isEmptyResult()) {
            telemetry.markEmpty();
            QueryServiceImpl.sendQuerySamplesResponseEmpty(responseObserver);
            return;
        }

        // Only the window begin exists: the first fragment's begin, or the resume point on a
        // continuation page. The upper bound is per fragment (#207).
        final long[] windowBegin = computeWindowBegin(resolvedQuery);
        final int pageSize = resolvedQuery.getPageSize();
        final RowChunk page = new RowChunk(resolvedQuery.getPvNames(), resolvedQuery.isUseSerializedColumns());

        long[] resumeAt = null;
        SampleRowMerger merger = null;
        try {
            merger = openMerger(resolvedQuery, mongoClient, windowBegin[0], windowBegin[1],
                    () -> OutboundReadinessGate.isCancelled(responseObserver));

            SampleRowMerger.Row row;
            while ((row = merger.nextRow()) != null) {
                final long rowBytes = page.rowBytes(row);
                if (page.rowCount() == 0 && page.emptyBytes() + rowBytes > byteBudget) {
                    // A single timestamp exceeds the whole byte budget: paging cannot make
                    // progress past it (the next page would meet the same row). Error out naming
                    // the timestamp.
                    sendOversizedRowError(row.seconds, row.nanos, page.emptyBytes() + rowBytes);
                    return;
                }
                if (page.rowCount() >= pageSize || page.bytes() + rowBytes > byteBudget) {
                    // the page is full and this row exists, so it is where the next page starts
                    resumeAt = new long[]{row.seconds, row.nanos};
                    break;
                }
                page.add(row, rowBytes);
            }

            if (merger.wasCancelled()) {
                // the client has gone; there is no one to answer
                telemetry.markAbandoned();
                return;
            }
        } catch (DpException e) {
            sendDrainFailure(e); // scalar-only reject (Q4), or error
            return;
        } finally {
            closeMerger(merger);
        }

        emitPage(resolvedQuery, page, resumeAt);
    }

    /**
     * Emits the page as a V2 ColumnTable. {@code resumeAt} is the timestamp of the first row not
     * included, or null when the result is exhausted.
     */
    private void emitPage(ResolvedQuery resolvedQuery, RowChunk page, long[] resumeAt) {

        if (page.rowCount() == 0) {
            // Retrieval produced no rows -- an empty window, or a status filter that excluded every
            // sample. QuerySamplesStreamDispatcher classifies the identical condition as empty, and
            // without this the unary path recorded it as success: the same operational state would
            // read as two different outcomes depending on which method the client called, and the
            // near-zero latencies would dilute the success distribution the outcome exists to keep
            // clean. Still a normal (empty) payload, not an ExceptionalResult.
            telemetry.markEmpty();
        }

        final ColumnTable columnTable = page.buildAndReset(resolvedQuery.isUseSerializedColumns());

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
