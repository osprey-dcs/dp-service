package com.ospreydcs.dp.service.query.handler.mongo.dispatch;

import com.ospreydcs.dp.grpc.v1.query.ColumnTable;
import com.ospreydcs.dp.grpc.v1.query.QuerySamplesResponse;
import com.ospreydcs.dp.service.common.exception.DpException;
import com.ospreydcs.dp.service.common.grpc.OutboundReadinessGate;
import com.ospreydcs.dp.service.query.handler.QueryTelemetry;
import com.ospreydcs.dp.service.query.handler.model.ResolvedQuery;
import com.ospreydcs.dp.service.query.handler.mongo.MongoQueryHandler;
import com.ospreydcs.dp.service.query.handler.mongo.client.MongoQueryClientInterface;
import com.ospreydcs.dp.service.query.service.QueryServiceImpl;
import io.grpc.stub.StreamObserver;

/**
 * Server-streaming {@code querySamplesStream} formatter (Q4/Q5/Q8/Q9). Reads the result through
 * the shared {@link SampleRowMerger} (issue #314) and sends the rows in chunks of at most
 * {@code limit} timestamps, each also kept under the outgoing message-size budget: a chunk is
 * sent before adding a row that would exceed the wire limit, even if it holds fewer rows. Memory
 * is one chunk plus the buckets overlapping the merge position, however long the window.
 *
 * <p>The column set is the resolved PV list (Q9), so it is the same on every chunk.
 * {@code nextPageToken} is empty on every message (the stream signals completion via
 * {@code onCompleted}). An empty result emits a single empty message. A non-scalar PV is rejected
 * (Q4), same as the unary path.
 */
public class QuerySamplesStreamDispatcher extends AbstractQuerySamplesDispatcher {

    /**
     * Outbound flow control (#274, plan D8), built here because the handler constructs this
     * dispatcher on the gRPC thread, where the ready handler must be registered.
     */
    private final OutboundReadinessGate gate;

    public QuerySamplesStreamDispatcher(
            StreamObserver<QuerySamplesResponse> responseObserver, QueryTelemetry telemetry) {
        this(responseObserver, MongoQueryHandler.getOutgoingMessageSizeLimitBytes(), telemetry);
    }

    /** Package/test constructor allowing the outgoing message-size budget to be injected. */
    public QuerySamplesStreamDispatcher(
            StreamObserver<QuerySamplesResponse> responseObserver, long byteBudget,
            QueryTelemetry telemetry) {
        super(responseObserver, byteBudget, telemetry);
        this.gate = OutboundReadinessGate.forObserver(
                responseObserver, MongoQueryHandler.getStreamReadyTimeoutSeconds(),
                "querySamplesStream id: " + responseObserver.hashCode());
    }

    @Override
    public void executeAndDispatch(ResolvedQuery resolvedQuery, MongoQueryClientInterface mongoClient) {

        if (resolvedQuery.isEmptyResult()) {
            emitEmptyChunkAndComplete();
            return;
        }

        final long[] windowBegin = computeWindowBegin(resolvedQuery);
        final int chunkRowLimit = resolvedQuery.getPageSize();
        final boolean useSerialized = resolvedQuery.isUseSerializedColumns();
        final RowChunk chunk = new RowChunk(resolvedQuery.getPvNames(), useSerialized);

        boolean emittedAnything = false;
        SampleRowMerger merger = null;
        try {
            // Cancellation is otherwise noticed only when a chunk is sent, and a quiet stretch or
            // a status filter can mean a long read between sends (#314, plan D6).
            merger = openMerger(resolvedQuery, mongoClient, windowBegin[0], windowBegin[1], gate::isCancelled);

            SampleRowMerger.Row row;
            while ((row = merger.nextRow()) != null) {
                final long rowBytes = chunk.rowBytes(row);

                // A single row larger than the whole budget cannot be chunked. Error out naming
                // the timestamp rather than emit an over-limit message gRPC would abort on.
                if (chunk.emptyBytes() + rowBytes > byteBudget) {
                    sendOversizedRowError(row.seconds, row.nanos, chunk.emptyBytes() + rowBytes);
                    return;
                }
                // byte flush: this row would take the chunk over the budget
                if (chunk.rowCount() > 0 && chunk.bytes() + rowBytes > byteBudget) {
                    if (!emitChunk(chunk, useSerialized)) {
                        return;
                    }
                    emittedAnything = true;
                }
                chunk.add(row, rowBytes);
                // count flush: the chunk reached the per-message row limit
                if (chunk.rowCount() >= chunkRowLimit) {
                    if (!emitChunk(chunk, useSerialized)) {
                        return;
                    }
                    emittedAnything = true;
                }
            }

            if (merger.wasCancelled()) {
                // no reader: stop without completing, as on a refused send
                telemetry.markAbandoned();
                return;
            }
            if (chunk.rowCount() > 0) {
                if (!emitChunk(chunk, useSerialized)) {
                    return;
                }
                emittedAnything = true;
            }
        } catch (DpException e) {
            sendDrainFailure(e); // scalar-only reject (Q4), or error
            return;
        } finally {
            closeMerger(merger);
        }

        if (!emittedAnything) {
            emitEmptyChunkAndComplete();
            return;
        }
        responseObserver.onCompleted();
    }

    /** Sends the chunk and empties it; false when the gate refused (nothing sent). */
    private boolean emitChunk(RowChunk chunk, boolean useSerialized) {
        if (!gate.awaitReady()) {
            telemetry.markAbandoned();
            return false; // abandoned: the gate logged why
        }
        emit(chunk.buildAndReset(useSerialized));
        return true;
    }

    /**
     * Sends the single empty message of an empty result, through the gate like every other send
     * (#324). A refused send is not followed by {@code onCompleted()}: the client is gone or not
     * draining. The outcome is marked empty only after the send, because
     * {@code markAbandoned()} does not overwrite an outcome already set, so marking first
     * recorded a cancelled client as a successful empty query.
     */
    private void emitEmptyChunkAndComplete() {
        if (!gate.awaitReady()) {
            telemetry.markAbandoned();
            return; // abandoned: the gate logged why
        }
        emit(ColumnTable.getDefaultInstance());
        telemetry.markEmpty();
        responseObserver.onCompleted();
    }

    private void emit(ColumnTable columnTable) {
        final QuerySamplesResponse.SampleQueryResult result =
                QuerySamplesResponse.SampleQueryResult.newBuilder()
                        .setColumnTable(columnTable)
                        .setNextPageToken("") // stream signals completion; token always empty
                        .build();
        // Size the response actually sent, not the nested result; see QueryBucketsUnaryDispatcher.
        final QuerySamplesResponse response = QueryServiceImpl.querySamplesResponse(result);
        telemetry.recordResponse(response.getSerializedSize());
        responseObserver.onNext(response);
    }
}
