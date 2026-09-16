package com.ospreydcs.dp.service.query.handler.mongo.dispatch;

import com.mongodb.client.MongoCursor;
import com.ospreydcs.dp.grpc.v1.common.DataBucket;
import com.ospreydcs.dp.grpc.v1.query.QueryBucketsResponse;
import com.ospreydcs.dp.service.common.bson.bucket.BucketDocument;
import com.ospreydcs.dp.service.common.exception.DpException;
import com.ospreydcs.dp.service.common.grpc.OutboundReadinessGate;
import com.ospreydcs.dp.service.query.handler.QueryTelemetry;
import com.ospreydcs.dp.service.query.handler.model.ResolvedQuery;
import com.ospreydcs.dp.service.query.handler.mongo.MongoQueryHandler;
import com.ospreydcs.dp.service.query.handler.mongo.client.MongoQueryClientInterface;
import com.ospreydcs.dp.service.query.service.QueryServiceImpl;
import io.grpc.stub.StreamObserver;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.List;

/**
 * Server-streaming {@code queryBucketsStream} formatter (Q7/Q8). Fire-and-consume: streams the whole
 * result of the (resolved intervals × PV list) overlap query to exhaustion, emitting
 * {@code BucketQueryResult} messages chunked by {@code limit} (per-message bucket count) and the
 * outgoing message-size budget — whichever bounds a chunk first.
 *
 * <p>{@code nextPageToken} is empty on every message (the stream itself signals completion via
 * {@code onCompleted}). An empty result emits a single empty message, then completes. A single bucket
 * larger than the whole budget is an indivisible-oversized error. Representation-flag handling is
 * identical to the unary dispatcher (shared via {@link AbstractQueryBucketsDispatcher}).
 */
public class QueryBucketsStreamDispatcher extends AbstractQueryBucketsDispatcher {

    private static final Logger logger = LogManager.getLogger();

    private final StreamObserver<QueryBucketsResponse> responseObserver;
    /**
     * Outbound flow control (#274, plan D8), built here because the handler constructs this
     * dispatcher on the gRPC thread, where the ready handler must be registered.
     */
    private final OutboundReadinessGate gate;

    public QueryBucketsStreamDispatcher(
            StreamObserver<QueryBucketsResponse> responseObserver, QueryTelemetry telemetry) {
        this(responseObserver, MongoQueryHandler.getOutgoingMessageSizeLimitBytes(), telemetry);
    }

    /** Package/test constructor allowing the outgoing message-size budget to be injected. */
    public QueryBucketsStreamDispatcher(
            StreamObserver<QueryBucketsResponse> responseObserver, long byteBudget,
            QueryTelemetry telemetry) {
        super(byteBudget, telemetry);
        this.responseObserver = responseObserver;
        this.gate = OutboundReadinessGate.forObserver(
                responseObserver, MongoQueryHandler.getStreamReadyTimeoutSeconds(),
                "queryBucketsStream id: " + responseObserver.hashCode());
    }

    @Override
    public void executeAndDispatch(ResolvedQuery resolvedQuery, MongoQueryClientInterface mongoClient) {

        // A query that resolves to no PVs or no retrieval intervals yields a single empty message.
        if (resolvedQuery.isEmptyResult()) {
            telemetry.markEmpty();
            emitChunk(new ArrayList<>());
            responseObserver.onCompleted();
            return;
        }

        final long queryStartNanos = System.nanoTime();
        final MongoCursor<BucketDocument> cursor = mongoClient.executeQueryBucketsV2Stream(resolvedQuery);
        telemetry.addDbNanos(System.nanoTime() - queryStartNanos);
        if (cursor == null) {
            final String msg = "executeQueryBucketsV2Stream returned null cursor";
            logger.error(msg + " id: " + responseObserver.hashCode());
            telemetry.markError();
            QueryServiceImpl.sendQueryBucketsResponseError(msg, responseObserver);
            return;
        }

        try (cursor) {
            if (!cursor.hasNext()) {
                telemetry.markEmpty();
                emitChunk(new ArrayList<>());
                responseObserver.onCompleted();
                return;
            }

            // limit == per-message chunk size (count); combined with the byte budget as the two flush
            // triggers. pageSize is normalized (default/clamped) by the resolver.
            final int chunkSizeLimit = resolvedQuery.getPageSize();

            final List<DataBucket> chunk = new ArrayList<>();
            long chunkBytes = 0;

            while (cursor.hasNext()) {
                final BucketDocument document = cursor.next();

                final DataBucket bucket;
                try {
                    bucket = buildBucket(document, resolvedQuery);
                } catch (DpException e) {
                    final String msg = "exception building bucket result: " + e.getMessage();
                    logger.error(msg, e);
                    telemetry.markError();
                    QueryServiceImpl.sendQueryBucketsResponseError(msg, responseObserver);
                    return;
                }

                final int bucketBytes = bucket.getSerializedSize();

                // byte flush: if adding this bucket would overflow the budget, flush the current chunk
                // first — but only if it already holds >= 1 bucket (zero-progress guard).
                if (!chunk.isEmpty() && chunkBytes + bucketBytes > byteBudget) {
                    if (!emitChunk(chunk)) {
                        return; // client gone or not draining: abandon (cursor closed by the try)
                    }
                    chunk.clear();
                    chunkBytes = 0;
                }

                // indivisible-oversized: a single bucket bigger than the whole budget cannot be chunked.
                if (chunk.isEmpty() && isIndivisibleOversized(bucketBytes)) {
                    final String msg = "single bucket for pv " + document.getPvName()
                            + " exceeds the outgoing message size limit (" + bucketBytes + " > "
                            + byteBudget + " bytes)";
                    logger.error(msg);
                    telemetry.markError();
                    QueryServiceImpl.sendQueryBucketsResponseError(msg, responseObserver);
                    return;
                }

                chunk.add(bucket);
                chunkBytes += bucketBytes;

                // count flush: chunk reached the per-message limit
                if (chunk.size() >= chunkSizeLimit) {
                    if (!emitChunk(chunk)) {
                        return;
                    }
                    chunk.clear();
                    chunkBytes = 0;
                }
            }

            // flush any trailing partial chunk
            if (!chunk.isEmpty() && !emitChunk(chunk)) {
                return;
            }

            responseObserver.onCompleted();

        } finally {
            // In a finally so the db stage is folded in on every exit from the cursor block,
            // including both error returns.
            recordCursorTime(cursor);
        }
    }

    /**
     * Emits one streamed BucketQueryResult message with an empty nextPageToken, once the transport
     * can take it. Returns false when the gate refused (the client cancelled or stopped reading):
     * nothing was sent and the caller must stop.
     */
    private boolean emitChunk(List<DataBucket> buckets) {
        if (!gate.awaitReady()) {
            telemetry.markAbandoned();
            return false; // abandoned: the gate logged why
        }
        final QueryBucketsResponse.BucketQueryResult result =
                QueryBucketsResponse.BucketQueryResult.newBuilder()
                        .addAllDataBuckets(buckets)
                        .setNextPageToken("") // stream signals completion; token always empty
                        .build();
        // Size the response actually sent, not the nested result; see QueryBucketsUnaryDispatcher.
        final QueryBucketsResponse response = QueryServiceImpl.queryBucketsResponse(result);
        telemetry.recordResponse(response.getSerializedSize());
        responseObserver.onNext(response);
        return true;
    }
}
