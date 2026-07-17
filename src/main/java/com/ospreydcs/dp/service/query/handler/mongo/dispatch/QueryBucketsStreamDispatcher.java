package com.ospreydcs.dp.service.query.handler.mongo.dispatch;

import com.mongodb.client.MongoCursor;
import com.ospreydcs.dp.grpc.v1.common.DataBucket;
import com.ospreydcs.dp.grpc.v1.query.QueryBucketsResponse;
import com.ospreydcs.dp.service.common.bson.bucket.BucketDocument;
import com.ospreydcs.dp.service.common.exception.DpException;
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

    public QueryBucketsStreamDispatcher(StreamObserver<QueryBucketsResponse> responseObserver) {
        this(responseObserver, MongoQueryHandler.getOutgoingMessageSizeLimitBytes());
    }

    /** Package/test constructor allowing the outgoing message-size budget to be injected. */
    public QueryBucketsStreamDispatcher(StreamObserver<QueryBucketsResponse> responseObserver, long byteBudget) {
        super(byteBudget);
        this.responseObserver = responseObserver;
    }

    @Override
    public void executeAndDispatch(ResolvedQuery resolvedQuery, MongoQueryClientInterface mongoClient) {

        // A query that resolves to no PVs or no retrieval intervals yields a single empty message.
        if (resolvedQuery.isEmptyResult()) {
            emitChunk(new ArrayList<>());
            responseObserver.onCompleted();
            return;
        }

        final MongoCursor<BucketDocument> cursor = mongoClient.executeQueryBucketsV2Stream(resolvedQuery);
        if (cursor == null) {
            final String msg = "executeQueryBucketsV2Stream returned null cursor";
            logger.error(msg + " id: " + responseObserver.hashCode());
            QueryServiceImpl.sendQueryBucketsResponseError(msg, responseObserver);
            return;
        }

        try (cursor) {
            if (!cursor.hasNext()) {
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
                    QueryServiceImpl.sendQueryBucketsResponseError(msg, responseObserver);
                    return;
                }

                final int bucketBytes = bucket.getSerializedSize();

                // byte flush: if adding this bucket would overflow the budget, flush the current chunk
                // first — but only if it already holds >= 1 bucket (zero-progress guard).
                if (!chunk.isEmpty() && chunkBytes + bucketBytes > byteBudget) {
                    emitChunk(chunk);
                    chunk.clear();
                    chunkBytes = 0;
                }

                // indivisible-oversized: a single bucket bigger than the whole budget cannot be chunked.
                if (chunk.isEmpty() && isIndivisibleOversized(bucketBytes)) {
                    final String msg = "single bucket for pv " + document.getPvName()
                            + " exceeds the outgoing message size limit (" + bucketBytes + " > "
                            + byteBudget + " bytes)";
                    logger.error(msg);
                    QueryServiceImpl.sendQueryBucketsResponseError(msg, responseObserver);
                    return;
                }

                chunk.add(bucket);
                chunkBytes += bucketBytes;

                // count flush: chunk reached the per-message limit
                if (chunk.size() >= chunkSizeLimit) {
                    emitChunk(chunk);
                    chunk.clear();
                    chunkBytes = 0;
                }
            }

            // flush any trailing partial chunk
            if (!chunk.isEmpty()) {
                emitChunk(chunk);
            }

            responseObserver.onCompleted();
        }
    }

    /** Emits one streamed BucketQueryResult message with an empty nextPageToken. */
    private void emitChunk(List<DataBucket> buckets) {
        final QueryBucketsResponse.BucketQueryResult result =
                QueryBucketsResponse.BucketQueryResult.newBuilder()
                        .addAllDataBuckets(buckets)
                        .setNextPageToken("") // stream signals completion; token always empty
                        .build();
        responseObserver.onNext(QueryServiceImpl.queryBucketsResponse(result));
    }
}
