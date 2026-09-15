package com.ospreydcs.dp.service.query.handler.mongo.dispatch;

import com.mongodb.client.MongoCursor;
import com.ospreydcs.dp.grpc.v1.common.DataBucket;
import com.ospreydcs.dp.grpc.v1.query.QueryBucketsResponse;
import com.ospreydcs.dp.service.common.bson.bucket.BucketDocument;
import com.ospreydcs.dp.service.common.exception.DpException;
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

import java.util.ArrayList;
import java.util.List;

/**
 * Unary {@code queryBuckets} formatter (Q2/Q7/Q8). Retrieves one bounded, keyset-paged page of
 * buckets and emits a {@code BucketQueryResult} with a {@code nextPageToken} when more pages follow.
 *
 * <p>A page ends when either the count limit ({@code pageSize}) or the outgoing message-size budget
 * would be exceeded, whichever comes first — both cases emit a normal page with a continuation token
 * (Q7). Every page emits at least one bucket (zero-progress guard); a single bucket larger than the
 * whole budget is an indivisible-oversized error. Empty result is an empty payload, not an
 * ExceptionalResult.
 */
public class QueryBucketsUnaryDispatcher extends AbstractQueryBucketsDispatcher {

    private static final Logger logger = LogManager.getLogger();

    private final StreamObserver<QueryBucketsResponse> responseObserver;

    public QueryBucketsUnaryDispatcher(
            StreamObserver<QueryBucketsResponse> responseObserver, QueryTelemetry telemetry) {
        this(responseObserver, MongoQueryHandler.getOutgoingMessageSizeLimitBytes(), telemetry);
    }

    /**
     * Package/test constructor allowing the outgoing message-size budget to be injected, so the
     * byte-budget page-split and indivisible-oversized paths can be exercised deterministically.
     */
    public QueryBucketsUnaryDispatcher(
            StreamObserver<QueryBucketsResponse> responseObserver, long byteBudget,
            QueryTelemetry telemetry) {
        super(byteBudget, telemetry);
        this.responseObserver = responseObserver;
    }

    @Override
    public void executeAndDispatch(ResolvedQuery resolvedQuery, MongoQueryClientInterface mongoClient) {

        // A query that resolves to no PVs or no retrieval intervals yields an empty result.
        if (resolvedQuery.isEmptyResult()) {
            telemetry.markEmpty();
            QueryServiceImpl.sendQueryBucketsResponseEmpty(responseObserver);
            return;
        }

        final long queryStartNanos = System.nanoTime();
        final MongoCursor<BucketDocument> cursor = mongoClient.executeQueryBucketsV2(resolvedQuery);
        telemetry.addDbNanos(System.nanoTime() - queryStartNanos);
        if (cursor == null) {
            final String msg = "executeQueryBucketsV2 returned null cursor";
            logger.error(msg + " id: " + responseObserver.hashCode());
            telemetry.markError();
            QueryServiceImpl.sendQueryBucketsResponseError(msg, responseObserver);
            return;
        }

        try (cursor) {
            if (!cursor.hasNext()) {
                telemetry.markEmpty();
                QueryServiceImpl.sendQueryBucketsResponseEmpty(responseObserver);
                return;
            }

            final int pageSize = resolvedQuery.getPageSize();

            final List<DataBucket> pageBuckets = new ArrayList<>();
            long pageBytes = 0;
            KeysetPosition lastKept = null;
            boolean hasMore = false;

            while (cursor.hasNext()) {
                final BucketDocument document = cursor.next();

                // count page-ender: the pageSize+1-th bucket signals a following page exists
                if (pageBuckets.size() >= pageSize) {
                    hasMore = true;
                    break;
                }

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

                // byte page-ender: if this bucket would overflow the budget, end the page BEFORE it —
                // but only if at least one bucket is already in the page (zero-progress guard).
                if (!pageBuckets.isEmpty() && pageBytes + bucketBytes > byteBudget) {
                    hasMore = true;
                    // this bucket was consumed from the cursor but not emitted; the continuation token
                    // resumes strictly after lastKept, so it will be re-read on the next page.
                    break;
                }

                // indivisible-oversized: a single bucket bigger than the whole budget cannot be paged.
                if (pageBuckets.isEmpty() && isIndivisibleOversized(bucketBytes)) {
                    final String msg = "single bucket for pv " + document.getPvName()
                            + " exceeds the outgoing message size limit (" + bucketBytes + " > "
                            + byteBudget + " bytes)";
                    logger.error(msg);
                    telemetry.markError();
                    QueryServiceImpl.sendQueryBucketsResponseError(msg, responseObserver);
                    return;
                }

                pageBuckets.add(bucket);
                pageBytes += bucketBytes;
                lastKept = KeysetPosition.ofBucket(
                        document.getPvName(),
                        document.getDataTimestamps().getFirstTime().getSeconds(),
                        document.getDataTimestamps().getFirstTime().getNanos());
            }

            final String nextPageToken = hasMore && lastKept != null ? PageToken.encode(lastKept) : "";

            final QueryBucketsResponse.BucketQueryResult.Builder resultBuilder =
                    QueryBucketsResponse.BucketQueryResult.newBuilder()
                            .addAllDataBuckets(pageBuckets)
                            .setNextPageToken(nextPageToken);

            final QueryBucketsResponse.BucketQueryResult result = resultBuilder.build();
            telemetry.recordResponse(result.getSerializedSize());
            QueryServiceImpl.sendQueryBucketsResponse(result, responseObserver);

        } finally {
            // In a finally so the db stage is folded in on every exit from the cursor block --
            // the empty-result return, both error returns, and the success path alike.
            recordCursorTime(cursor);
        }
    }
}
