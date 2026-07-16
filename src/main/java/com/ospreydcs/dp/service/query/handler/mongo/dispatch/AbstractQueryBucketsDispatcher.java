package com.ospreydcs.dp.service.query.handler.mongo.dispatch;

import com.ospreydcs.dp.grpc.v1.common.DataBucket;
import com.ospreydcs.dp.service.common.bson.bucket.BucketDocument;
import com.ospreydcs.dp.service.common.exception.DpException;
import com.ospreydcs.dp.service.query.handler.model.ResolvedQuery;

/**
 * Shared base for the Query API V2 bucket dispatchers (unary {@link QueryBucketsUnaryDispatcher} and
 * streaming {@code QueryBucketsStreamDispatcher}). Holds the outgoing message-size budget and the
 * per-bucket build + representation-flag handling, so the two dispatchers differ only in their
 * accumulate-and-flush (stream) vs. accumulate-one-page (unary) control flow.
 */
public abstract class AbstractQueryBucketsDispatcher extends QueryV2Dispatcher {

    protected final long byteBudget;

    protected AbstractQueryBucketsDispatcher(long byteBudget) {
        this.byteBudget = byteBudget;
    }

    /**
     * Builds a V2 {@link DataBucket} from a stored bucket document, honoring the resolved query's
     * representation flags (useSerializedColumns pass-through, excludeColumnMetadata suppression).
     */
    protected DataBucket buildBucket(BucketDocument document, ResolvedQuery resolvedQuery) throws DpException {
        return BucketDocument.dataBucketFromDocumentV2(
                document,
                resolvedQuery.isUseSerializedColumns(),
                resolvedQuery.isExcludeColumnMetadata());
    }

    /**
     * True when a single bucket is larger than the entire message-size budget and therefore cannot be
     * paged/chunked out of (an indivisible-oversized error condition, Q7).
     */
    protected boolean isIndivisibleOversized(int bucketBytes) {
        return bucketBytes > byteBudget;
    }
}
