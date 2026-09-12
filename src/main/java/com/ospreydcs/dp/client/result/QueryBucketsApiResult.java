package com.ospreydcs.dp.client.result;

import com.ospreydcs.dp.grpc.v1.common.DataBucket;

import java.util.List;

/**
 * Result of {@code queryBuckets()} / {@code queryBucketsStream()} (Query API V2).
 *
 * <p>For the unary method, {@code nextPageToken} is non-empty when more pages are available; for
 * the streaming method the buckets are the accumulated result of the whole stream and {@code
 * nextPageToken} is always empty.
 *
 * <p>Buckets are returned whole, so a bucket may carry samples outside the requested TimeRange.  An
 * empty query result is a success carrying an empty bucket list.
 */
public class QueryBucketsApiResult extends ApiResultBase {

    // instance variables
    public final List<DataBucket> dataBuckets;
    public final String nextPageToken;

    public QueryBucketsApiResult(boolean isError, String errorMessage) {
        super(isError, errorMessage);
        this.dataBuckets = null;
        this.nextPageToken = "";
    }

    public QueryBucketsApiResult(boolean isError, String errorMessage, ApiResultStatus apiResultStatus) {
        super(isError, errorMessage, apiResultStatus);
        this.dataBuckets = null;
        this.nextPageToken = "";
    }

    public QueryBucketsApiResult(List<DataBucket> dataBuckets, String nextPageToken) {
        super(false, "");
        this.dataBuckets = dataBuckets;
        this.nextPageToken = nextPageToken != null ? nextPageToken : "";
    }

}
