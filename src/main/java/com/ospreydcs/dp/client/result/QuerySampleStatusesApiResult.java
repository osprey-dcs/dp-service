package com.ospreydcs.dp.client.result;

import com.ospreydcs.dp.grpc.v1.common.SampleStatusBucket;

import java.util.List;

/**
 * Result of querySampleStatuses() / querySampleStatusesStream(). For the unary method,
 * nextPageToken is non-empty when more pages are available; for the streaming method the buckets
 * are the accumulated result of the whole stream and nextPageToken is always empty.
 */
public class QuerySampleStatusesApiResult extends ApiResultBase {

    // instance variables
    public final List<SampleStatusBucket> sampleStatusBuckets;
    public final String nextPageToken;

    public QuerySampleStatusesApiResult(boolean isError, String errorMessage) {
        super(isError, errorMessage);
        this.sampleStatusBuckets = null;
        this.nextPageToken = "";
    }

    public QuerySampleStatusesApiResult(boolean isError, String errorMessage, ApiResultStatus apiResultStatus) {
        super(isError, errorMessage, apiResultStatus);
        this.sampleStatusBuckets = null;
        this.nextPageToken = "";
    }

    public QuerySampleStatusesApiResult(List<SampleStatusBucket> sampleStatusBuckets, String nextPageToken) {
        super(false, "");
        this.sampleStatusBuckets = sampleStatusBuckets;
        this.nextPageToken = nextPageToken != null ? nextPageToken : "";
    }

}
