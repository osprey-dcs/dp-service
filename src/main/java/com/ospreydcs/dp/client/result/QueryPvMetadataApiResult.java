package com.ospreydcs.dp.client.result;

import com.ospreydcs.dp.grpc.v1.common.PvMetadata;

import java.util.List;

/**
 * Result of queryPvMetadata().  nextPageToken is non-empty when more pages are available; supply it
 * as the pageToken of the next request.  An empty nextPageToken indicates the last page.
 */
public class QueryPvMetadataApiResult extends ApiResultBase {

    // instance variables
    public final List<PvMetadata> pvMetadata;
    public final String nextPageToken;

    public QueryPvMetadataApiResult(boolean isError, String errorMessage) {
        super(isError, errorMessage);
        this.pvMetadata = null;
        this.nextPageToken = "";
    }

    public QueryPvMetadataApiResult(boolean isError, String errorMessage, ApiResultStatus apiResultStatus) {
        super(isError, errorMessage, apiResultStatus);
        this.pvMetadata = null;
        this.nextPageToken = "";
    }

    public QueryPvMetadataApiResult(List<PvMetadata> pvMetadata, String nextPageToken) {
        super(false, "");
        this.pvMetadata = pvMetadata;
        this.nextPageToken = nextPageToken != null ? nextPageToken : "";
    }

}
