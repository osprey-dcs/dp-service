package com.ospreydcs.dp.client.result;

import com.ospreydcs.dp.grpc.v1.query.QueryPvMetadataResponse;

public class QueryPvMetadataApiResult extends ApiResultBase {

    // instance variables
    public final QueryPvMetadataResponse queryPvMetadataResponse;

    public QueryPvMetadataApiResult(boolean isError, String errorMessage) {
        super(isError, errorMessage);
        this.queryPvMetadataResponse = null;
    }

    public QueryPvMetadataApiResult(QueryPvMetadataResponse queryPvMetadataResponse) {
        super(false, "");
        this.queryPvMetadataResponse = queryPvMetadataResponse;
    }

}
