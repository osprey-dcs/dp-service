package com.ospreydcs.dp.client.result;

import com.ospreydcs.dp.grpc.v1.query.QueryPvStatsResponse;

public class QueryPvStatsApiResult extends ApiResultBase {

    // instance variables
    public final QueryPvStatsResponse queryPvStatsResponse;

    public QueryPvStatsApiResult(boolean isError, String errorMessage) {
        super(isError, errorMessage);
        this.queryPvStatsResponse = null;
    }

    public QueryPvStatsApiResult(QueryPvStatsResponse queryPvStatsResponse) {
        super(false, "");
        this.queryPvStatsResponse = queryPvStatsResponse;
    }

}
