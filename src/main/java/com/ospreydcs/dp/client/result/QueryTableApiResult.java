package com.ospreydcs.dp.client.result;

import com.ospreydcs.dp.grpc.v1.query.QueryTableResponse;

public class QueryTableApiResult extends ApiResultBase {

    // instance variables
    public final QueryTableResponse queryTableResponse;

    public QueryTableApiResult(boolean isError, String errorMessage) {
        super(isError, errorMessage);
        this.queryTableResponse = null;
    }

    public QueryTableApiResult(QueryTableResponse queryTableResponse) {
        super(false, "");
        this.queryTableResponse = queryTableResponse;
    }

}
