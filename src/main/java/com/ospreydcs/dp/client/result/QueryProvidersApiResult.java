package com.ospreydcs.dp.client.result;

import com.ospreydcs.dp.grpc.v1.query.QueryProvidersResponse;

import java.util.List;

public class QueryProvidersApiResult extends ApiResultBase {
    
    // instance variables
    public final List<QueryProvidersResponse.ProvidersResult.ProviderInfo> providerInfos;

    public QueryProvidersApiResult(boolean isError, String errorMessage) {
        super(isError, errorMessage);
        this.providerInfos = null;
    }

    public QueryProvidersApiResult(List<QueryProvidersResponse.ProvidersResult.ProviderInfo> providerInfos) {
        super(false, "");
        this.providerInfos = providerInfos;
    }

}
