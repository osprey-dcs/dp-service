package com.ospreydcs.dp.client.result;

import com.ospreydcs.dp.grpc.v1.annotation.DataSet;

import java.util.List;

public class QueryDataSetsApiResult extends ApiResultBase {
    
    // instance variables
    public final List<DataSet> dataSets;

    public QueryDataSetsApiResult(boolean isError, String errorMessage) {
        super(isError, errorMessage);
        this.dataSets = null;
    }

    public QueryDataSetsApiResult(List<DataSet> dataSets) {
        super(false, "");
        this.dataSets = dataSets;
    }

}
