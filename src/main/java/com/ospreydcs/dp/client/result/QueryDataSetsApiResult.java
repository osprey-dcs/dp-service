package com.ospreydcs.dp.client.result;

import com.ospreydcs.dp.grpc.v1.annotation.DataSet;

import java.util.List;

/**
 * Result of queryDataSets().  nextPageToken is non-empty when more pages are available; supply it
 * as the pageToken of the next request.  An empty nextPageToken indicates the last page.
 */
public class QueryDataSetsApiResult extends ApiResultBase {
    
    // instance variables
    public final List<DataSet> dataSets;
    public final String nextPageToken;

    public QueryDataSetsApiResult(boolean isError, String errorMessage) {
        super(isError, errorMessage);
        this.dataSets = null;
        this.nextPageToken = "";
    }

    public QueryDataSetsApiResult(boolean isError, String errorMessage, ApiResultStatus apiResultStatus) {
        super(isError, errorMessage, apiResultStatus);
        this.dataSets = null;
        this.nextPageToken = "";
    }

    public QueryDataSetsApiResult(List<DataSet> dataSets, String nextPageToken) {
        super(false, "");
        this.dataSets = dataSets;
        this.nextPageToken = nextPageToken != null ? nextPageToken : "";
    }

}
