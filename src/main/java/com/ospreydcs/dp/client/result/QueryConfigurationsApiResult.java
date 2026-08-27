package com.ospreydcs.dp.client.result;

import com.ospreydcs.dp.grpc.v1.common.Configuration;

import java.util.List;

/**
 * Result of queryConfigurations().  nextPageToken is non-empty when more pages are available;
 * supply it as the pageToken of the next request.  An empty nextPageToken indicates the last page.
 */
public class QueryConfigurationsApiResult extends ApiResultBase {

    // instance variables
    public final List<Configuration> configurations;
    public final String nextPageToken;

    public QueryConfigurationsApiResult(boolean isError, String errorMessage) {
        super(isError, errorMessage);
        this.configurations = null;
        this.nextPageToken = "";
    }

    public QueryConfigurationsApiResult(boolean isError, String errorMessage, ApiResultStatus apiResultStatus) {
        super(isError, errorMessage, apiResultStatus);
        this.configurations = null;
        this.nextPageToken = "";
    }

    public QueryConfigurationsApiResult(List<Configuration> configurations, String nextPageToken) {
        super(false, "");
        this.configurations = configurations;
        this.nextPageToken = nextPageToken != null ? nextPageToken : "";
    }

}
