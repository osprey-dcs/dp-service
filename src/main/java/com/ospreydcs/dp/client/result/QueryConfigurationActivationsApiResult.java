package com.ospreydcs.dp.client.result;

import com.ospreydcs.dp.grpc.v1.common.ConfigurationActivation;

import java.util.List;

/**
 * Result of queryConfigurationActivations().  nextPageToken is non-empty when more pages are
 * available; supply it as the pageToken of the next request.  An empty nextPageToken indicates the
 * last page.
 */
public class QueryConfigurationActivationsApiResult extends ApiResultBase {

    // instance variables
    public final List<ConfigurationActivation> configurationActivations;
    public final String nextPageToken;

    public QueryConfigurationActivationsApiResult(boolean isError, String errorMessage) {
        super(isError, errorMessage);
        this.configurationActivations = null;
        this.nextPageToken = "";
    }

    public QueryConfigurationActivationsApiResult(
            boolean isError, String errorMessage, ApiResultStatus apiResultStatus
    ) {
        super(isError, errorMessage, apiResultStatus);
        this.configurationActivations = null;
        this.nextPageToken = "";
    }

    public QueryConfigurationActivationsApiResult(
            List<ConfigurationActivation> configurationActivations, String nextPageToken
    ) {
        super(false, "");
        this.configurationActivations = configurationActivations;
        this.nextPageToken = nextPageToken != null ? nextPageToken : "";
    }

}
