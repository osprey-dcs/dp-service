package com.ospreydcs.dp.client.result;

import com.ospreydcs.dp.grpc.v1.common.Configuration;

public class GetConfigurationApiResult extends ApiResultBase {

    // instance variables
    public final Configuration configuration;

    public GetConfigurationApiResult(boolean isError, String errorMessage) {
        super(isError, errorMessage);
        this.configuration = null;
    }

    public GetConfigurationApiResult(boolean isError, String errorMessage, ApiResultStatus apiResultStatus) {
        super(isError, errorMessage, apiResultStatus);
        this.configuration = null;
    }

    public GetConfigurationApiResult(Configuration configuration) {
        super(false, "");
        this.configuration = configuration;
    }

}
