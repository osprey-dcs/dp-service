package com.ospreydcs.dp.client.result;

import com.ospreydcs.dp.grpc.v1.common.ConfigurationActivation;

/**
 * Result of getConfigurationActivationById() / getConfigurationActivationByCompositeKey().  A key
 * with no matching record is reported as a rejection (isReject() true), not as a successful result
 * with a null configurationActivation.
 */
public class GetConfigurationActivationApiResult extends ApiResultBase {

    // instance variables
    public final ConfigurationActivation configurationActivation;

    public GetConfigurationActivationApiResult(boolean isError, String errorMessage) {
        super(isError, errorMessage);
        this.configurationActivation = null;
    }

    public GetConfigurationActivationApiResult(
            boolean isError, String errorMessage, ApiResultStatus apiResultStatus
    ) {
        super(isError, errorMessage, apiResultStatus);
        this.configurationActivation = null;
    }

    public GetConfigurationActivationApiResult(ConfigurationActivation configurationActivation) {
        super(false, "");
        this.configurationActivation = configurationActivation;
    }

}
