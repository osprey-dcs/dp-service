package com.ospreydcs.dp.client.result;

import com.ospreydcs.dp.grpc.v1.ingestion.RegisterProviderResponse;

public class RegisterProviderApiResult extends ApiResultBase {

    // instance variables
    public final RegisterProviderResponse registerProviderResponse;

    public RegisterProviderApiResult(boolean isError, String errorMessage) {
        super(isError, errorMessage);
        this.registerProviderResponse = null;
    }

    public RegisterProviderApiResult(RegisterProviderResponse registerProviderResponse) {
        super(false, "");
        this.registerProviderResponse = registerProviderResponse;
    }

}
