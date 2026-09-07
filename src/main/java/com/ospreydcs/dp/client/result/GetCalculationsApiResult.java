package com.ospreydcs.dp.client.result;

import com.ospreydcs.dp.grpc.v1.annotation.Calculations;

public class GetCalculationsApiResult extends ApiResultBase {

    // instance variables
    public final Calculations calculations;

    public GetCalculationsApiResult(boolean isError, String errorMessage) {
        super(isError, errorMessage);
        this.calculations = null;
    }

    public GetCalculationsApiResult(boolean isError, String errorMessage, ApiResultStatus apiResultStatus) {
        super(isError, errorMessage, apiResultStatus);
        this.calculations = null;
    }

    public GetCalculationsApiResult(Calculations calculations) {
        super(false, "");
        this.calculations = calculations;
    }

}
