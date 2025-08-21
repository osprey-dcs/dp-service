package com.ospreydcs.dp.client.result;

import com.ospreydcs.dp.service.common.model.ResultStatus;

public abstract class ApiResultBase {

    public final ResultStatus resultStatus;

    public ApiResultBase(boolean isError, String errorMessage) {
        this.resultStatus = new ResultStatus(isError, errorMessage);
    }
}
