package com.ospreydcs.dp.service.common.mongo;

import com.mongodb.client.result.UpdateResult;

public class UpdateResultWrapper {

    // instance variables
    public UpdateResult updateResult = null;
    public String exception = null;

    public UpdateResultWrapper(UpdateResult updateResult) {
        this.updateResult = updateResult;
    }

    public UpdateResultWrapper(String exception) {
        this.exception = exception;
    }

    public boolean isException() {
        return exception != null;
    }
}
