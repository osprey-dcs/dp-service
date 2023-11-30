package com.ospreydcs.dp.service.query.handler.model;

public class ValidateQueryRequestResult {

    public Boolean isError = null;
    public String msg = null;

    public ValidateQueryRequestResult(Boolean isError, String msg) {
        this.isError = isError;
        this.msg = msg;
    }

}
