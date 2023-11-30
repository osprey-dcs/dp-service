package com.ospreydcs.dp.service.ingest.handler.model;

public class ValidateIngestionRequestResult {

    public Boolean isError = null;
    public String msg = null;

    public ValidateIngestionRequestResult(Boolean isError, String msg) {
        this.isError = isError;
        this.msg = msg;
    }

}
