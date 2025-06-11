package com.ospreydcs.dp.service.common.model;

public class ResultStatus {

    public Boolean isError = null;
    public String msg = null;

    public ResultStatus(Boolean isError, String msg) {
        this.isError = isError;
        this.msg = msg;
    }
}
