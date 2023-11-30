package com.ospreydcs.dp.service.common.model;

public class ValidationResult {

    public Boolean isError = null;
    public String msg = null;

    public ValidationResult(Boolean isError, String msg) {
        this.isError = isError;
        this.msg = msg;
    }
}
