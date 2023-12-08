package com.ospreydcs.dp.service.query.handler.model;

public class HandlerQueryResult {

    public boolean isError = false;
    public String message = null;

    public HandlerQueryResult(boolean isError, String message) {
        this.isError = isError;
        this.message = message;
    }
}
