package com.ospreydcs.dp.service.ingest.handler.model;

public class HandlerIngestionResult {

    public boolean isError = false;
    public String message = null;

    public HandlerIngestionResult(boolean isError, String message) {
        this.isError = isError;
        this.message = message;
    }
}
