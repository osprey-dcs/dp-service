package com.ospreydcs.dp.service.ingest.model;

import com.mongodb.client.result.InsertManyResult;

public class IngestionTaskResult {

    public final boolean isError;
    public final String msg;
    public final InsertManyResult insertManyResult;

    public IngestionTaskResult(boolean isError, String msg, InsertManyResult insertManyResult) {
        this.isError = isError;
        this.msg = msg;
        this.insertManyResult = insertManyResult;
    }

}
