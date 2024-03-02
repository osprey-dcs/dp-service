package com.ospreydcs.dp.service.common.model;

import com.mongodb.client.result.InsertOneResult;

public class MongoInsertOneResult {

    public Boolean isError = null;
    public String message = null;
    public final InsertOneResult insertOneResult;

    public MongoInsertOneResult(boolean isError, String message, InsertOneResult result) {
        this.isError = isError;
        this.message = message;
        this.insertOneResult = result;
    }
}
