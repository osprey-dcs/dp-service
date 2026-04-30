package com.ospreydcs.dp.service.common.model;

public class MongoDeleteResult {

    public final boolean isError;
    public final String message;
    public final String deletedPvName;

    public MongoDeleteResult(boolean isError, String message, String deletedPvName) {
        this.isError = isError;
        this.message = message;
        this.deletedPvName = deletedPvName;
    }
}
