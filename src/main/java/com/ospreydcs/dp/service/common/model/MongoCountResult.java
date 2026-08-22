package com.ospreydcs.dp.service.common.model;

/**
 * Result of a MongoDB operation whose outcome is a count of affected items, e.g. individual
 * sample statuses upserted by saveSampleStatuses or removed by deleteSampleStatuses. On error the
 * count reflects the items already persisted/removed before the failure (partial persistence is
 * documented behavior for those operations).
 */
public class MongoCountResult {

    public final boolean isError;
    public final String message;
    public final long count;

    public MongoCountResult(boolean isError, String message, long count) {
        this.isError = isError;
        this.message = message;
        this.count = count;
    }
}
