package com.ospreydcs.dp.service.common.model;

/**
 * Result of a MongoDB operation whose outcome is a count of affected items, e.g. individual
 * sample statuses upserted by saveSampleStatuses or removed by deleteSampleStatuses. On error the
 * count reflects the items already persisted/removed before the failure (partial persistence is
 * documented behavior for those operations).
 *
 * <p>See {@link MongoSaveResult} for the distinction between {@link #isReject} (business-rule
 * rejection, RESULT_STATUS_REJECT) and a plain {@link #isError} (infrastructure failure,
 * RESULT_STATUS_ERROR). No current site returns a rejection from this type; the flag exists so a
 * business rule added on the sample status path classifies correctly rather than reproducing the
 * collapse described in issue #235.
 */
public class MongoCountResult {

    public final boolean isError;
    public final boolean isReject;
    public final String message;
    public final long count;

    public MongoCountResult(boolean isError, String message, long count) {
        this(isError, false, message, count);
    }

    public MongoCountResult(boolean isError, boolean isReject, String message, long count) {
        this.isError = isError;
        this.isReject = isReject;
        this.message = message;
        this.count = count;
    }

    /** Creates a result for a business-rule rejection, sent to the client as RESULT_STATUS_REJECT. */
    public static MongoCountResult reject(String message, long count) {
        return new MongoCountResult(true, true, message, count);
    }

    /** Creates a result for an infrastructure failure, sent to the client as RESULT_STATUS_ERROR. */
    public static MongoCountResult error(String message, long count) {
        return new MongoCountResult(true, false, message, count);
    }
}
