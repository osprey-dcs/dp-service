package com.ospreydcs.dp.service.common.model;

/**
 * Result of a MongoDB delete operation. See {@link MongoSaveResult} for the distinction between
 * {@link #isReject} (business-rule rejection, RESULT_STATUS_REJECT) and a plain {@link #isError}
 * (infrastructure failure, RESULT_STATUS_ERROR).
 *
 * <p>Note that "no matching record" is not a failure here: the delete methods signal it with
 * {@code isError=false} and a null {@link #deletedPvName}, and the dispatcher turns that into a
 * rejection.
 */
public class MongoDeleteResult {

    public final boolean isError;
    public final boolean isReject;
    public final String message;
    public final String deletedPvName;

    public MongoDeleteResult(boolean isError, String message, String deletedPvName) {
        this(isError, false, message, deletedPvName);
    }

    public MongoDeleteResult(boolean isError, boolean isReject, String message, String deletedPvName) {
        this.isError = isError;
        this.isReject = isReject;
        this.message = message;
        this.deletedPvName = deletedPvName;
    }

    /** Creates a result for a business-rule rejection, sent to the client as RESULT_STATUS_REJECT. */
    public static MongoDeleteResult reject(String message) {
        return new MongoDeleteResult(true, true, message, null);
    }

    /** Creates a result for an infrastructure failure, sent to the client as RESULT_STATUS_ERROR. */
    public static MongoDeleteResult error(String message) {
        return new MongoDeleteResult(true, false, message, null);
    }
}
