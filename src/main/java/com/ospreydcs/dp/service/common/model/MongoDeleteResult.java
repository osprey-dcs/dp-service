package com.ospreydcs.dp.service.common.model;

/**
 * Result of a MongoDB delete operation. See {@link MongoSaveResult} for the distinction between
 * {@link #isReject} (business-rule rejection, RESULT_STATUS_REJECT) and a plain {@link #isError}
 * (infrastructure failure, RESULT_STATUS_ERROR).
 *
 * <p>Two distinct not-found-style outcomes reach the dispatcher through this type, and they are
 * not interchangeable:
 * <ul>
 *   <li><b>No matching record</b> — not a failure at this layer. The delete methods signal it with
 *       {@code isError=false} and a null {@link #deletedIdentifier}, and the dispatcher turns that
 *       into a rejection. Use this when the delete simply matched nothing.</li>
 *   <li><b>Blocked by a business rule</b> — e.g. {@code deleteConfiguration} refusing while
 *       activations exist. Use {@link #reject}, which sets both flags. The record may well exist;
 *       the service is declining to remove it.</li>
 * </ul>
 * {@code deleteConfiguration} uses both, so pick by which condition actually held rather than by
 * what the surrounding method does elsewhere.
 */
public class MongoDeleteResult {

    public final boolean isError;
    public final boolean isReject;
    public final String message;
    public final String deletedIdentifier;

    public MongoDeleteResult(boolean isError, String message, String deletedIdentifier) {
        this(isError, false, message, deletedIdentifier);
    }

    /**
     * Private so that {@code isReject} without {@code isError} cannot be constructed. Build failure
     * results with {@link #reject} or {@link #error}.
     */
    private MongoDeleteResult(boolean isError, boolean isReject, String message, String deletedIdentifier) {
        this.isError = isError;
        this.isReject = isReject;
        this.message = message;
        this.deletedIdentifier = deletedIdentifier;
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
