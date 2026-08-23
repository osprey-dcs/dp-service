package com.ospreydcs.dp.service.common.model;

/**
 * Result of a MongoDB save operation.
 *
 * <p>Failures come in two kinds, and callers act on them differently. A <i>rejection</i>
 * ({@link #isReject}) means the request violated a business rule — a referenced entity does not
 * exist, or a constraint would be broken — so the service declined it and retrying the same
 * request is pointless. An <i>error</i> means the service failed to handle an otherwise valid
 * request (a MongoException, an unacknowledged write), where a retry may well succeed. The
 * dispatchers map these to RESULT_STATUS_REJECT and RESULT_STATUS_ERROR respectively.
 *
 * <p>{@link #isReject} implies {@link #isError}, so call sites that only test {@code isError}
 * continue to see every failure. Prefer the {@link #reject} and {@link #error} factories over the
 * constructor when building a failure result.
 */
public class MongoSaveResult {
    
    public final Boolean isError;
    public final boolean isReject;
    public final String message;
    public final String documentId;
    public final boolean isNewDocument;

    public MongoSaveResult(Boolean isError, String message, String documentId, boolean isNewDocument) {
        this(isError, false, message, documentId, isNewDocument);
    }

    public MongoSaveResult(
            Boolean isError, boolean isReject, String message, String documentId, boolean isNewDocument
    ) {
        this.isError = isError;
        this.isReject = isReject;
        this.message = message;
        this.documentId = documentId;
        this.isNewDocument = isNewDocument;
    }

    /** Creates a result for a business-rule rejection, sent to the client as RESULT_STATUS_REJECT. */
    public static MongoSaveResult reject(String message, String documentId, boolean isNewDocument) {
        return new MongoSaveResult(true, true, message, documentId, isNewDocument);
    }

    /** Creates a result for an infrastructure failure, sent to the client as RESULT_STATUS_ERROR. */
    public static MongoSaveResult error(String message, String documentId, boolean isNewDocument) {
        return new MongoSaveResult(true, false, message, documentId, isNewDocument);
    }
}
