package com.ospreydcs.dp.client.result;

import com.ospreydcs.dp.grpc.v1.common.ExceptionalResult;

/**
 * Categorizes the outcome of a client API call.
 *
 * <p>This is a client-side enum rather than the protobuf {@link
 * ExceptionalResult.ExceptionalResultStatus} because a client API result has two outcomes the wire
 * enum cannot express: a successful call ({@link #NONE}), and a failure generated locally without
 * any response from the service ({@link #LOCAL_FAILURE}).  Wrapping the wire enum also keeps
 * callers of the client API insulated from protobuf enum evolution.
 */
public enum ApiResultStatus {

    /**
     * The call succeeded.  This is the status of every result whose {@code resultStatus.isError} is
     * false.
     */
    NONE,

    /**
     * The request was rejected by the service before it was handled.
     *
     * <p>Note that this covers <em>both</em> a request that failed server-side validation and a
     * well-formed request whose target record does not exist: the services report both with
     * {@code RESULT_STATUS_REJECT}, and the wire response carries nothing that distinguishes them.
     * A caller that needs to treat "record does not exist" as a normal condition — for instance to
     * decide whether a save would overwrite an existing record — must therefore be prepared for a
     * malformed request to produce the same status, and should validate its request before relying
     * on that reading.  Only the human-readable message separates the two cases.
     */
    REJECT,

    /**
     * The service encountered an error while handling the request.
     */
    ERROR,

    /**
     * The service was not ready to handle the request.  The protobuf enum documents this as
     * covering invalid bidirectional query cursor operations only.
     *
     * <p>No client API call currently wired to this enum issues a bidirectional cursor request, so
     * this status is not reachable through any of them today.  It is mapped by {@link #fromProto}
     * so that a caller of a future bidirectional API, or a client talking to a service that starts
     * returning the status elsewhere, sees the real category rather than a fallback.
     */
    NOT_READY,

    /**
     * The call failed without a status-bearing response from the service.  This covers a transport
     * failure delivered through {@code onError}, an await timeout, an interrupted wait, and a
     * malformed response sequence detected client-side.  The accompanying message describes the
     * failure.
     */
    LOCAL_FAILURE;

    /**
     * Maps a protobuf {@link ExceptionalResult.ExceptionalResultStatus} to its client-side
     * counterpart.
     *
     * <p>An unrecognized value — which a client built against an older protobuf revision can
     * receive from a newer service — maps to {@link #ERROR} rather than throwing, so that an
     * unknown failure is still reported as a failure.
     *
     * <p><strong>Zero-value hazard:</strong> {@code RESULT_STATUS_REJECT} is 0, the protobuf
     * default, so a service that builds an {@code ExceptionalResult} without calling {@code
     * setExceptionalResultStatus()} sends a value indistinguishable from a deliberate reject, and
     * this method maps it to {@link #REJECT}.  That matters because callers branch on {@link
     * ApiResultBase#isReject()} to read "target record does not exist" — so a server-side omission
     * can present as a benign not-found and, for a caller deciding whether a save would overwrite,
     * turn into a silent clobber.  Every dispatcher must therefore set the status explicitly;
     * {@code ApiResultBaseTest.testEveryExceptionalResultSetsStatus()} guards this by scanning the
     * service sources for a builder that omits it.
     */
    public static ApiResultStatus fromProto(ExceptionalResult.ExceptionalResultStatus protoStatus) {

        if (protoStatus == null) {
            return ERROR;
        }

        return switch (protoStatus) {
            case RESULT_STATUS_REJECT -> REJECT;
            case RESULT_STATUS_ERROR -> ERROR;
            case RESULT_STATUS_NOT_READY -> NOT_READY;
            default -> ERROR;
        };
    }
}
