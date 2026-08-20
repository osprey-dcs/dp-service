package com.ospreydcs.dp.client.result;

import com.ospreydcs.dp.service.common.model.ResultStatus;

public abstract class ApiResultBase {

    public final ResultStatus resultStatus;

    /**
     * Categorizes the failure carried by {@link #resultStatus}, so callers can distinguish a
     * rejection from a service error without matching on the message string.  This is {@link
     * ApiResultStatus#NONE} for a successful result.
     *
     * <p>Held here rather than on {@link ResultStatus} because {@code ResultStatus} lives in the
     * server package and is shared with the dispatchers, jobs and validation utilities, none of
     * which have any use for a client-facing status.
     */
    public final ApiResultStatus apiResultStatus;

    /**
     * Constructs a result whose failure, if any, was generated locally rather than reported by the
     * service.  Retained so that existing callers are unaffected.
     */
    public ApiResultBase(boolean isError, String errorMessage) {
        this(isError, errorMessage, isError ? ApiResultStatus.LOCAL_FAILURE : ApiResultStatus.NONE);
    }

    public ApiResultBase(boolean isError, String errorMessage, ApiResultStatus apiResultStatus) {
        this.resultStatus = new ResultStatus(isError, errorMessage);
        // a successful result is never categorized as a failure, and vice versa, regardless of what
        // the caller passed
        if (!isError) {
            this.apiResultStatus = ApiResultStatus.NONE;
        } else if (apiResultStatus == null || apiResultStatus == ApiResultStatus.NONE) {
            this.apiResultStatus = ApiResultStatus.LOCAL_FAILURE;
        } else {
            this.apiResultStatus = apiResultStatus;
        }
    }

    /**
     * Returns true if the call failed.  Equivalent to reading {@code resultStatus.isError}.
     */
    public boolean isError() {
        return resultStatus.isError;
    }

    /**
     * Returns true if the service rejected the request.
     *
     * <p>A reject means the service declined the request without handling it.  This covers both a
     * request that failed server-side validation and a well-formed request whose target record does
     * not exist — see {@link ApiResultStatus#REJECT} for why the two cannot be told apart from the
     * status alone.
     */
    public boolean isReject() {
        return apiResultStatus == ApiResultStatus.REJECT;
    }
}
