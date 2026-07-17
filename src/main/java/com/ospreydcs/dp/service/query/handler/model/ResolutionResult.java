package com.ospreydcs.dp.service.query.handler.model;

import com.ospreydcs.dp.service.common.model.ResultStatus;

/**
 * Outcome of Query API V2 request resolution: either a successful {@link ResolvedQuery}, or a
 * {@link ResultStatus} describing a validation/resolution rejection or error. Exactly one of the two
 * is non-null. A {@code ResultStatus} with {@code isError == false} is not used here — a resolution
 * failure is always a reject or error, and an empty-but-valid result is represented by a
 * {@link ResolvedQuery} whose {@link ResolvedQuery#isEmptyResult()} is true.
 */
public final class ResolutionResult {

    private final ResolvedQuery resolvedQuery;
    private final ResultStatus errorStatus;

    private ResolutionResult(ResolvedQuery resolvedQuery, ResultStatus errorStatus) {
        this.resolvedQuery = resolvedQuery;
        this.errorStatus = errorStatus;
    }

    public static ResolutionResult of(ResolvedQuery resolvedQuery) {
        return new ResolutionResult(resolvedQuery, null);
    }

    /** A rejection (client error, e.g. bad request / cap exceeded) — maps to RESULT_STATUS_REJECT. */
    public static ResolutionResult reject(String message) {
        return new ResolutionResult(null, new ResultStatus(true, message));
    }

    public boolean isError() {
        return errorStatus != null;
    }

    public ResolvedQuery getResolvedQuery() {
        return resolvedQuery;
    }

    public ResultStatus getErrorStatus() {
        return errorStatus;
    }
}
