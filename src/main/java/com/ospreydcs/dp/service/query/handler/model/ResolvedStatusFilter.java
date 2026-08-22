package com.ospreydcs.dp.service.query.handler.model;

import java.util.List;
import java.util.Set;

/**
 * Internal (non-proto) resolved form of a validated {@code QuerySpec.sampleStatusSelector}: the
 * required domain, the layer wildcard list (empty = all layers in the domain), the status-code
 * match set (empty = any code, i.e. "labeled at all"), and the mode (INCLUDE keeps only samples
 * with a matching status; EXCLUDE drops them).
 *
 * <p>Carried on {@link ResolvedQuery} for the sample-oriented methods only — the resolver rejects
 * a bucket-oriented request with the selector set, since whole storage buckets cannot represent
 * per-sample filtering.
 */
public record ResolvedStatusFilter(
        String domain,
        List<String> layers,
        Set<Integer> statusCodes,
        boolean includeMode
) {

    /** True when the given status code matches the selector (empty statusCodes = any code). */
    public boolean matchesCode(int statusCode) {
        return statusCodes.isEmpty() || statusCodes.contains(statusCode);
    }
}
