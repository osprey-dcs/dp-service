package com.ospreydcs.dp.service.query.handler.model;

import java.util.Collections;
import java.util.List;

/**
 * Internal (non-proto) resolved-query object for Query API V2 — the "ExecutionPlan" seam between
 * request resolution (step 2) and retrieval/formatting (steps 3/5). Built by the resolver from a
 * validated {@code QuerySpec + ExecutionOptions + ResultRepresentation}; immutable; carried into the
 * V2 job and dispatcher.
 *
 * <p>Holds the concrete resolved PV <b>name list</b> (Q9 requires the list, not just a filter, so
 * every resolved PV can get a column even with no data in range), the effective — possibly
 * fragmented — retrieval intervals (Q3), the normalized page size (Q7), the decoded continuation
 * position (Q1/Q2, {@code null} on the first page), and the representation flags (Q5/Q8).
 */
public final class ResolvedQuery {

    public enum ResultMode { BUCKET, SAMPLE }

    private final List<String> pvNames;
    private final List<TimeInterval> retrievalIntervals;
    private final int pageSize;
    private final KeysetPosition pageStart; // null on first page
    private final boolean useSerializedColumns;
    private final boolean excludeColumnMetadata;
    private final ResultMode mode;
    private final boolean streaming;

    public ResolvedQuery(
            List<String> pvNames,
            List<TimeInterval> retrievalIntervals,
            int pageSize,
            KeysetPosition pageStart,
            boolean useSerializedColumns,
            boolean excludeColumnMetadata,
            ResultMode mode,
            boolean streaming) {
        this.pvNames = Collections.unmodifiableList(pvNames);
        this.retrievalIntervals = Collections.unmodifiableList(retrievalIntervals);
        this.pageSize = pageSize;
        this.pageStart = pageStart;
        this.useSerializedColumns = useSerializedColumns;
        this.excludeColumnMetadata = excludeColumnMetadata;
        this.mode = mode;
        this.streaming = streaming;
    }

    /** Sorted-ascending list of resolved PV names; drives Q9 column order/presence. */
    public List<String> getPvNames() {
        return pvNames;
    }

    /** Sorted, disjoint effective retrieval intervals (single element = whole timeRange). */
    public List<TimeInterval> getRetrievalIntervals() {
        return retrievalIntervals;
    }

    public int getPageSize() {
        return pageSize;
    }

    /** Decoded continuation position, or {@code null} on the first page. */
    public KeysetPosition getPageStart() {
        return pageStart;
    }

    public boolean isUseSerializedColumns() {
        return useSerializedColumns;
    }

    public boolean isExcludeColumnMetadata() {
        return excludeColumnMetadata;
    }

    public ResultMode getMode() {
        return mode;
    }

    public boolean isStreaming() {
        return streaming;
    }

    /**
     * True when this query resolves to no PVs or no retrieval intervals — the retrieval layer should
     * short-circuit to an empty (not exceptional) result.
     */
    public boolean isEmptyResult() {
        return pvNames.isEmpty() || retrievalIntervals.isEmpty();
    }
}
