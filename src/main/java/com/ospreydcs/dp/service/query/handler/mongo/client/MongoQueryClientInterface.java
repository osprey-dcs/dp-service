package com.ospreydcs.dp.service.query.handler.mongo.client;

import com.mongodb.client.MongoCursor;
import com.ospreydcs.dp.grpc.v1.query.*;
import com.ospreydcs.dp.service.common.bson.PvMetadataQueryResultDocument;
import com.ospreydcs.dp.service.common.bson.ProviderDocument;
import com.ospreydcs.dp.service.common.bson.ProviderMetadataQueryResultDocument;
import com.ospreydcs.dp.service.common.bson.bucket.BucketDocument;
import com.ospreydcs.dp.service.common.bson.dataset.DataBlockDocument;
import com.ospreydcs.dp.service.common.exception.DpException;
import com.ospreydcs.dp.service.query.handler.model.ResolvedQuery;
import com.ospreydcs.dp.service.query.handler.model.TimeInterval;
import org.bson.conversions.Bson;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;

public interface MongoQueryClientInterface {

    boolean init();
    boolean fini();

    MongoCursor<BucketDocument> executeDataBlockQuery(DataBlockDocument dataBlock);

    MongoCursor<BucketDocument> executeQueryData(QueryDataRequest.QuerySpec querySpec);

    MongoCursor<BucketDocument> executeQueryTable(QueryTableRequest request);

    MongoCursor<PvMetadataQueryResultDocument> executeQueryPvStats(QueryPvStatsRequest request);

    MongoCursor<PvMetadataQueryResultDocument> executeQueryPvStats(Collection<String> pvNameList);

    MongoCursor<PvMetadataQueryResultDocument> executeQueryPvStats(String pvNamePatternString);

    /**
     * Returns the subset of the specified PV names that exist in the archive. This is a cheap
     * existence check backed by a {@code distinct} on the pvName index, avoiding the full stat
     * aggregation (sort + group over all buckets for each PV) performed by executeQueryPvStats().
     * Returns an empty collection if pvNameList is null or empty, and null if a database error
     * occurs.
     * <p>
     * The result is bounded by the size of the input {@code pvNameList} (a matched-names subset),
     * so it is safe for the small name sets passed by subscription/dataset validation. It is NOT
     * suitable for unbounded name sets: {@code distinct} materializes its result into a single
     * 16MB BSON document, which a very large result could exceed.
     */
    Collection<String> executeQueryPvExistence(Collection<String> pvNameList);

    /**
     * Resolves a PV-name regex pattern to the concrete set of PV names present in the buckets
     * collection ({@code distinct} on the indexed pvName restricted by the pattern). Query API V2
     * uses this so pattern selectors materialize a concrete name list (Q9). Returns the matched
     * names, or null on database error. Throws {@link java.util.regex.PatternSyntaxException} if the
     * pattern does not compile (caller catches → reject, Q10).
     */
    List<String> resolvePvNamesByPattern(String pvNamePattern);

    /**
     * Resolves a set of PV-metadata criterion filters (built from the shared filter helpers) to the
     * concrete set of PV names, then intersects with archive existence — dropping names that have no
     * buckets at all (Q11 decision b). Returns the intersected names, or null on database error.
     * An empty {@code criteriaFilters} matches all metadata records.
     */
    List<String> resolvePvNamesByMetadata(List<Bson> criteriaFilters);

    /**
     * Resolves a set of configuration-activation criterion filters (built from the shared filter
     * helpers, non-temporal arms only) to the matching activations' {@code [startTime, endTime)}
     * intervals. Open-ended activations (absent endTime) use {@code Long.MAX_VALUE} seconds as the
     * end sentinel; the caller intersects with the query timeRange (Q3). Returns the intervals
     * (un-unioned), or null on database error.
     */
    List<TimeInterval> resolveConfigurationIntervals(List<Bson> criteriaFilters);

    /**
     * Retrieves one page of buckets for a Query API V2 bucket query. Builds a bounded, resumable
     * cursor: AND of the resolved PV-name filter, an {@code $or} of the per-fragment bucket-overlap
     * predicates (Q3), and — when the resolved query carries a bucket keyset position — a seek
     * strictly after that {@code (pvName, firstTimeSecs, firstTimeNanos)} tuple (Q2). Sorted by the
     * compound {@code (pvName, firstTimeSecs, firstTimeNanos)} key and limited to
     * {@code pageSize + 1} (the extra probe row lets the caller detect a following page). Returns
     * null on a null/empty resolution or on a retrieval failure (a database error, or a failed
     * pvStats span read — #232 plan D8); the dispatcher screens empty resolutions before calling, so
     * a null it receives is reported as an error.
     */
    MongoCursor<BucketDocument> executeQueryBucketsV2(ResolvedQuery resolvedQuery);

    /**
     * Retrieves the full, unbounded bucket cursor for a Query API V2 streaming bucket query. Same
     * PV-name filter and {@code $or} fragment overlap as {@link #executeQueryBucketsV2}, but with no
     * keyset seek and no limit — the entire result is streamed to exhaustion and chunked into
     * messages downstream (fire-and-consume). Returns null on a null/empty resolution or on a
     * retrieval failure (a database error, or a failed pvStats span read — #232 plan D8); the
     * dispatcher screens empty resolutions before calling, so a null it receives is reported as an
     * error.
     */
    MongoCursor<BucketDocument> executeQueryBucketsV2Stream(ResolvedQuery resolvedQuery);

    /**
     * Retrieves buckets for one time slice of a Query API V2 sample (column-table) query: the
     * half-open window {@code [windowBegin, windowEnd)} intersected with the resolved config
     * fragments (Q3, via {@code TimeInterval.clampToWindow}), for the resolved PV list. The samples
     * dispatchers retrieve a page as a sequence of such slices, each over every resolved PV, so
     * that every slice is complete across PVs before any row is emitted (issue #274, plan D1);
     * the first slice of a page begins at the resume timestamp ({@code pageStart}) or the earliest
     * fragment begin. Sorted by {@code (pvName, firstTimeSecs, firstTimeNanos)}; no keyset seek and
     * no {@code pageSize+1} probe. Returns null on a null/empty resolution, when no fragment
     * overlaps the slice, or on a retrieval failure (a database error, or a failed pvStats span
     * read — #232 plan D8). The samples dispatchers screen the first two before calling, so a null
     * they receive is reported as an error, never as an empty slice.
     */
    MongoCursor<BucketDocument> executeQuerySamplesV2(
            ResolvedQuery resolvedQuery,
            long windowBeginSecs, long windowBeginNanos,
            long windowEndSecs, long windowEndNanos);

    /**
     * Slice-drain form of
     * {@link #executeQuerySamplesV2(ResolvedQuery, long, long, long, long)}, carrying a scratch
     * holder that lets the implementation resolve the request's {@code pvStats} span-class
     * partition once for the whole page instead of once per slice (#274).
     *
     * <p>The partition depends only on the request's PV list and the stored spans, so it is the
     * same for every slice of one request; without the holder a page re-read {@code pvStats} once
     * per slice. The holder is created by the caller per retrieval loop and discarded with it —
     * that is what keeps this a <b>per-request</b> hoist. Spans must never be held across requests
     * (#232, plan D7): a stored span only grows, so a stale one is too small, and a too-small
     * {@code firstTime} bound silently omits buckets rather than failing.
     *
     * <p>Defaulted to the unhoisted call so a test double or an alternate client implementation
     * need not know about span classes at all; only {@code MongoSyncQueryClient} overrides it.
     */
    default MongoCursor<BucketDocument> executeQuerySamplesV2(
            ResolvedQuery resolvedQuery,
            long windowBeginSecs, long windowBeginNanos,
            long windowEndSecs, long windowEndNanos,
            SpanClassHolder spanClassHolder) {
        return executeQuerySamplesV2(
                resolvedQuery, windowBeginSecs, windowBeginNanos, windowEndSecs, windowEndNanos);
    }

    /**
     * A one-request scratch slot for the resolved span-class partition, so the samples slice drain
     * resolves it once per page rather than once per slice (#274).
     *
     * <p>Deliberately a plain mutable holder created and dropped inside a single retrieval loop,
     * not a cache: it is keyed by nothing, outlives nothing, and cannot be consulted by a later
     * request. See the #232 "never cache on the read side" invariant — the whole hazard is a span
     * that has grown since it was read, and a holder scoped to one page cannot go stale within it.
     */
    final class SpanClassHolder {
        private List<SpanClass> spanClasses;
        private boolean resolved = false;

        public boolean isResolved() {
            return resolved;
        }

        public List<SpanClass> get() {
            return spanClasses;
        }

        /** Records the partition for the rest of this page; null means resolution failed. */
        public void set(List<SpanClass> resolvedSpanClasses) {
            this.spanClasses = resolvedSpanClasses;
            this.resolved = true;
        }
    }

    /**
     * Resolves the query's sampleStatusSelector to the per-PV sets of epoch-nanos timestamps whose
     * statuses match it, over the same clamped page window the sample retrieval uses. Queries the
     * sampleStatusBuckets collection for the resolved PVs and the selector's (domain, layers) with
     * the standard span-overlap predicate, expands the matching documents, and keeps a timestamp
     * when its status code is in the selector's statusCodes (empty = any code). PVs with no
     * matching statuses are absent from the map. The map is the assembly-time join input: memory
     * is bounded by the number of labeled samples in the window, which since #274 is one time
     * slice {@code [windowBegin, windowEnd)} rather than the whole page. Returns an empty map when
     * the clamped window is empty, or null on database error.
     *
     * @throws DpException when a stored sample status document is malformed
     */
    Map<String, Set<Long>> resolveSampleStatusTimestamps(
            ResolvedQuery resolvedQuery,
            long windowBeginSecs, long windowBeginNanos,
            long windowEndSecs, long windowEndNanos) throws DpException;

    MongoCursor<ProviderDocument> executeQueryProviders(QueryProvidersRequest request);

    MongoCursor<ProviderMetadataQueryResultDocument> executeQueryProviderStats(QueryProviderStatsRequest request);

    MongoCursor<ProviderMetadataQueryResultDocument> executeQueryProviderStats(String id);
}
