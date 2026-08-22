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

    /**
     * Verifies that stored buckets satisfy the configured maximum bucket span before the query-side
     * time-range lower bound is applied (#197). Called at startup by every service that issues
     * bucket time-range queries — the query service directly, and the annotation service through
     * dataset export — since the flag it controls is process-wide.
     *
     * <p>Implemented by {@code MongoSyncClient}; defaults to a no-op for clients backed by
     * something other than the buckets collection, such as test stubs.
     *
     * @return true if startup may proceed; verification failure disables the bound rather than
     *         failing startup, so this does not return false in the default implementation
     */
    default boolean verifyBucketSpans() {
        return true;
    }

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
     * null on a null/empty resolution.
     */
    MongoCursor<BucketDocument> executeQueryBucketsV2(ResolvedQuery resolvedQuery);

    /**
     * Retrieves the full, unbounded bucket cursor for a Query API V2 streaming bucket query. Same
     * PV-name filter and {@code $or} fragment overlap as {@link #executeQueryBucketsV2}, but with no
     * keyset seek and no limit — the entire result is streamed to exhaustion and chunked into
     * messages downstream (fire-and-consume). Returns null on a null/empty resolution.
     */
    MongoCursor<BucketDocument> executeQueryBucketsV2Stream(ResolvedQuery resolvedQuery);

    /**
     * Retrieves buckets for a Query API V2 sample (column-table) query, over the page window
     * {@code [windowBeginSecs.windowBeginNanos, endTime)} intersected with the resolved config
     * fragments (Q3), for the resolved PV list. The window begin is the resume timestamp
     * ({@code pageStart}) on a continuation page, or each fragment's own begin on the first page;
     * the caller passes the effective window-begin so the same overlap machinery is reused. Sorted
     * by {@code (pvName, firstTimeSecs, firstTimeNanos)}. Unlike the bucket path there is no keyset
     * seek and no {@code pageSize+1} probe — the sample page is bounded by distinct-timestamp count
     * and the byte budget during assembly, not by a bucket-count limit. Returns null on a null/empty
     * resolution.
     */
    MongoCursor<BucketDocument> executeQuerySamplesV2(
            ResolvedQuery resolvedQuery, long windowBeginSecs, long windowBeginNanos);

    /**
     * Resolves the query's sampleStatusSelector to the per-PV sets of epoch-nanos timestamps whose
     * statuses match it, over the same clamped page window the sample retrieval uses. Queries the
     * sampleStatusBuckets collection for the resolved PVs and the selector's (domain, layers) with
     * the standard span-overlap predicate, expands the matching documents, and keeps a timestamp
     * when its status code is in the selector's statusCodes (empty = any code). PVs with no
     * matching statuses are absent from the map. The map is the assembly-time join input: memory
     * is bounded by the number of labeled samples in the window. Returns an empty map when the
     * clamped window is empty, or null on database error.
     *
     * @throws DpException when a stored sample status document is malformed
     */
    Map<String, Set<Long>> resolveSampleStatusTimestamps(
            ResolvedQuery resolvedQuery, long windowBeginSecs, long windowBeginNanos) throws DpException;

    MongoCursor<ProviderDocument> executeQueryProviders(QueryProvidersRequest request);

    MongoCursor<ProviderMetadataQueryResultDocument> executeQueryProviderStats(QueryProviderStatsRequest request);

    MongoCursor<ProviderMetadataQueryResultDocument> executeQueryProviderStats(String id);
}
