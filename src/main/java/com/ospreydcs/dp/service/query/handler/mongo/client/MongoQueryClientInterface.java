package com.ospreydcs.dp.service.query.handler.mongo.client;

import com.mongodb.client.MongoCursor;
import com.ospreydcs.dp.grpc.v1.query.*;
import com.ospreydcs.dp.service.common.bson.PvMetadataQueryResultDocument;
import com.ospreydcs.dp.service.common.bson.ProviderDocument;
import com.ospreydcs.dp.service.common.bson.ProviderMetadataQueryResultDocument;
import com.ospreydcs.dp.service.common.bson.bucket.BucketDocument;
import com.ospreydcs.dp.service.common.bson.dataset.DataBlockDocument;
import com.ospreydcs.dp.service.query.handler.model.TimeInterval;
import org.bson.conversions.Bson;

import java.util.Collection;
import java.util.List;

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

    MongoCursor<ProviderDocument> executeQueryProviders(QueryProvidersRequest request);

    MongoCursor<ProviderMetadataQueryResultDocument> executeQueryProviderStats(QueryProviderStatsRequest request);

    MongoCursor<ProviderMetadataQueryResultDocument> executeQueryProviderStats(String id);
}
