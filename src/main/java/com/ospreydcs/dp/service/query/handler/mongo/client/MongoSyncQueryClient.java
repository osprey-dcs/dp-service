package com.ospreydcs.dp.service.query.handler.mongo.client;

import com.mongodb.client.MongoCursor;
import com.mongodb.client.model.Accumulators;
import com.mongodb.client.model.Aggregates;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.Projections;
import com.ospreydcs.dp.grpc.v1.query.*;
import com.ospreydcs.dp.service.common.bson.BsonConstants;
import com.ospreydcs.dp.service.common.bson.PvMetadataQueryResultDocument;
import com.ospreydcs.dp.service.common.bson.ProviderDocument;
import com.ospreydcs.dp.service.common.bson.ProviderMetadataQueryResultDocument;
import com.ospreydcs.dp.service.common.bson.bucket.BucketDocument;
import com.ospreydcs.dp.service.common.bson.configuration.ConfigurationActivationDocument;
import com.ospreydcs.dp.service.common.bson.dataset.DataBlockDocument;
import com.ospreydcs.dp.service.common.bson.pvmetadata.PvMetadataDocument;
import com.ospreydcs.dp.service.common.mongo.MongoQueryFilterBuilder;
import com.ospreydcs.dp.service.common.mongo.MongoSyncClient;
import com.ospreydcs.dp.service.query.handler.model.KeysetPosition;
import com.ospreydcs.dp.service.query.handler.model.ResolvedQuery;
import com.ospreydcs.dp.service.query.handler.model.TimeInterval;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.bson.conversions.Bson;
import org.bson.types.ObjectId;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.regex.Pattern;

import static com.mongodb.client.model.Filters.*;
import static com.mongodb.client.model.Indexes.ascending;

public class MongoSyncQueryClient extends MongoSyncClient implements MongoQueryClientInterface {

    private static final Logger logger = LogManager.getLogger();

    public MongoCursor<BucketDocument> executeBucketDocumentQuery(
            Bson columnNameFilter,
            long startTimeSeconds,
            long startTimeNanos,
            long endTimeSeconds,
            long endTimeNanos
    ) {
        // Bucket overlap predicate (firstTime < end AND lastTime >= begin) built from the shared
        // filter builder so the V1 retrieval path and the V2 $or fragmentation cannot drift.
        final Bson overlapFilter = MongoQueryFilterBuilder.bucketOverlapsRangeFilter(
                startTimeSeconds, startTimeNanos, endTimeSeconds, endTimeNanos);
        final Bson filter = and(columnNameFilter, overlapFilter);

        logger.debug("executing query columns: " + columnNameFilter
                + " startSeconds: " + startTimeSeconds
                + " endSeconds: " + endTimeSeconds);

        return mongoCollectionBuckets
                .find(filter)
                .sort(ascending(
                        BsonConstants.BSON_KEY_PV_NAME,
                        BsonConstants.BSON_KEY_BUCKET_FIRST_TIME_SECS,
                        BsonConstants.BSON_KEY_BUCKET_FIRST_TIME_NANOS
                ))
                .cursor();
    }

    @Override
    public MongoCursor<BucketDocument> executeDataBlockQuery(DataBlockDocument dataBlock) {

        final long startTimeSeconds = dataBlock.getBeginTime().getSeconds();
        final long startTimeNanos = dataBlock.getBeginTime().getNanos();
        final long endTimeSeconds = dataBlock.getEndTime().getSeconds();
        final long endTimeNanos = dataBlock.getEndTime().getNanos();

        final Bson columnNameFilter = in(BsonConstants.BSON_KEY_PV_NAME, dataBlock.getPvNames());
        return executeBucketDocumentQuery(
                columnNameFilter, startTimeSeconds, startTimeNanos, endTimeSeconds, endTimeNanos);
    }

    @Override
    public MongoCursor<BucketDocument> executeQueryData(QueryDataRequest.QuerySpec querySpec) {

        // snippet to get query plan
//        Document explanation = collection.find().explain(ExplainVerbosity.EXECUTION_STATS);
//        List<String> keys = Arrays.asList("queryPlanner", "winningPlan");
//        System.out.println(explanation.getEmbedded(keys, Document.class).toJson());

        final long startTimeSeconds = querySpec.getBeginTime().getEpochSeconds();
        final long startTimeNanos = querySpec.getBeginTime().getNanoseconds();
        final long endTimeSeconds = querySpec.getEndTime().getEpochSeconds();
        final long endTimeNanos = querySpec.getEndTime().getNanoseconds();

        final Bson columnNameFilter = in(BsonConstants.BSON_KEY_PV_NAME, querySpec.getPvNamesList());
        return executeBucketDocumentQuery(
                columnNameFilter, startTimeSeconds, startTimeNanos, endTimeSeconds, endTimeNanos);
    }

    @Override
    public MongoCursor<BucketDocument> executeQueryTable(QueryTableRequest request) {
        
        final long startTimeSeconds = request.getBeginTime().getEpochSeconds();
        final long startTimeNanos = request.getBeginTime().getNanoseconds();
        final long endTimeSeconds = request.getEndTime().getEpochSeconds();
        final long endTimeNanos = request.getEndTime().getNanoseconds();

        // create name filter using either list of pv names, or pv name pattern
        Bson columnNameFilter = null;
        switch (request.getPvNameSpecCase()) {
            case PVNAMELIST -> {
                columnNameFilter = in(BsonConstants.BSON_KEY_PV_NAME, request.getPvNameList().getPvNamesList());
            }
            case PVNAMEPATTERN -> {
                final Pattern pvNamePattern = Pattern.compile(
                        request.getPvNamePattern().getPattern(), Pattern.CASE_INSENSITIVE);
                columnNameFilter = Filters.regex(BsonConstants.BSON_KEY_PV_NAME, pvNamePattern);
            }
            case PVNAMESPEC_NOT_SET -> {
                return null;
            }
        }

        // execute query
        return executeBucketDocumentQuery(
                columnNameFilter, startTimeSeconds, startTimeNanos, endTimeSeconds, endTimeNanos);
    }

    private MongoCursor<PvMetadataQueryResultDocument> executeQueryPvMetadata(Bson columnNameFilter) {

        // NOTE: PROJECTION MUST INCLUDE KEYS FOR ALL FIELDS USED IN SORTING and GROUPING!!!
        // If not the values will silently be null and lead to unexpected results!!

        Bson bucketFieldProjection = Projections.fields(Projections.include(
                BsonConstants.BSON_KEY_PV_NAME,
                BsonConstants.BSON_KEY_BUCKET_ID,
                BsonConstants.BSON_KEY_BUCKET_DATA_TYPE,
                BsonConstants.BSON_KEY_BUCKET_DATA_TIMESTAMPS_CASE,
                BsonConstants.BSON_KEY_BUCKET_DATA_TIMESTAMPS_TYPE,
                BsonConstants.BSON_KEY_BUCKET_FIRST_TIME,
                BsonConstants.BSON_KEY_BUCKET_FIRST_TIME_SECS,
                BsonConstants.BSON_KEY_BUCKET_FIRST_TIME_NANOS,
                BsonConstants.BSON_KEY_BUCKET_LAST_TIME,
                BsonConstants.BSON_KEY_BUCKET_SAMPLE_COUNT,
                BsonConstants.BSON_KEY_BUCKET_SAMPLE_PERIOD,
                BsonConstants.BSON_KEY_BUCKET_PROVIDER_ID,
                BsonConstants.BSON_KEY_BUCKET_PROVIDER_NAME
        ));

        // Sort fields must appear in projection.
        Bson bucketSort = ascending(
                BsonConstants.BSON_KEY_PV_NAME,
                BsonConstants.BSON_KEY_BUCKET_FIRST_TIME_SECS,
                BsonConstants.BSON_KEY_BUCKET_FIRST_TIME_NANOS);

        Bson metadataSort = ascending(BsonConstants.BSON_KEY_PV_METADATA_PV_NAME);

        logger.debug("executeQueryMetadata query: {}", columnNameFilter.toString());

        var aggregateIterable = mongoCollectionBuckets.withDocumentClass(PvMetadataQueryResultDocument.class)
                .aggregate(
                        Arrays.asList(
                                Aggregates.match(columnNameFilter),
                                Aggregates.project(bucketFieldProjection),
                                Aggregates.sort(bucketSort), // sort buckets here so that records are ordered for group opeator

                                // Bucket fields for grouping must appear in projection!!
                                Aggregates.group(
                                        "$" + BsonConstants.BSON_KEY_PV_NAME,
                                        Accumulators.last(
                                                BsonConstants.BSON_KEY_PV_METADATA_PV_NAME,
                                                "$" + BsonConstants.BSON_KEY_PV_NAME),
                                        Accumulators.last(
                                                BsonConstants.BSON_KEY_PV_METADATA_LAST_BUCKET_ID,
                                                "$" + BsonConstants.BSON_KEY_BUCKET_ID),
                                        Accumulators.last(
                                                BsonConstants.BSON_KEY_PV_METADATA_LAST_BUCKET_DATA_TYPE,
                                                "$" + BsonConstants.BSON_KEY_BUCKET_DATA_TYPE),
                                        Accumulators.last(
                                                BsonConstants.BSON_KEY_PV_METADATA_LAST_BUCKET_DATA_TIMESTAMPS_CASE,
                                                "$" + BsonConstants.BSON_KEY_BUCKET_DATA_TIMESTAMPS_CASE),
                                        Accumulators.last(
                                                BsonConstants.BSON_KEY_PV_METADATA_LAST_BUCKET_DATA_TIMESTAMPS_TYPE,
                                                "$" + BsonConstants.BSON_KEY_BUCKET_DATA_TIMESTAMPS_TYPE),
                                        Accumulators.last(
                                                BsonConstants.BSON_KEY_PV_METADATA_LAST_BUCKET_SAMPLE_COUNT,
                                                "$" + BsonConstants.BSON_KEY_BUCKET_SAMPLE_COUNT),
                                        Accumulators.last(
                                                BsonConstants.BSON_KEY_PV_METADATA_LAST_BUCKET_SAMPLE_PERIOD,
                                                "$" + BsonConstants.BSON_KEY_BUCKET_SAMPLE_PERIOD),
                                        Accumulators.first(
                                                // save the first time of the first document in group to the firstTime field
                                                BsonConstants.BSON_KEY_PV_METADATA_FIRST_DATA_TIMESTAMP,
                                                "$" + BsonConstants.BSON_KEY_BUCKET_FIRST_TIME),
                                        Accumulators.last(
                                                // save the last time of the last document to the lastTime field
                                                BsonConstants.BSON_KEY_PV_METADATA_LAST_DATA_TIMESTAMP,
                                                "$" + BsonConstants.BSON_KEY_BUCKET_LAST_TIME),
                                        Accumulators.sum(
                                                // count number of bucket documents in group for this pv
                                                BsonConstants.BSON_KEY_PV_METADATA_NUM_BUCKETS,
                                                1),
                                        Accumulators.last(
                                                // save the providerId of the last document to the providerid field
                                                BsonConstants.BSON_KEY_PV_METADATA_LAST_PROVIDER_ID,
                                                "$" + BsonConstants.BSON_KEY_BUCKET_PROVIDER_ID),
                                        Accumulators.last(
                                                // save the providerName of the last document to the providerName field
                                                BsonConstants.BSON_KEY_PV_METADATA_LAST_PROVIDER_NAME,
                                                "$" + BsonConstants.BSON_KEY_BUCKET_PROVIDER_NAME)
                                ),
                                Aggregates.sort(metadataSort) // sort metadata documents so result is sorted
                                ));

//        aggregateIterable.forEach(bucketDocument -> {System.out.println(bucketDocument.toString());});

        return aggregateIterable.cursor();
    }

    @Override
    public MongoCursor<PvMetadataQueryResultDocument> executeQueryPvStats(Collection<String> pvNameList) {
        final Bson pvNameFilter = in(BsonConstants.BSON_KEY_PV_NAME, pvNameList);
        return executeQueryPvMetadata(pvNameFilter);
    }

    @Override
    public MongoCursor<PvMetadataQueryResultDocument> executeQueryPvStats(String pvNamePatternString) {
        final Pattern pvNamePattern = Pattern.compile(pvNamePatternString, Pattern.CASE_INSENSITIVE);
        final Bson pvNameFilter = Filters.regex(BsonConstants.BSON_KEY_PV_NAME, pvNamePattern);
        return executeQueryPvMetadata(pvNameFilter);
    }

    @Override
    public Collection<String> executeQueryPvExistence(Collection<String> pvNameList) {

        // An empty (or null) list has no existing PVs by definition; short-circuit to avoid a
        // round-trip to MongoDB and a null $in filter (which would throw).
        if (pvNameList == null || pvNameList.isEmpty()) {
            return new HashSet<>();
        }

        // Cheap existence check: a distinct on the pvName index restricted to the requested names.
        // Unlike executeQueryPvStats(), this does not sort or group over every bucket for each PV -
        // it only needs to know which of the requested names appear at all in the archive.
        final Bson pvNameFilter = in(BsonConstants.BSON_KEY_PV_NAME, pvNameList);

        try {
            final Set<String> existingPvNames = new HashSet<>();
            try (final MongoCursor<String> cursor = mongoCollectionBuckets
                    .distinct(BsonConstants.BSON_KEY_PV_NAME, pvNameFilter, String.class)
                    .iterator()) {
                while (cursor.hasNext()) {
                    existingPvNames.add(cursor.next());
                }
            }
            return existingPvNames;

        } catch (Exception ex) {
            logger.error("executeQueryPvExistence database error for {} pv name(s): {}",
                    pvNameList.size(), ex.getMessage(), ex);
            return null;
        }
    }

    @Override
    public List<String> resolvePvNamesByPattern(String pvNamePattern) {

        // Compile the pattern up front so an invalid regex surfaces as a PatternSyntaxException the
        // caller can turn into a clean reject (Q10), rather than failing deep in the driver.
        final Pattern compiled = Pattern.compile(pvNamePattern, Pattern.CASE_INSENSITIVE);
        final Bson pvNameFilter = Filters.regex(BsonConstants.BSON_KEY_PV_NAME, compiled);

        try {
            final List<String> pvNames = new ArrayList<>();
            try (final MongoCursor<String> cursor = mongoCollectionBuckets
                    .distinct(BsonConstants.BSON_KEY_PV_NAME, pvNameFilter, String.class)
                    .iterator()) {
                while (cursor.hasNext()) {
                    pvNames.add(cursor.next());
                }
            }
            return pvNames;
        } catch (Exception ex) {
            logger.error("resolvePvNamesByPattern database error: {}", ex.getMessage(), ex);
            return null;
        }
    }

    @Override
    public List<String> resolvePvNamesByMetadata(List<Bson> criteriaFilters) {

        // An empty criteria list matches all metadata records.
        final Bson metadataFilter = (criteriaFilters == null || criteriaFilters.isEmpty())
                ? Filters.exists(BsonConstants.BSON_KEY_PV_METADATA_PV_NAME)
                : and(criteriaFilters);

        try {
            // Collect the pvName of every matching metadata record.
            final Set<String> matchedNames = new HashSet<>();
            try (final MongoCursor<PvMetadataDocument> cursor =
                         mongoCollectionPvMetadata.find(metadataFilter).iterator()) {
                while (cursor.hasNext()) {
                    matchedNames.add(cursor.next().getPvName());
                }
            }

            if (matchedNames.isEmpty()) {
                return new ArrayList<>();
            }

            // Intersect with archive existence (Q11 b): drop names that have no buckets at all.
            final List<String> existing = new ArrayList<>();
            final Bson existenceFilter = in(BsonConstants.BSON_KEY_PV_NAME, matchedNames);
            try (final MongoCursor<String> cursor = mongoCollectionBuckets
                    .distinct(BsonConstants.BSON_KEY_PV_NAME, existenceFilter, String.class)
                    .iterator()) {
                while (cursor.hasNext()) {
                    existing.add(cursor.next());
                }
            }
            return existing;

        } catch (Exception ex) {
            logger.error("resolvePvNamesByMetadata database error: {}", ex.getMessage(), ex);
            return null;
        }
    }

    @Override
    public List<TimeInterval> resolveConfigurationIntervals(List<Bson> criteriaFilters) {

        // A configuration selector with no criteria matches nothing (the resolver short-circuits this
        // case before calling). Defensively honor that contract here too: an empty filter list yields
        // no intervals rather than scanning and unioning every activation in the collection.
        if (criteriaFilters == null || criteriaFilters.isEmpty()) {
            return new ArrayList<>();
        }
        final Bson activationFilter = and(criteriaFilters);

        try {
            final List<TimeInterval> intervals = new ArrayList<>();
            try (final MongoCursor<ConfigurationActivationDocument> cursor =
                         mongoCollectionConfigurationActivations.find(activationFilter).iterator()) {
                while (cursor.hasNext()) {
                    final ConfigurationActivationDocument activation = cursor.next();
                    final Instant start = activation.getStartTime();
                    if (start == null) {
                        continue; // an activation with no start time cannot bound a retrieval range
                    }
                    final Instant end = activation.getEndTime(); // null = open-ended
                    final long endSecs = (end == null) ? Long.MAX_VALUE : end.getEpochSecond();
                    final long endNanos = (end == null) ? 0L : end.getNano();
                    intervals.add(new TimeInterval(
                            start.getEpochSecond(), start.getNano(), endSecs, endNanos));
                }
            }
            return intervals;

        } catch (Exception ex) {
            logger.error("resolveConfigurationIntervals database error: {}", ex.getMessage(), ex);
            return null;
        }
    }

    @Override
    public MongoCursor<BucketDocument> executeQueryBucketsV2(ResolvedQuery resolvedQuery) {

        if (resolvedQuery == null || resolvedQuery.isEmptyResult()) {
            return null;
        }

        // Base filter: PV-name filter AND the $or of per-fragment overlap predicates.
        final List<Bson> andParts = new ArrayList<>();
        andParts.add(bucketBaseFilterV2(resolvedQuery));

        // Keyset seek (unary paging) is ANDed at top level, NOT distributed into the $or branches
        // (Q3 correctness note). Absent on the first page and on streaming queries.
        final KeysetPosition pageStart = resolvedQuery.getPageStart();
        if (pageStart != null) {
            andParts.add(bucketKeysetSeekFilter(pageStart));
        }

        try {
            return mongoCollectionBuckets
                    .find(and(andParts))
                    .sort(bucketV2Sort())
                    .limit(resolvedQuery.getPageSize() + 1) // +1 probe row to detect a following page
                    .cursor();
        } catch (Exception ex) {
            logger.error("executeQueryBucketsV2 database error: {}", ex.getMessage(), ex);
            return null;
        }
    }

    @Override
    public MongoCursor<BucketDocument> executeQueryBucketsV2Stream(ResolvedQuery resolvedQuery) {

        if (resolvedQuery == null || resolvedQuery.isEmptyResult()) {
            return null;
        }

        // Streaming is fire-and-consume: no keyset seek and no limit — the full result of the
        // (resolved intervals × PV list) overlap query is streamed to exhaustion, chunked downstream.
        try {
            return mongoCollectionBuckets
                    .find(bucketBaseFilterV2(resolvedQuery))
                    .sort(bucketV2Sort())
                    .cursor();
        } catch (Exception ex) {
            logger.error("executeQueryBucketsV2Stream database error: {}", ex.getMessage(), ex);
            return null;
        }
    }

    @Override
    public MongoCursor<BucketDocument> executeQuerySamplesV2(
            ResolvedQuery resolvedQuery, long windowBeginSecs, long windowBeginNanos) {

        if (resolvedQuery == null || resolvedQuery.isEmptyResult()) {
            return null;
        }

        final Bson pvNameFilter = in(BsonConstants.BSON_KEY_PV_NAME, resolvedQuery.getPvNames());

        // Per-fragment overlap predicates with each fragment's lower bound clamped to the page window
        // begin (windowBegin = resume timestamp on a continuation page, or timeRange begin on page 1).
        // The clamp lives on TimeInterval so this filter and the dispatcher's sample-level retention
        // trim are derived from the same interval set (#207) — see clampToWindowBegin.
        final List<Bson> fragmentFilters = new ArrayList<>();
        for (TimeInterval interval : TimeInterval.clampToWindowBegin(
                resolvedQuery.getRetrievalIntervals(), windowBeginSecs, windowBeginNanos)) {
            fragmentFilters.add(MongoQueryFilterBuilder.bucketOverlapsRangeFilter(
                    interval.getBeginSeconds(), interval.getBeginNanos(),
                    interval.getEndSeconds(), interval.getEndNanos()));
        }

        if (fragmentFilters.isEmpty()) {
            // nothing overlaps the page window
            return null;
        }

        final Bson overlapFilter = (fragmentFilters.size() == 1)
                ? fragmentFilters.get(0)
                : or(fragmentFilters);

        try {
            return mongoCollectionBuckets
                    .find(and(pvNameFilter, overlapFilter))
                    .sort(bucketV2Sort())
                    .cursor();
        } catch (Exception ex) {
            logger.error("executeQuerySamplesV2 database error: {}", ex.getMessage(), ex);
            return null;
        }
    }

    /**
     * Base V2 bucket filter shared by the unary and streaming retrieval paths: the resolved PV-name
     * {@code in(...)} filter AND the {@code $or} of the per-fragment bucket-overlap predicates
     * (single fragment → no {@code $or} wrapper). Built from the shared filter builder so V1 and V2
     * overlap semantics cannot drift.
     */
    private static Bson bucketBaseFilterV2(ResolvedQuery resolvedQuery) {
        final Bson pvNameFilter = in(BsonConstants.BSON_KEY_PV_NAME, resolvedQuery.getPvNames());

        final List<Bson> fragmentFilters = new ArrayList<>();
        for (TimeInterval interval : resolvedQuery.getRetrievalIntervals()) {
            fragmentFilters.add(MongoQueryFilterBuilder.bucketOverlapsRangeFilter(
                    interval.getBeginSeconds(), interval.getBeginNanos(),
                    interval.getEndSeconds(), interval.getEndNanos()));
        }
        final Bson overlapFilter = (fragmentFilters.size() == 1)
                ? fragmentFilters.get(0)
                : or(fragmentFilters);

        return and(pvNameFilter, overlapFilter);
    }

    /** Compound V2 bucket sort {@code (pvName, firstTimeSecs, firstTimeNanos)}. */
    private static Bson bucketV2Sort() {
        return ascending(
                BsonConstants.BSON_KEY_PV_NAME,
                BsonConstants.BSON_KEY_BUCKET_FIRST_TIME_SECS,
                BsonConstants.BSON_KEY_BUCKET_FIRST_TIME_NANOS);
    }

    /**
     * Keyset seek predicate selecting buckets strictly after {@code (pvName, firstTimeSecs,
     * firstTimeNanos)} in the compound sort order (Q2). Lexicographic tuple {@code >}. No tiebreaker
     * needed — the composite bucket {@code _id} proves {@code (pvName, firstTime)} uniqueness.
     */
    private static Bson bucketKeysetSeekFilter(KeysetPosition pos) {
        final String p = pos.getPvName();
        final long s = pos.getSeconds();
        final long n = pos.getNanos();
        return or(
                gt(BsonConstants.BSON_KEY_PV_NAME, p),
                and(
                        eq(BsonConstants.BSON_KEY_PV_NAME, p),
                        gt(BsonConstants.BSON_KEY_BUCKET_FIRST_TIME_SECS, s)),
                and(
                        eq(BsonConstants.BSON_KEY_PV_NAME, p),
                        eq(BsonConstants.BSON_KEY_BUCKET_FIRST_TIME_SECS, s),
                        gt(BsonConstants.BSON_KEY_BUCKET_FIRST_TIME_NANOS, n)));
    }

    @Override
    public MongoCursor<PvMetadataQueryResultDocument> executeQueryPvStats(QueryPvStatsRequest request) {
        if (request.hasPvNameList()) {
            return executeQueryPvStats(request.getPvNameList().getPvNamesList());
        } else {
            return executeQueryPvStats(request.getPvNamePattern().getPattern());
        }
    }

    @Override
    public MongoCursor<ProviderDocument> executeQueryProviders(QueryProvidersRequest request) {
        
        // create filter to select providers
        final List<Bson> globalFilterList = new ArrayList<>();
        final List<Bson> criteriaFilterList = new ArrayList<>();
        final List<QueryProvidersRequest.Criterion> criterionList = request.getCriteriaList();
        for (QueryProvidersRequest.Criterion criterion : criterionList) {

            switch (criterion.getCriterionCase()) {

                case IDCRITERION -> {
                    // provider id filter, combined with other filters by AND operator
                    final String providerId = criterion.getIdCriterion().getId();
                    if (!providerId.isBlank()) {
                        Bson idFilter = Filters.eq(BsonConstants.BSON_KEY_PROVIDER_ID, new ObjectId(providerId));
                        globalFilterList.add(idFilter);
                    }
                }

                case TEXTCRITERION -> {
                    // name filter, combined with other filters by AND operator
                    final String nameText = criterion.getTextCriterion().getText();
                    if ( ! nameText.isBlank()) {
                        final Bson nameFilter = Filters.text(nameText);
                        globalFilterList.add(nameFilter);
                    }
                }

                case TAGSCRITERION -> {
                    // tags filter, combined with other filters by OR operator
                    final String tagValue = criterion.getTagsCriterion().getTagValue();
                    if ( ! tagValue.isBlank()) {
                        Bson tagsFilter = Filters.in(BsonConstants.BSON_KEY_TAGS, tagValue);
                        criteriaFilterList.add(tagsFilter);
                    }
                }

                case ATTRIBUTESCRITERION -> {
                    // attributes filter, combined with other filters by OR operator
                    final String attributeKey = criterion.getAttributesCriterion().getKey();
                    final String attributeValue = criterion.getAttributesCriterion().getValue();
                    if ( ! attributeKey.isBlank() && ! attributeValue.isBlank()) {
                        final String mapKey = BsonConstants.BSON_KEY_ATTRIBUTES + "." + attributeKey;
                        Bson attributesFilter = Filters.eq(mapKey, attributeValue);
                        criteriaFilterList.add(attributesFilter);
                    }
                }

                case CRITERION_NOT_SET -> {
                    // shouldn't happen since validation checks for this, but...
                    logger.error("executeQueryProviders unexpected error criterion case not set");
                }
            }
        }

        if (globalFilterList.isEmpty() && criteriaFilterList.isEmpty()) {
            // shouldn't happen since validation checks for this, but...
            logger.debug("no search criteria specified in QueryAnnotationsRequest filter");
            return null;
        }

        // create global filter to be combined with and operator (default matches all Annotations)
        Bson globalFilter = Filters.exists(BsonConstants.BSON_KEY_ANNOTATION_ID);
        if (globalFilterList.size() > 0) {
            globalFilter = and(globalFilterList);
        }

        // create criteria filter to be combined with or operator (default matches all Annotations)
        Bson criteriaFilter = Filters.exists(BsonConstants.BSON_KEY_ANNOTATION_ID);
        if (criteriaFilterList.size() > 0) {
            criteriaFilter = or(criteriaFilterList);
        }

        // combine global filter with criteria filter using and operator
        final Bson queryFilter = and(globalFilter, criteriaFilter);
        
        logger.debug("executing queryProviders filter: " + queryFilter.toString());

        final MongoCursor<ProviderDocument> resultCursor = mongoCollectionProviders
                .find(queryFilter)
                .sort(ascending(BsonConstants.BSON_KEY_PROVIDER_NAME))
                .cursor();

        if (resultCursor == null) {
            logger.error("executeQueryProviders received null cursor from mongodb.find");
        }

        return resultCursor;
    }

    @Override
    public MongoCursor<ProviderMetadataQueryResultDocument> executeQueryProviderStats(
            QueryProviderStatsRequest request
    ) {
        if (request.getProviderId().isBlank()) {
            // this has already been validated but just in case...
            logger.error("executeQueryProviderStats unexpected error providerId not specified");
            return null;
        }

        return executeQueryProviderStats(request.getProviderId());
    }

    @Override
    public MongoCursor<ProviderMetadataQueryResultDocument> executeQueryProviderStats(String providerid) {

        // generate filter for buckets query by providerId
        final Bson providerIdFilter = eq(BsonConstants.BSON_KEY_BUCKET_PROVIDER_ID, providerid);

        // NOTE: PROJECTION MUST INCLUDE KEYS FOR ALL FIELDS USED IN SORTING and GROUPING!!!
        // If not the values will silently be null and lead to unexpected results!!
        Bson bucketFieldProjection = Projections.fields(Projections.include(
                BsonConstants.BSON_KEY_BUCKET_PROVIDER_ID,
                BsonConstants.BSON_KEY_PV_NAME,
                BsonConstants.BSON_KEY_BUCKET_FIRST_TIME
        ));

        // Sort fields must appear in projection.  Specifies sorting of documents with specified providerId by bucket firstTime.
        Bson bucketSort = ascending(BsonConstants.BSON_KEY_BUCKET_FIRST_TIME);

        // This is used to sort the result of the final aggregated result.
        Bson metadataSort = ascending(BsonConstants.BSON_KEY_BUCKET_PROVIDER_ID);

        logger.debug("executeQueryProviderStats query: {}", providerIdFilter.toString());

        var aggregateIterable = mongoCollectionBuckets.withDocumentClass(ProviderMetadataQueryResultDocument.class)
                .aggregate(
                        Arrays.asList(
                                Aggregates.match(providerIdFilter),
                                Aggregates.project(bucketFieldProjection),
                                Aggregates.sort(bucketSort), // sort buckets here so that records are ordered for group opeator

                                // Bucket fields for grouping must appear in projection!!
                                Aggregates.group(
                                        "$" + BsonConstants.BSON_KEY_BUCKET_PROVIDER_ID,
                                        Accumulators.addToSet(
                                                // collect a set of unique PV names for this provider
                                                BsonConstants.BSON_KEY_PROVIDER_METADATA_PV_NAMES,
                                                "$" + BsonConstants.BSON_KEY_PV_NAME),
                                        Accumulators.first(
                                                // save the first time of the first bucket document for this provider
                                                BsonConstants.BSON_KEY_PROVIDER_METADATA_FIRST_BUCKET_TIMESTAMP,
                                                "$" + BsonConstants.BSON_KEY_BUCKET_FIRST_TIME),
                                        Accumulators.last(
                                                // save the first time of the last bucket document for this provider
                                                BsonConstants.BSON_KEY_PROVIDER_METADATA_LAST_BUCKET_TIMESTAMP,
                                                "$" + BsonConstants.BSON_KEY_BUCKET_FIRST_TIME),
                                        Accumulators.sum(
                                                // count number of bucket documents in group for this provider
                                                BsonConstants.BSON_KEY_PROVIDER_METADATA_NUM_BUCKETS,
                                                1)
                                ),
                                Aggregates.sort(metadataSort) // sort metadata documents so result is sorted
                        ));

        // aggregateIterable.forEach(bucketDocument -> {System.out.println(bucketDocument.toString());});

        return aggregateIterable.cursor();
    }

}
