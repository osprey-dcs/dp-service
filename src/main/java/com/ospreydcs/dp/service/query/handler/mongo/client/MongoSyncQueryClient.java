package com.ospreydcs.dp.service.query.handler.mongo.client;

import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoIterable;
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
import com.ospreydcs.dp.service.common.bson.pvstats.PvStatsDocument;
import com.ospreydcs.dp.service.common.bson.samplestatus.SampleStatusBucketDocument;
import com.ospreydcs.dp.service.common.bson.samplestatus.SampleStatusDocumentUtility;
import com.ospreydcs.dp.service.common.exception.DpException;
import com.ospreydcs.dp.service.common.mongo.MongoClientBase;
import com.ospreydcs.dp.service.common.mongo.MongoQueryFilterBuilder;
import com.ospreydcs.dp.service.common.mongo.MongoSyncClient;
import com.ospreydcs.dp.service.common.mongo.TimedMongoCursor;
import com.ospreydcs.dp.service.query.handler.model.KeysetPosition;
import com.ospreydcs.dp.service.query.handler.model.ResolvedQuery;
import com.ospreydcs.dp.service.query.handler.model.ResolvedStatusFilter;
import com.ospreydcs.dp.service.query.handler.model.TimeInterval;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.bson.conversions.Bson;
import org.bson.types.ObjectId;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.regex.Pattern;

import static com.mongodb.client.model.Filters.*;
import static com.mongodb.client.model.Indexes.ascending;

public class MongoSyncQueryClient extends MongoSyncClient implements MongoQueryClientInterface {

    private static final Logger logger = LogManager.getLogger();

    /**
     * Resolves the {@code firstTime} lower-bound span for a query naming {@code pvNames}: the
     * largest {@code maxBucketSpanSeconds} over their {@code pvStats} documents (#232, plan D1).
     * One indexed {@code $in} read on {@code _id}. A PV with no document contributes nothing
     * (plan D6), so an empty list or no matches yields {@code 0}, which the filter builder turns
     * into {@code firstTime.seconds >= beginSeconds} -- correct for PVs with no stored buckets.
     *
     * <p>Read fresh on every query, deliberately not cached (plan D7): a cached value can only be
     * too small once a longer bucket is ingested, and a too-small bound silently drops that bucket.
     *
     * @throws DpException if the read fails; the caller must report the query as an error rather
     *                     than fall back to the unbounded scan (plan D8)
     */
    public long resolveMaxBucketSpanSeconds(Collection<String> pvNames) throws DpException {
        if (pvNames == null || pvNames.isEmpty()) {
            return 0L;
        }
        return resolveMaxBucketSpanSeconds(
                in(BsonConstants.BSON_KEY_PV_STATS_PV_NAME, pvNames), pvNames.size() + " named PVs");
    }

    /**
     * Pattern variant of {@link #resolveMaxBucketSpanSeconds(Collection)} (plan D11): the V1 table
     * query's pattern branch has no PV list, so the same regex is matched against
     * {@code pvStats._id} and the maximum taken over the matches. The same single read as the list
     * case, and a tighter bound than a global maximum.
     */
    public long resolveMaxBucketSpanSeconds(Pattern pvNamePattern) throws DpException {
        return resolveMaxBucketSpanSeconds(
                regex(BsonConstants.BSON_KEY_PV_STATS_PV_NAME, pvNamePattern),
                "pattern " + pvNamePattern);
    }

    private long resolveMaxBucketSpanSeconds(Bson pvStatsFilter, String description) throws DpException {
        long maxBucketSpanSeconds = 0L;
        for (long documentSpanSeconds : readPvStatsSpans(pvStatsFilter, description).values()) {
            maxBucketSpanSeconds = Math.max(maxBucketSpanSeconds, documentSpanSeconds);
        }
        logger.debug("resolved maxBucketSpanSeconds: {} for {}", maxBucketSpanSeconds, description);
        return maxBucketSpanSeconds;
    }

    /**
     * Partitions a request's PVs into {@link SpanClass}es by each PV's own {@code pvStats} span
     * (issue #274, plan D11): the same single {@code $in} read on {@code _id} as
     * {@link #resolveMaxBucketSpanSeconds(Collection)}, but the spans are kept per PV instead of
     * collapsed to one maximum, so each class's find can carry its own {@code firstTime} lower
     * bound. A PV with no document falls in class 0 with span 0 (plan D6). An empty name list
     * yields an empty partition, which no retrieval method reaches -- every caller screens it.
     *
     * @throws DpException if the read fails; the caller must report the query as an error rather
     *                     than fall back to the unbounded scan (plan D8)
     */
    public List<SpanClass> resolveSpanClasses(Collection<String> pvNames) throws DpException {
        if (pvNames == null || pvNames.isEmpty()) {
            return List.of();
        }
        final Map<String, Long> spanByPv = readPvStatsSpans(
                in(BsonConstants.BSON_KEY_PV_STATS_PV_NAME, pvNames), pvNames.size() + " named PVs");
        final List<SpanClass> classes = SpanClass.partition(pvNames, spanByPv);
        logger.debug("resolved {} span classes for {}", classes.size(), pvNames.size() + " named PVs");
        return classes;
    }

    /**
     * Reads the {@code pvStats} documents matching {@code pvStatsFilter} into a pvName-to-span
     * map. A negative stored span is logged and clamped to 0 (see the comment inside); a document
     * with no span is absent from the map.
     */
    private Map<String, Long> readPvStatsSpans(Bson pvStatsFilter, String description) throws DpException {
        final Map<String, Long> spanByPv = new HashMap<>();
        try (MongoCursor<PvStatsDocument> cursor = mongoCollectionPvStats
                .find(pvStatsFilter)
                .projection(Projections.include(BsonConstants.BSON_KEY_PV_STATS_MAX_BUCKET_SPAN_SECONDS))
                .cursor()) {
            while (cursor.hasNext()) {
                final PvStatsDocument document = cursor.next();
                final long documentSpanSeconds = document.getMaxBucketSpanSeconds();
                if (documentSpanSeconds < 0) {
                    // No writing path can store this: ingestion only ever issues $max with a
                    // non-negative span, and the v5 seed filters $gte 0 before merging. So it means
                    // the collection was edited by hand or corrupted, and it is worth an operator's
                    // attention -- but not a refusal to serve. Clamping to 0 gives the same bound a
                    // PV with no document gets (D6), narrowing results for this one PV; rejecting
                    // instead would fail EVERY query naming it, and because the bound is a max over
                    // the request's PVs, every multi-PV query that happens to include it. That is
                    // the outcome plan D10 already refuses to create at write time, so it should
                    // not be introduced here at read time.
                    logger.warn(
                            "pvStats document pvName: {} has negative maxBucketSpanSeconds: {}; "
                                    + "treating as 0. No writing path can store this -- the document "
                                    + "was likely edited by hand. Queries naming this PV may miss "
                                    + "buckets that start before the query window until it is repaired "
                                    + "with a $max update (see doc/runbooks/schema-migration.md, note on version 5).",
                            document.getPvName(), documentSpanSeconds);
                    spanByPv.put(document.getPvName(), 0L);
                    continue;
                }
                spanByPv.put(document.getPvName(), documentSpanSeconds);
            }
        } catch (RuntimeException ex) {
            // MongoException and codec failures alike: any failure to establish the bound is a
            // query error, never a silent fallback to the unbounded scan (plan D8).
            throw new DpException("pvStats read failed for " + description + ": " + ex.getMessage(), ex);
        }
        logger.debug("read {} pvStats documents for {}", spanByPv.size(), description);
        return spanByPv;
    }

    /**
     * Opens the cursor for a span-class-partitioned bucket query (issue #274, plan D11): one
     * {@code find} per class, built by {@code finder}, merged in {@code (pvName, firstTime)} order
     * by {@link MergedBucketCursor}, and wrapped in {@link TimedMongoCursor} so the merge and the
     * inputs' time are charged to the request's {@code db} stage together. A single class -- every
     * PV the request named shares one span class, the common case -- returns that class's cursor
     * directly, with the same filter the query issued before partitioning existed.
     *
     * <p>Every class cursor is opened here, eagerly, inside the caller's {@code try}: the driver
     * issues the find at {@code cursor()}, so a failure (a missing hinted index, an outage) throws
     * from this method and reaches the caller's catch, which returns the null cursor every
     * dispatcher turns into an error response. Cursors already opened when a later one fails are
     * closed first.
     */
    private MongoCursor<BucketDocument> openSpanClassCursors(
            List<SpanClass> classes, Function<SpanClass, FindIterable<BucketDocument>> finder) {

        if (classes.isEmpty()) {
            throw new IllegalArgumentException("openSpanClassCursors requires at least one span class");
        }
        if (classes.size() == 1) {
            return timedCursor(finder.apply(classes.get(0)));
        }
        final List<MongoCursor<BucketDocument>> cursors = new ArrayList<>(classes.size());
        try {
            for (SpanClass spanClass : classes) {
                cursors.add(finder.apply(spanClass).cursor());
            }
        } catch (RuntimeException ex) {
            for (MongoCursor<BucketDocument> opened : cursors) {
                try {
                    opened.close();
                } catch (RuntimeException closeEx) {
                    ex.addSuppressed(closeEx);
                }
            }
            throw ex;
        }
        return new TimedMongoCursor<>(new MergedBucketCursor(cursors));
    }

    /**
     * Opens the V1 bucket retrieval cursor, reporting a database failure as the null cursor every
     * dispatcher turns into an error response -- the same contract the V2 methods below follow.
     *
     * <p>The catch is not optional. {@code cursor()} issues the find, so a {@code MongoException}
     * lands here and not at iteration: an unhinted query can fail this way for the usual reasons
     * (an outage mid-request), and since #271 a missing {@link MongoClientBase#BUCKET_QUERY_INDEX_KEYS}
     * index fails every call with {@code BadValue} "hint provided does not correspond to an
     * existing index". Uncaught, that escapes {@code QueryDataJob}/{@code QueryTableJob} into
     * {@code QueueHandlerBase}'s worker, which logs it and takes the next job -- so
     * {@code dispatcher.handleResult()} never runs and the caller's response stream stays open
     * until it times out, with no error ever sent. That is the failure mode #271's "fails loudly"
     * trade-off (plan D3) depends on not happening, and it is what the release notes and the SLAC
     * runbook promise operators. {@code MongoSyncQueryClientMissingIndexTest} pins it.
     */
    public MongoCursor<BucketDocument> executeBucketDocumentQuery(
            Bson columnNameFilter,
            long startTimeSeconds,
            long startTimeNanos,
            long endTimeSeconds,
            long endTimeNanos,
            long maxBucketSpanSeconds
    ) {
        try {
            return timedCursor(bucketDocumentQuery(
                    columnNameFilter, startTimeSeconds, startTimeNanos, endTimeSeconds, endTimeNanos,
                    maxBucketSpanSeconds));
        } catch (Exception ex) {
            logger.error("executeBucketDocumentQuery database error: {}", ex.getMessage(), ex);
            return null;
        }
    }

    /**
     * Span-class form of {@link #executeBucketDocumentQuery(Bson, long, long, long, long, long)}
     * for the V1 paths that name their PVs (queryData, the queryTable name-list arm, and the
     * annotation data-block export): one find per class, each with its own bound (issue #274,
     * plan D11), merged in sort order. Same null-cursor contract.
     */
    public MongoCursor<BucketDocument> executeBucketDocumentQuery(
            List<SpanClass> spanClasses,
            long startTimeSeconds,
            long startTimeNanos,
            long endTimeSeconds,
            long endTimeNanos
    ) {
        try {
            return openSpanClassCursors(spanClasses, spanClass -> bucketDocumentQuery(
                    in(BsonConstants.BSON_KEY_PV_NAME, spanClass.pvNames()),
                    startTimeSeconds, startTimeNanos, endTimeSeconds, endTimeNanos,
                    spanClass.maxSpanSeconds()));
        } catch (Exception ex) {
            logger.error("executeBucketDocumentQuery database error: {}", ex.getMessage(), ex);
            return null;
        }
    }

    /**
     * Builds the V1 bucket retrieval query -- name filter, shared overlap predicate with the
     * {@code firstTime} lower bound, and the {@code (pvName, firstTime)} sort -- without opening
     * a cursor. {@link #executeBucketDocumentQuery} is this plus {@code cursor()}; the split
     * exists so that {@code MongoBucketQueryPlanTest} can {@code explain()} the exact query the
     * service issues and pin the planner's index bounds (#232), which no result-level test can
     * observe. Package-private on purpose: production callers go through the cursor method.
     */
    FindIterable<BucketDocument> bucketDocumentQuery(
            Bson columnNameFilter,
            long startTimeSeconds,
            long startTimeNanos,
            long endTimeSeconds,
            long endTimeNanos,
            long maxBucketSpanSeconds
    ) {
        // Bucket overlap predicate (firstTime < end AND lastTime >= begin) built from the shared
        // filter builder so the V1 retrieval path and the V2 $or fragmentation cannot drift. The
        // span is the per-query pvStats maximum resolved by the caller (#232).
        final Bson overlapFilter = MongoQueryFilterBuilder.bucketOverlapsRangeFilter(
                startTimeSeconds, startTimeNanos, endTimeSeconds, endTimeNanos, maxBucketSpanSeconds);
        final Bson filter = and(columnNameFilter, overlapFilter);

        logger.debug("executing query columns: " + columnNameFilter
                + " startSeconds: " + startTimeSeconds
                + " endSeconds: " + endTimeSeconds
                + " maxBucketSpanSeconds: " + maxBucketSpanSeconds);

        return bucketFind(filter);
    }

    /**
     * The one place a bucket retrieval query is opened: {@code find(filter)}, the
     * {@code (pvName, firstTime)} sort, and a {@code hint} pinning the planner to the compound
     * index declared as {@link MongoClientBase#BUCKET_QUERY_INDEX_KEYS} (#271). Every V1 and V2
     * bucket query goes through here so no path can lose the hint or the sort.
     *
     * <p>Why hint: a long-lived archive accumulates {@code pvName}-prefixed indexes the current
     * code never declared (indexes retired in beta-1.6.0 and rel-1.15.0 are never dropped at
     * startup, and operators add their own). Each one widens the planner's candidate set, and the
     * multi-plan trial runs every candidate's span-bounded scan before choosing -- on a customer
     * archive that trial was the query time. Worse, on a recent-window query the planner picked a
     * {@code lastTime}-led index whose plan needs a blocking {@code SORT} stage. With the hint
     * there is one candidate, no trial, and the index's leading {@code (pvName, firstTime)} both
     * carries the #232 lower bound and streams the sort. The trade: if the index is missing the
     * query fails with a driver error rather than degrading to a collection scan, which is the
     * right failure -- startup index creation re-creates it, and a silent full scan on tens of
     * millions of buckets is the four-minute query that started #257.
     * {@code MongoBucketQueryPlanTest} pins the resulting plan shape against an adversarial index set.
     */
    FindIterable<BucketDocument> bucketFind(Bson filter) {
        return mongoCollectionBuckets
                .find(filter)
                .sort(bucketSort())
                .hint(MongoClientBase.BUCKET_QUERY_INDEX_KEYS);
    }

    /**
     * Opens a cursor wrapped in the {@link TimedMongoCursor} decorator that measures the query
     * pipeline's {@code db} stage (issue #212, D3).
     *
     * <p>Every bucket retrieval cursor this client hands to a dispatcher goes through here, for the
     * same reason {@link #bucketFind} exists: a second {@code .cursor()} call added elsewhere would
     * return an unwrapped cursor, and the request it serves would report a {@code db} stage of
     * roughly zero with all of its database time silently folded into {@code process}. That is a
     * plausible-looking wrong number rather than a missing one, so it would survive review.
     *
     * <p>Deliberately separate from {@code bucketFind}, which returns a {@code FindIterable} so
     * that {@code MongoBucketQueryPlanTest} can {@code explain()} the exact query the service
     * issues (#232/#271). Wrapping inside {@code bucketFind} would mean the plan test explained a
     * different object than production opens; wrapping here leaves that split intact.
     *
     * <p>Takes {@code MongoIterable} rather than {@code FindIterable} so an aggregate-backed
     * retrieval path can use it unchanged.
     */
    static <T> MongoCursor<T> timedCursor(MongoIterable<T> iterable) {
        return new TimedMongoCursor<>(iterable.cursor());
    }

    @Override
    public MongoCursor<BucketDocument> executeDataBlockQuery(DataBlockDocument dataBlock) {

        final long startTimeSeconds = dataBlock.getBeginTime().getSeconds();
        final long startTimeNanos = dataBlock.getBeginTime().getNanos();
        final long endTimeSeconds = dataBlock.getEndTime().getSeconds();
        final long endTimeNanos = dataBlock.getEndTime().getNanos();

        // Per-PV firstTime lower-bound spans from pvStats, partitioned into span classes (#232,
        // #274); a failed read is a query error reported through the null cursor (plan D8), never
        // a fallback to the unbounded scan.
        final List<SpanClass> spanClasses;
        try {
            spanClasses = resolveSpanClasses(dataBlock.getPvNames());
        } catch (DpException ex) {
            logger.error("executeDataBlockQuery pvStats read error: {}", ex.getMessage(), ex);
            return null;
        }
        if (spanClasses.isEmpty()) {
            logger.error("executeDataBlockQuery data block names no PVs");
            return null;
        }

        return executeBucketDocumentQuery(
                spanClasses, startTimeSeconds, startTimeNanos, endTimeSeconds, endTimeNanos);
    }

    @Override
    public MongoCursor<BucketDocument> executeQueryData(QueryDataRequest.QuerySpec querySpec) {

        // To inspect the query plan, explain() the FindIterable from bucketDocumentQuery(); the
        // plan-shape test MongoBucketQueryPlanTest does exactly that and pins the index bounds.

        final long startTimeSeconds = querySpec.getBeginTime().getEpochSeconds();
        final long startTimeNanos = querySpec.getBeginTime().getNanoseconds();
        final long endTimeSeconds = querySpec.getEndTime().getEpochSeconds();
        final long endTimeNanos = querySpec.getEndTime().getNanoseconds();

        // Per-PV firstTime lower-bound spans from pvStats, partitioned into span classes (#232,
        // #274); a failed read is a query error reported through the null cursor (plan D8), never
        // a fallback to the unbounded scan.
        final List<SpanClass> spanClasses;
        try {
            spanClasses = resolveSpanClasses(querySpec.getPvNamesList());
        } catch (DpException ex) {
            logger.error("executeQueryData pvStats read error: {}", ex.getMessage(), ex);
            return null;
        }
        if (spanClasses.isEmpty()) {
            // validated upstream; a null here is reported as an error, not an empty result
            logger.error("executeQueryData querySpec names no PVs");
            return null;
        }

        return executeBucketDocumentQuery(
                spanClasses, startTimeSeconds, startTimeNanos, endTimeSeconds, endTimeNanos);
    }

    @Override
    public MongoCursor<BucketDocument> executeQueryTable(QueryTableRequest request) {
        
        final long startTimeSeconds = request.getBeginTime().getEpochSeconds();
        final long startTimeNanos = request.getBeginTime().getNanoseconds();
        final long endTimeSeconds = request.getEndTime().getEpochSeconds();
        final long endTimeNanos = request.getEndTime().getNanoseconds();

        // Resolve the per-query firstTime lower bound from pvStats the same way on both arms
        // (#232): the name-list arm partitions its PVs into span classes, one find per class
        // (#274, plan D11); the pattern arm has no PV list, so it keeps a single find bounded by
        // the maximum span over the pvStats ids matching the same pattern (plan D11 of #232). A
        // failed read is a query error reported through the null cursor (plan D8), never an
        // unbounded scan.
        try {
            switch (request.getPvNameSpecCase()) {
                case PVNAMELIST -> {
                    final List<String> pvNames = request.getPvNameList().getPvNamesList();
                    final List<SpanClass> spanClasses = resolveSpanClasses(pvNames);
                    if (spanClasses.isEmpty()) {
                        logger.error("executeQueryTable pvNameList names no PVs");
                        return null;
                    }
                    return executeBucketDocumentQuery(
                            spanClasses, startTimeSeconds, startTimeNanos, endTimeSeconds, endTimeNanos);
                }
                case PVNAMEPATTERN -> {
                    final Pattern pvNamePattern = Pattern.compile(
                            request.getPvNamePattern().getPattern(), Pattern.CASE_INSENSITIVE);
                    final Bson columnNameFilter = Filters.regex(BsonConstants.BSON_KEY_PV_NAME, pvNamePattern);
                    final long maxBucketSpanSeconds = resolveMaxBucketSpanSeconds(pvNamePattern);
                    return executeBucketDocumentQuery(
                            columnNameFilter, startTimeSeconds, startTimeNanos, endTimeSeconds, endTimeNanos,
                            maxBucketSpanSeconds);
                }
                default -> {
                    // PVNAMESPEC_NOT_SET
                    return null;
                }
            }
        } catch (DpException ex) {
            logger.error("executeQueryTable pvStats read error: {}", ex.getMessage(), ex);
            return null;
        }
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

        // Per-PV firstTime lower-bound spans from pvStats, partitioned into span classes (#232,
        // #274); a failed read is a query error reported through the null cursor (plan D8), never
        // a fallback to the unbounded scan.
        final List<SpanClass> spanClasses;
        try {
            spanClasses = resolveSpanClasses(resolvedQuery.getPvNames());
        } catch (DpException ex) {
            logger.error("executeQueryBucketsV2 pvStats read error: {}", ex.getMessage(), ex);
            return null;
        }

        try {
            // +1 probe row per class to detect a following page; the merged consumer stops after
            // pageSize + 1 documents, so the extra rows of the other classes are never read.
            return openSpanClassCursors(spanClasses, spanClass ->
                    bucketQueryV2(resolvedQuery, spanClass).limit(resolvedQuery.getPageSize() + 1));
        } catch (Exception ex) {
            logger.error("executeQueryBucketsV2 database error: {}", ex.getMessage(), ex);
            return null;
        }
    }

    /**
     * Builds the V2 bucket retrieval query -- base filter plus the keyset seek when the query
     * resumes a page -- without a limit or cursor, so that {@code MongoBucketQueryPlanTest} can
     * {@code explain()} the exact query {@link #executeQueryBucketsV2} issues (the same split
     * {@link #bucketDocumentQuery} makes for V1). Package-private on purpose.
     */
    FindIterable<BucketDocument> bucketQueryV2(ResolvedQuery resolvedQuery, long maxBucketSpanSeconds) {
        return bucketQueryV2(resolvedQuery, new SpanClass(resolvedQuery.getPvNames(), maxBucketSpanSeconds));
    }

    /**
     * The per-span-class form of {@link #bucketQueryV2(ResolvedQuery, long)}: the class's PV list
     * and its own bound (issue #274, plan D11). {@link #executeQueryBucketsV2} issues one of these
     * per class; the plan test explains each.
     */
    FindIterable<BucketDocument> bucketQueryV2(ResolvedQuery resolvedQuery, SpanClass spanClass) {
        // Base filter: PV-name filter AND the $or of per-fragment overlap predicates.
        final List<Bson> andParts = new ArrayList<>();
        andParts.add(bucketBaseFilterV2(resolvedQuery, spanClass));

        // Keyset seek (unary paging) is ANDed at top level, NOT distributed into the $or branches
        // (Q3 correctness note). Absent on the first page and on streaming queries.
        final KeysetPosition pageStart = resolvedQuery.getPageStart();
        if (pageStart != null) {
            andParts.add(bucketKeysetSeekFilter(pageStart));
        }
        return bucketFind(and(andParts));
    }

    @Override
    public MongoCursor<BucketDocument> executeQueryBucketsV2Stream(ResolvedQuery resolvedQuery) {

        if (resolvedQuery == null || resolvedQuery.isEmptyResult()) {
            return null;
        }

        // Per-PV firstTime lower-bound spans from pvStats, partitioned into span classes (#232,
        // #274); a failed read is a query error reported through the null cursor (plan D8), never
        // a fallback to the unbounded scan.
        final List<SpanClass> spanClasses;
        try {
            spanClasses = resolveSpanClasses(resolvedQuery.getPvNames());
        } catch (DpException ex) {
            logger.error("executeQueryBucketsV2Stream pvStats read error: {}", ex.getMessage(), ex);
            return null;
        }

        // Streaming is fire-and-consume: no keyset seek and no limit — the full result of the
        // (resolved intervals × PV list) overlap query is streamed to exhaustion, chunked downstream.
        try {
            return openSpanClassCursors(spanClasses, spanClass -> bucketStreamQueryV2(resolvedQuery, spanClass));
        } catch (Exception ex) {
            logger.error("executeQueryBucketsV2Stream database error: {}", ex.getMessage(), ex);
            return null;
        }
    }

    /**
     * Builds the V2 streaming bucket query for one span class -- the base filter with the class's
     * PV list and bound, no seek, no limit -- without opening a cursor (issue #274, plan D11).
     * Package-private for the plan test.
     */
    FindIterable<BucketDocument> bucketStreamQueryV2(ResolvedQuery resolvedQuery, SpanClass spanClass) {
        return bucketFind(bucketBaseFilterV2(resolvedQuery, spanClass));
    }

    @Override
    public MongoCursor<BucketDocument> executeQuerySamplesV2(
            ResolvedQuery resolvedQuery,
            long windowBeginSecs, long windowBeginNanos,
            long windowEndSecs, long windowEndNanos) {

        if (resolvedQuery == null || resolvedQuery.isEmptyResult()) {
            return null;
        }

        // Each fragment is intersected with the slice window [windowBegin, windowEnd) (#274): the
        // begin is the resume timestamp on a continuation page or the previous slice's end, the
        // end is the slice end. The clamp lives on TimeInterval so this filter and the
        // dispatcher's sample-level retention trim are derived from the same interval set (#207)
        // — see clampToWindow.
        final List<TimeInterval> clampedIntervals = TimeInterval.clampToWindow(
                resolvedQuery.getRetrievalIntervals(),
                windowBeginSecs, windowBeginNanos, windowEndSecs, windowEndNanos);
        if (clampedIntervals.isEmpty()) {
            // nothing overlaps the slice. The samples dispatchers screen this same condition, from
            // the same clamp, before calling — so a null they receive is one of the failures below.
            return null;
        }

        // Per-PV firstTime lower-bound spans from pvStats, partitioned into span classes (#232,
        // #274); a failed read is a query error reported through the null cursor (plan D8), never
        // a fallback to the unbounded scan.
        final List<SpanClass> spanClasses;
        try {
            spanClasses = resolveSpanClasses(resolvedQuery.getPvNames());
        } catch (DpException ex) {
            logger.error("executeQuerySamplesV2 pvStats read error: {}", ex.getMessage(), ex);
            return null;
        }

        try {
            return openSpanClassCursors(spanClasses, spanClass ->
                    bucketSamplesQueryV2(resolvedQuery, clampedIntervals, spanClass));
        } catch (Exception ex) {
            logger.error("executeQuerySamplesV2 database error: {}", ex.getMessage(), ex);
            return null;
        }
    }

    /**
     * Builds the V2 samples retrieval query over {@code clampedIntervals} (the output of
     * {@code TimeInterval.clampToWindow}, non-empty) without opening a cursor, so that
     * {@code MongoBucketQueryPlanTest} can {@code explain()} the exact fragment {@code $or}
     * {@link #executeQuerySamplesV2} issues. Package-private on purpose.
     */
    FindIterable<BucketDocument> bucketSamplesQueryV2(
            ResolvedQuery resolvedQuery, List<TimeInterval> clampedIntervals, long maxBucketSpanSeconds) {
        return bucketSamplesQueryV2(
                resolvedQuery, clampedIntervals, new SpanClass(resolvedQuery.getPvNames(), maxBucketSpanSeconds));
    }

    /**
     * The per-span-class form of {@link #bucketSamplesQueryV2(ResolvedQuery, List, long)} (issue
     * #274, plan D11): the class's PV list and its own bound over the same clamped fragments.
     */
    FindIterable<BucketDocument> bucketSamplesQueryV2(
            ResolvedQuery resolvedQuery, List<TimeInterval> clampedIntervals, SpanClass spanClass) {

        final Bson pvNameFilter = in(BsonConstants.BSON_KEY_PV_NAME, spanClass.pvNames());
        return bucketFind(and(pvNameFilter, fragmentsOverlapFilter(clampedIntervals, spanClass.maxSpanSeconds())));
    }

    /**
     * The overlap predicate for a set of retrieval fragments: the {@code $or} of one
     * {@link MongoQueryFilterBuilder#bucketOverlapsRangeFilter} per fragment, ANDed with the
     * {@code firstTime.seconds} index bounds hoisted above the {@code $or} over the fragments'
     * earliest begin and latest end (#271). Each branch already carries its own bounds, but a
     * predicate inside an {@code $or} branch is not an index bound for the planner's single-scan
     * plan: measured on {@code MongoBucketQueryPlanTest}'s fixture, the two-fragment {@code $or}
     * alone won with {@code firstTime.seconds: [MinKey, MaxKey]} -- each PV's whole history --
     * with or without the index hint. The hoisted pair is implied by the branches (every matching
     * bucket satisfies some branch, hence the extremes), so it changes no result; it gives the
     * single-scan plan the interval {@code [minBegin - span, maxEnd]} instead. The cost between
     * fragments is scanned and filtered out, which is #203's remaining multiplier. A single
     * fragment needs no hoist: its own bounds are already top-level.
     *
     * <p>Rejects an empty interval list rather than building a filter for it. Both callers already
     * screen the case -- {@code ResolvedQuery.isEmptyResult()} for the base filter, and
     * {@code clampedIntervals.isEmpty()} on the samples path -- so arriving here with no intervals
     * is a caller bug, and both ways of "handling" it fail in the silent direction: an empty
     * {@code $or} is a driver error at query time, while the hoisted bounds would be built from
     * the {@code Long.MAX_VALUE}/{@code MIN_VALUE} loop sentinels, giving an impossible interval
     * that matches nothing and reads as an ordinary empty result. Fail at the call instead.
     */
    private static Bson fragmentsOverlapFilter(List<TimeInterval> intervals, long maxBucketSpanSeconds) {

        if (intervals == null || intervals.isEmpty()) {
            throw new IllegalArgumentException(
                    "fragmentsOverlapFilter requires at least one retrieval interval");
        }

        final List<Bson> fragmentFilters = new ArrayList<>();
        long minBeginSeconds = Long.MAX_VALUE;
        long maxEndSeconds = Long.MIN_VALUE;
        for (TimeInterval interval : intervals) {
            fragmentFilters.add(MongoQueryFilterBuilder.bucketOverlapsRangeFilter(
                    interval.getBeginSeconds(), interval.getBeginNanos(),
                    interval.getEndSeconds(), interval.getEndNanos(),
                    maxBucketSpanSeconds));
            minBeginSeconds = Math.min(minBeginSeconds, interval.getBeginSeconds());
            maxEndSeconds = Math.max(maxEndSeconds, interval.getEndSeconds());
        }
        if (fragmentFilters.size() == 1) {
            return fragmentFilters.get(0);
        }
        final List<Bson> parts = new ArrayList<>(MongoQueryFilterBuilder.bucketFirstTimeSecondsIndexBounds(
                minBeginSeconds, maxEndSeconds, maxBucketSpanSeconds));
        parts.add(or(fragmentFilters));
        return and(parts);
    }

    /**
     * Saturating epoch-nanos conversion for filter bounds: an out-of-range instant clamps to the
     * long extreme instead of overflowing, which is correct for a comparison bound.
     */
    private static long saturatedEpochNanos(long seconds, long nanos) {
        try {
            return Math.addExact(Math.multiplyExact(seconds, 1_000_000_000L), nanos);
        } catch (ArithmeticException ex) {
            return seconds < 0 ? Long.MIN_VALUE : Long.MAX_VALUE;
        }
    }

    @Override
    public Map<String, Set<Long>> resolveSampleStatusTimestamps(
            ResolvedQuery resolvedQuery,
            long windowBeginSecs, long windowBeginNanos,
            long windowEndSecs, long windowEndNanos) throws DpException {

        final ResolvedStatusFilter statusFilter = resolvedQuery.getStatusFilter();
        if (statusFilter == null) {
            return Map.of();
        }

        // Bound the fetch by the same clamped slice the sample retrieval uses (#207, #274): a
        // status can only affect samples inside some clamped fragment. The bounds here are the
        // slice's extremes [first fragment begin, last fragment end) — statuses in the gaps between
        // fragments are harmless to include (their samples are dropped by the fragment retention
        // test regardless of mode), so per-fragment precision is not required for correctness.
        final List<TimeInterval> clampedFragments = TimeInterval.clampToWindow(
                resolvedQuery.getRetrievalIntervals(),
                windowBeginSecs, windowBeginNanos, windowEndSecs, windowEndNanos);
        if (clampedFragments.isEmpty()) {
            return Map.of();
        }
        final TimeInterval firstFragment = clampedFragments.get(0);
        final TimeInterval lastFragment = clampedFragments.get(clampedFragments.size() - 1);
        final long windowBeginTotalNanos =
                saturatedEpochNanos(firstFragment.getBeginSeconds(), firstFragment.getBeginNanos());
        final long windowEndTotalNanos =
                saturatedEpochNanos(lastFragment.getEndSeconds(), lastFragment.getEndNanos());

        // Span-overlap predicate on the epoch-nanos scalars. Note: status documents have no
        // maximum span (sparse labeling over an arbitrarily wide range is first-class), so no
        // #197-style firstTime lower bound may ever be added here.
        final List<Bson> filters = new ArrayList<>();
        filters.add(in(BsonConstants.BSON_KEY_SAMPLE_STATUS_PV_NAME, resolvedQuery.getPvNames()));
        filters.add(eq(BsonConstants.BSON_KEY_SAMPLE_STATUS_DOMAIN, statusFilter.domain()));
        if (!statusFilter.layers().isEmpty()) {
            filters.add(in(BsonConstants.BSON_KEY_SAMPLE_STATUS_LAYER, statusFilter.layers()));
        }
        filters.add(lt(BsonConstants.BSON_KEY_SAMPLE_STATUS_FIRST_TIME_NANOS, windowEndTotalNanos));
        filters.add(gte(BsonConstants.BSON_KEY_SAMPLE_STATUS_LAST_TIME_NANOS, windowBeginTotalNanos));

        final Map<String, Set<Long>> matchingTimestampsByPv = new HashMap<>();
        try (MongoCursor<SampleStatusBucketDocument> cursor =
                     mongoCollectionSampleStatusBuckets.find(and(filters)).cursor()) {
            while (cursor.hasNext()) {
                final SampleStatusBucketDocument document = cursor.next();
                for (SampleStatusDocumentUtility.StatusPoint point :
                        SampleStatusDocumentUtility.expandDocument(document)) {
                    // keep only in-window matching timestamps: memory stays bounded by the number
                    // of labeled samples in the window
                    if (point.timestampNanos() < windowBeginTotalNanos
                            || point.timestampNanos() >= windowEndTotalNanos) {
                        continue;
                    }
                    if (statusFilter.matchesCode(point.statusCode())) {
                        matchingTimestampsByPv
                                .computeIfAbsent(document.getPvName(), k -> new HashSet<>())
                                .add(point.timestampNanos());
                    }
                }
            }
        } catch (DpException ex) {
            // malformed stored document: must surface as a reportable error, never be read as
            // "no statuses" (in EXCLUDE mode that would silently return filtered-out samples)
            throw ex;
        } catch (Exception ex) {
            logger.error("resolveSampleStatusTimestamps database error: {}", ex.getMessage(), ex);
            return null;
        }

        return matchingTimestampsByPv;
    }

    /**
     * Base V2 bucket filter shared by the unary and streaming retrieval paths: the resolved PV-name
     * {@code in(...)} filter AND the fragments' overlap predicate from
     * {@link #fragmentsOverlapFilter} (single fragment → no {@code $or} wrapper). Built from the
     * shared filter builder so V1 and V2 overlap semantics cannot drift. The PV list and the
     * {@code firstTime} lower-bound span come from one {@link SpanClass} of the request's
     * partition (#232, #274 plan D11), applied to every fragment's bound and to the hoisted one.
     */
    private static Bson bucketBaseFilterV2(ResolvedQuery resolvedQuery, SpanClass spanClass) {
        final Bson pvNameFilter = in(BsonConstants.BSON_KEY_PV_NAME, spanClass.pvNames());
        return and(pvNameFilter,
                fragmentsOverlapFilter(resolvedQuery.getRetrievalIntervals(), spanClass.maxSpanSeconds()));
    }

    /**
     * Compound bucket sort {@code (pvName, firstTimeSecs, firstTimeNanos)}, shared by V1 and V2: a
     * prefix of {@link MongoClientBase#BUCKET_QUERY_INDEX_KEYS}, so the hinted index streams it
     * with no SORT stage.
     */
    private static Bson bucketSort() {
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
