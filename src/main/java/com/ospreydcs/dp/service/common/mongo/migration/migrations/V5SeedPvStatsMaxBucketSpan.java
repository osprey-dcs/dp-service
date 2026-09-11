package com.ospreydcs.dp.service.common.mongo.migration.migrations;

import com.mongodb.MongoException;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Accumulators;
import com.mongodb.client.model.Aggregates;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.MergeOptions;
import com.ospreydcs.dp.service.common.exception.DpException;
import com.ospreydcs.dp.service.common.mongo.MongoClientBase;
import com.ospreydcs.dp.service.common.mongo.migration.Migration;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.bson.Document;
import org.bson.conversions.Bson;

import java.util.List;

/**
 * Seeds the per-PV bucket span statistics in {@code pvStats} from the existing {@code buckets}
 * archive, then drops the {@code bucketSpanVerification} marker collection (#232, plan D10).
 *
 * <p>#232 replaced the startup full-collection scan for the longest stored bucket with a per-PV
 * {@code maxBucketSpanSeconds} that ingestion records as it writes buckets, and the query side now
 * derives the {@code firstTime} lower bound of every bucket overlap filter from that value. A PV
 * with no {@code pvStats} document is treated as having a span of zero (D6), so without this seed
 * every bucket ingested before the upgrade would be invisible to any query whose window begins
 * after the bucket's first second — until the PV happened to ingest a bucket at least as long,
 * which for a retired or slowly sampled PV is never.
 *
 * <p>The seed is one server-side pipeline over {@code buckets}: {@code $group} by {@code pvName}
 * taking the {@code $max} of {@code lastTime.seconds - firstTime.seconds} (the whole-seconds
 * measure ingestion records, D3), then {@code $merge} into {@code pvStats} keyed on {@code _id},
 * inserting a missing document and otherwise keeping the larger of the stored and computed values.
 * Between the two, a {@code $match} drops groups whose maximum is null or negative: a PV whose
 * every bucket lacks {@code dataTimestamps} yields null, and one whose every bucket has
 * {@code lastTime} before {@code firstTime} yields a negative. Neither may be stored — the query
 * side decodes the field into a primitive {@code long}, and the filter builder rejects a negative
 * span — so such a PV is left without a document (span zero, D6) rather than seeded with a value
 * that would turn every query naming it into an error. A PV with at least one well-formed bucket is
 * unaffected: {@code $max} ignores null, and a non-negative span dominates a negative one.
 *
 * <p>The marker collection is then dropped. Nothing reads or writes it after #232; its name survives
 * only as {@link MongoClientBase#COLLECTION_NAME_BUCKET_SPAN_VERIFICATION_LEGACY} so the runner's
 * emptiness probe still classifies a pre-v5 database holding only that marker as legacy.
 *
 * <p>On a large archive this is a one-time full scan of {@code buckets}, the same order of work as
 * v4. Group state is per PV, so memory is bounded by the number of distinct PVs, not buckets.
 * Waiting service processes time out after five minutes with the held-claim message and must simply
 * be restarted; see the operator guidance in {@code doc/schema-migration.md}.
 *
 * <p><b>Idempotency.</b> A re-run recomputes the same per-PV maxima, and the {@code $max} on match
 * leaves an equal or larger stored value untouched, so applying the migration twice yields the same
 * {@code pvStats} content as applying it once. For the same reason the seed commutes with
 * ingestion's own {@code $max} upsert ({@code PvStatsMaxSpanUpdater}): whichever side observed the
 * longer bucket wins and neither lowers the other's value, so a re-run after ingestion has begun
 * writing {@code pvStats} — a crash between applying and recording the version, or the stuck-claim
 * recovery — cannot regress a value. Dropping a collection that does not exist is a no-op, so the
 * drop step is idempotent too.
 *
 * <p>The {@code $merge} form used here — a {@code whenMatched} pipeline referencing {@code $$new} —
 * was verified against a real mongo:8.0 server before this class was written, per the #254 lesson.
 */
public class V5SeedPvStatsMaxBucketSpan implements Migration {

    private static final Logger logger = LogManager.getLogger();

    // Stored field names as of schema version 5. Deliberately local rather than the shared
    // BsonConstants keys: a migration reads the shape the data had when it was written, and must
    // keep doing so after the constants move on.
    static final String FIELD_ID = "_id";
    static final String FIELD_PV_NAME = "pvName";
    static final String FIELD_FIRST_TIME_SECONDS = "dataTimestamps.firstTime.seconds";
    static final String FIELD_LAST_TIME_SECONDS = "dataTimestamps.lastTime.seconds";
    static final String FIELD_MAX_BUCKET_SPAN_SECONDS = "maxBucketSpanSeconds";

    @Override
    public int version() {
        return 5;
    }

    @Override
    public String description() {
        return "seed pvStats maxBucketSpanSeconds from buckets; drop bucketSpanVerification";
    }

    @Override
    public void apply(MongoDatabase database) throws DpException {

        try {
            final MongoCollection<Document> buckets =
                    database.getCollection(MongoClientBase.COLLECTION_NAME_BUCKETS);
            final MongoCollection<Document> pvStats =
                    database.getCollection(MongoClientBase.COLLECTION_NAME_PV_STATS);

            // toCollection() is the driver's execution entry point for a pipeline ending in $merge;
            // the aggregate returns no documents to iterate.
            buckets.aggregate(seedPipeline()).allowDiskUse(true).toCollection();
            logger.info(
                    "V5SeedPvStatsMaxBucketSpan: seeded {} from {}; {} now holds {} document(s)",
                    MongoClientBase.COLLECTION_NAME_PV_STATS,
                    MongoClientBase.COLLECTION_NAME_BUCKETS,
                    MongoClientBase.COLLECTION_NAME_PV_STATS,
                    pvStats.countDocuments());

            database.getCollection(MongoClientBase.COLLECTION_NAME_BUCKET_SPAN_VERIFICATION_LEGACY).drop();
            logger.info(
                    "V5SeedPvStatsMaxBucketSpan: dropped legacy collection {}",
                    MongoClientBase.COLLECTION_NAME_BUCKET_SPAN_VERIFICATION_LEGACY);

        } catch (MongoException ex) {
            logger.error(
                    "V5SeedPvStatsMaxBucketSpan: mongo exception seeding pvStats: {}",
                    ex.getMessage(), ex);
            throw new DpException("error seeding pvStats bucket span statistics: " + ex.getMessage(), ex);
        }
    }

    /**
     * The D10 pipeline. Package-private so the test can pin its shape independently of a server run.
     */
    static List<Bson> seedPipeline() {

        // {$group: {_id: "$pvName", maxBucketSpanSeconds: {$max: {$subtract: [last, first]}}}}
        final Document spanExpression = new Document("$subtract", List.of(
                "$" + FIELD_LAST_TIME_SECONDS, "$" + FIELD_FIRST_TIME_SECONDS));
        final Bson groupByPv = Aggregates.group(
                "$" + FIELD_PV_NAME, Accumulators.max(FIELD_MAX_BUCKET_SPAN_SECONDS, spanExpression));

        // {$match: {maxBucketSpanSeconds: {$gte: 0}}} — drops null (no timestamps on any bucket)
        // and negative (lastTime before firstTime on every bucket) maxima; see the class Javadoc.
        final Bson dropUnusable = Aggregates.match(Filters.gte(FIELD_MAX_BUCKET_SPAN_SECONDS, 0L));

        // {$merge: {into: "pvStats", on: "_id",
        //           whenMatched: [{$set: {maxBucketSpanSeconds: {$max: [stored, $$new]}}}],
        //           whenNotMatched: "insert"}}
        final Document keepLargerOnMatch = new Document("$set", new Document(
                FIELD_MAX_BUCKET_SPAN_SECONDS, new Document("$max", List.of(
                        "$" + FIELD_MAX_BUCKET_SPAN_SECONDS,
                        "$$new." + FIELD_MAX_BUCKET_SPAN_SECONDS))));
        final Bson mergeIntoPvStats = Aggregates.merge(
                MongoClientBase.COLLECTION_NAME_PV_STATS,
                new MergeOptions()
                        .uniqueIdentifier(FIELD_ID)
                        .whenMatched(MergeOptions.WhenMatched.PIPELINE)
                        .whenMatchedPipeline(List.of(keepLargerOnMatch))
                        .whenNotMatched(MergeOptions.WhenNotMatched.INSERT));

        return List.of(groupByPv, dropUnusable, mergeIntoPvStats);
    }
}
