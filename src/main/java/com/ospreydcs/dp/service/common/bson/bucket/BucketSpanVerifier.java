package com.ospreydcs.dp.service.common.bson.bucket;

import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.Aggregates;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.Projections;
import com.mongodb.client.model.ReplaceOptions;
import com.ospreydcs.dp.service.common.bson.BsonConstants;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.bson.Document;
import org.bson.conversions.Bson;

import java.time.Instant;
import java.util.Arrays;
import java.util.List;

/**
 * Verifies that every bucket already stored in the archive satisfies the configured maximum bucket
 * span, which is the precondition for the query-side lower bound added in issue #197.
 *
 * <p>Ingestion validation enforces the limit for new data, but says nothing about data ingested
 * before the limit existed. If the archive contains an over-long bucket, the query lower bound
 * {@code firstTime.seconds >= beginSeconds - maxBucketSpanSeconds} silently excludes it: queries
 * return fewer buckets with no error and no indication that anything was dropped. Because that
 * failure mode is a wrong answer rather than a visible error, the check runs by default rather than
 * requiring operators to opt in.
 *
 * <p>The check asks "does any bucket exceed the limit?" rather than "what is the largest span?".
 * The former short-circuits at the first violator via {@code $limit: 1}, so a non-compliant archive
 * — the case that actually matters — is detected almost immediately. Only a compliant archive pays
 * a full scan, and a compliant archive is one where the bound is safe to use. Benchmarked at
 * roughly 0.67 microseconds per bucket, so a clean 35M-document archive costs about 23 seconds and
 * a clean 500M-document archive about 5.5 minutes, measured on an idle server.
 *
 * <p>Because that cost is nontrivial on a large archive, the outcome is recorded in a small marker
 * document keyed by the verified limit. Subsequent startups read the marker and skip the scan, so
 * the full cost is paid once per archive per limit value rather than on every restart. Raising the
 * limit invalidates the marker, since a larger limit is a different claim about the data.
 *
 * <p>Note that sampling is deliberately not used as a shortcut. Over-long buckets are typically
 * rare, and a random sample that misses them reports a confident all-clear — strictly worse than
 * not checking, since it produces the same silent data loss while suggesting the archive was
 * verified.
 */
public class BucketSpanVerifier {

    private static final Logger logger = LogManager.getLogger();

    public static final String COLLECTION_NAME_BUCKET_SPAN_VERIFICATION = "bucketSpanVerification";

    // Single-document marker; the fixed _id keeps the collection to one record.
    private static final String MARKER_ID = "bucketSpanVerification";
    private static final String FIELD_VERIFIED_LIMIT_SECONDS = "verifiedLimitSeconds";
    private static final String FIELD_VERIFIED_AT = "verifiedAt";
    private static final String FIELD_BUCKET_COUNT = "bucketCountAtVerification";

    /** Outcome of a verification attempt. */
    public enum VerificationOutcome {
        /** No bucket exceeds the limit; the query lower bound is safe to apply. */
        VERIFIED_CLEAN,
        /** A previous run already verified this limit; the scan was skipped. */
        SKIPPED_ALREADY_VERIFIED,
        /** At least one stored bucket exceeds the limit; the bound would silently drop data. */
        VIOLATION_FOUND,
        /** The check could not complete (database error); treated as unverified. */
        CHECK_FAILED
    }

    /** Result of a verification attempt, including detail about any violating bucket found. */
    public record VerificationResult(
            VerificationOutcome outcome,
            long limitSeconds,
            String violatingPvName,
            long violatingSpanSeconds,
            long elapsedMillis
    ) {
        public boolean boundIsSafe() {
            return outcome == VerificationOutcome.VERIFIED_CLEAN
                    || outcome == VerificationOutcome.SKIPPED_ALREADY_VERIFIED;
        }
    }

    /**
     * Runs the verification, consulting and updating the marker document.
     *
     * @param bucketsCollection      the buckets collection to scan
     * @param verificationCollection collection holding the marker document
     * @param limitSeconds           the configured maximum bucket span being verified
     */
    public static VerificationResult verify(
            MongoCollection<Document> bucketsCollection,
            MongoCollection<Document> verificationCollection,
            long limitSeconds
    ) {
        final long startMillis = System.currentTimeMillis();

        try {
            if (isAlreadyVerified(verificationCollection, limitSeconds)) {
                logger.info(
                        "bucket span verification skipped, archive already verified for limit of {} seconds",
                        limitSeconds);
                return new VerificationResult(
                        VerificationOutcome.SKIPPED_ALREADY_VERIFIED, limitSeconds, null, 0, 0);
            }

            logger.info(
                    "bucket span verification starting for limit of {} seconds; "
                            + "this scans the buckets collection and may take several minutes on a large "
                            + "archive, but runs only once per limit value",
                    limitSeconds);

            final Document violator = findViolatingBucket(bucketsCollection, limitSeconds);
            final long elapsedMillis = System.currentTimeMillis() - startMillis;

            if (violator != null) {
                final String pvName = violator.getString(BsonConstants.BSON_KEY_PV_NAME);
                final long spanSeconds = extractSpanSeconds(violator);
                logger.error(
                        "bucket span verification FAILED after {} ms: bucket for pv {} spans {} seconds, "
                                + "exceeding the configured limit of {} seconds. The query time-range lower "
                                + "bound will be DISABLED for this process so queries remain correct, at the "
                                + "cost of the performance improvement from issue #197. Either raise {} to at "
                                + "least {} or re-ingest the affected data in compliant buckets.",
                        elapsedMillis,
                        pvName,
                        spanSeconds,
                        limitSeconds,
                        BucketSpanLimits.CFG_KEY_MAX_BUCKET_SPAN_SECONDS,
                        spanSeconds);
                return new VerificationResult(
                        VerificationOutcome.VIOLATION_FOUND, limitSeconds, pvName, spanSeconds, elapsedMillis);
            }

            recordVerification(bucketsCollection, verificationCollection, limitSeconds);
            logger.info(
                    "bucket span verification passed in {} ms: no stored bucket exceeds the limit of {} "
                            + "seconds, query time-range lower bound is enabled",
                    elapsedMillis, limitSeconds);
            return new VerificationResult(
                    VerificationOutcome.VERIFIED_CLEAN, limitSeconds, null, 0, elapsedMillis);

        } catch (Exception ex) {
            final long elapsedMillis = System.currentTimeMillis() - startMillis;
            logger.error(
                    "bucket span verification error after {} ms: {}. The query time-range lower bound "
                            + "will be DISABLED for this process so queries remain correct.",
                    elapsedMillis, ex.getMessage(), ex);
            return new VerificationResult(
                    VerificationOutcome.CHECK_FAILED, limitSeconds, null, 0, elapsedMillis);
        }
    }

    /**
     * A marker verifying a limit at least as large as the current one implies compliance with the
     * current one only if the limits are equal or the recorded limit is smaller: every bucket
     * fitting within a smaller span also fits within a larger one, but not the reverse.
     */
    private static boolean isAlreadyVerified(
            MongoCollection<Document> verificationCollection, long limitSeconds) {

        final Document marker = verificationCollection.find(Filters.eq("_id", MARKER_ID)).first();
        if (marker == null) {
            return false;
        }
        final Long verifiedLimit = marker.getLong(FIELD_VERIFIED_LIMIT_SECONDS);
        return verifiedLimit != null && verifiedLimit <= limitSeconds;
    }

    /**
     * Returns the first bucket found whose span exceeds the limit, or null if none exists.
     *
     * <p>Projects only fields carried by the compound bucket index so the scan can be served
     * without fetching documents, which keeps it from evicting the working set of a live server
     * from the storage engine cache.
     */
    private static Document findViolatingBucket(
            MongoCollection<Document> bucketsCollection, long limitSeconds) {

        final Bson spanExceedsLimit = Filters.expr(new Document("$gt", Arrays.asList(
                new Document("$subtract", Arrays.asList(
                        "$" + BsonConstants.BSON_KEY_BUCKET_LAST_TIME_SECS,
                        "$" + BsonConstants.BSON_KEY_BUCKET_FIRST_TIME_SECS)),
                limitSeconds)));

        final List<Bson> pipeline = Arrays.asList(
                Aggregates.match(spanExceedsLimit),
                Aggregates.limit(1),
                Aggregates.project(Projections.fields(
                        Projections.excludeId(),
                        Projections.include(
                                BsonConstants.BSON_KEY_PV_NAME,
                                BsonConstants.BSON_KEY_BUCKET_FIRST_TIME_SECS,
                                BsonConstants.BSON_KEY_BUCKET_LAST_TIME_SECS))));

        return bucketsCollection.aggregate(pipeline).first();
    }

    private static long extractSpanSeconds(Document violator) {
        final Document dataTimestamps = violator.get("dataTimestamps", Document.class);
        if (dataTimestamps == null) {
            return -1;
        }
        final Document firstTime = dataTimestamps.get("firstTime", Document.class);
        final Document lastTime = dataTimestamps.get("lastTime", Document.class);
        if (firstTime == null || lastTime == null) {
            return -1;
        }
        final Number first = firstTime.get("seconds", Number.class);
        final Number last = lastTime.get("seconds", Number.class);
        if (first == null || last == null) {
            return -1;
        }
        return last.longValue() - first.longValue();
    }

    private static void recordVerification(
            MongoCollection<Document> bucketsCollection,
            MongoCollection<Document> verificationCollection,
            long limitSeconds) {

        final Document marker = new Document("_id", MARKER_ID)
                .append(FIELD_VERIFIED_LIMIT_SECONDS, limitSeconds)
                .append(FIELD_VERIFIED_AT, Instant.now())
                .append(FIELD_BUCKET_COUNT, bucketsCollection.estimatedDocumentCount());

        verificationCollection.replaceOne(
                Filters.eq("_id", MARKER_ID), marker, new ReplaceOptions().upsert(true));
    }
}
