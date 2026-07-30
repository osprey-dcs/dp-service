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

    // Bucket fields that BucketDocument deserialization dereferences; absence makes a bucket
    // unservable, so the scan flags it.
    private static final String FIELD_DATA_COLUMN = "dataColumn";
    private static final String FIELD_DATA_TIMESTAMPS = "dataTimestamps";

    // Computed projection fields naming which required field a scan hit is missing.
    private static final String PROJECTED_HAS_DATA_COLUMN = "hasDataColumn";
    private static final String PROJECTED_HAS_DATA_TIMESTAMPS = "hasDataTimestamps";

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

    /**
     * A stored bucket missing a field that deserialization requires. Such a bucket cannot be
     * returned to a client: the query path rejects it with an error rather than serving a partial
     * result, so it blocks every query whose time range covers it until it is repaired or removed.
     *
     * @param bucketId     document id, for locating the bucket
     * @param pvName       PV the bucket belongs to (may be null if that field is missing too)
     * @param missingField which required field is absent
     */
    public record CorruptBucket(String bucketId, String pvName, String missingField) {
    }

    /**
     * Result of a verification attempt, including detail about any violating bucket found.
     *
     * <p>{@code corruptBucket} is reported independently of {@code outcome}: a malformed bucket does
     * not make the span bound unsafe, so it is surfaced for repair without disabling the query
     * optimization. The two problems are unrelated and are treated as such.
     */
    public record VerificationResult(
            VerificationOutcome outcome,
            long limitSeconds,
            String violatingPvName,
            long violatingSpanSeconds,
            CorruptBucket corruptBucket,
            long elapsedMillis
    ) {
        public boolean boundIsSafe() {
            return outcome == VerificationOutcome.VERIFIED_CLEAN
                    || outcome == VerificationOutcome.SKIPPED_ALREADY_VERIFIED;
        }

        public boolean foundCorruptBucket() {
            return corruptBucket != null;
        }
    }

    /**
     * Runs the verification, consulting and updating the marker document.
     *
     * <p>Checks two independent archive invariants in a single pass, since both require visiting
     * stored buckets and the marginal cost of the second predicate is small (measured at roughly
     * 20% over the span check alone):
     * <ul>
     *   <li>no bucket exceeds the configured span limit, the precondition for the query lower bound
     *   <li>no bucket is missing a field that deserialization requires, which would make every query
     *       covering it fail
     * </ul>
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
                        "bucket archive verification skipped, already verified for limit of {} seconds",
                        limitSeconds);
                return new VerificationResult(
                        VerificationOutcome.SKIPPED_ALREADY_VERIFIED, limitSeconds, null, 0, null, 0);
            }

            logger.info(
                    "bucket archive verification starting for span limit of {} seconds; this scans the "
                            + "buckets collection for over-long and malformed buckets and may take several "
                            + "minutes on a large archive, but runs only once per limit value",
                    limitSeconds);

            final Document offender = findOffendingBucket(bucketsCollection, limitSeconds);
            final long elapsedMillis = System.currentTimeMillis() - startMillis;

            if (offender != null) {
                final CorruptBucket corruptBucket = asCorruptBucket(offender);

                if (corruptBucket != null) {
                    // Malformed bucket: does not affect the span bound, but blocks any query
                    // covering it, so report it for repair and leave the bound enabled.
                    //
                    // Deliberately does not record the verification marker. The scan stopped at
                    // this bucket without establishing the span invariant for the rest of the
                    // archive, and re-running on the next startup keeps an unrepaired bucket
                    // visible rather than reporting it once and going quiet.
                    logger.error(
                            "bucket archive verification found a MALFORMED bucket after {} ms: id {} "
                                    + "for pv {} is missing {}. Queries whose time range covers this bucket "
                                    + "will fail with a deserialization error until it is repaired or "
                                    + "removed. The query time-range lower bound remains enabled, since a "
                                    + "malformed bucket does not affect the span invariant.",
                            elapsedMillis,
                            corruptBucket.bucketId(),
                            corruptBucket.pvName(),
                            corruptBucket.missingField());
                    return new VerificationResult(
                            VerificationOutcome.VERIFIED_CLEAN, limitSeconds, null, 0,
                            corruptBucket, elapsedMillis);
                }

                final String pvName = offender.getString(BsonConstants.BSON_KEY_PV_NAME);
                final long spanSeconds = extractSpanSeconds(offender);
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
                        VerificationOutcome.VIOLATION_FOUND, limitSeconds, pvName, spanSeconds,
                        null, elapsedMillis);
            }

            recordVerification(bucketsCollection, verificationCollection, limitSeconds);
            logger.info(
                    "bucket archive verification passed in {} ms: no stored bucket exceeds the limit of "
                            + "{} seconds or is missing required fields, query time-range lower bound is "
                            + "enabled",
                    elapsedMillis, limitSeconds);
            return new VerificationResult(
                    VerificationOutcome.VERIFIED_CLEAN, limitSeconds, null, 0, null, elapsedMillis);

        } catch (Exception ex) {
            final long elapsedMillis = System.currentTimeMillis() - startMillis;
            logger.error(
                    "bucket archive verification error after {} ms: {}. The query time-range lower bound "
                            + "will be DISABLED for this process so queries remain correct.",
                    elapsedMillis, ex.getMessage(), ex);
            return new VerificationResult(
                    VerificationOutcome.CHECK_FAILED, limitSeconds, null, 0, null, elapsedMillis);
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
     * Returns the first bucket found that either exceeds the span limit or is missing a field
     * required for deserialization, or null if the archive satisfies both invariants.
     *
     * <p>Both predicates are combined in one {@code $or} so the collection is visited once. The
     * scan short-circuits at the first offender via {@code $limit}, so a non-compliant archive is
     * detected without reading the remainder; only a clean archive pays a full pass.
     *
     * <p>The projection reports which required field is absent, so the caller can name it in the
     * error an operator will act on.
     */
    private static Document findOffendingBucket(
            MongoCollection<Document> bucketsCollection, long limitSeconds) {

        final Bson spanExceedsLimit = Filters.expr(new Document("$gt", Arrays.asList(
                new Document("$subtract", Arrays.asList(
                        "$" + BsonConstants.BSON_KEY_BUCKET_LAST_TIME_SECS,
                        "$" + BsonConstants.BSON_KEY_BUCKET_FIRST_TIME_SECS)),
                limitSeconds)));

        // A bucket missing either field cannot be deserialized: BucketDocument rejects it and the
        // query fails rather than returning a partial result.
        final Bson missingRequiredField = Filters.or(
                Filters.exists(FIELD_DATA_COLUMN, false),
                Filters.exists(FIELD_DATA_TIMESTAMPS, false));

        final List<Bson> pipeline = Arrays.asList(
                Aggregates.match(Filters.or(spanExceedsLimit, missingRequiredField)),
                Aggregates.limit(1),
                Aggregates.project(Projections.fields(
                        Projections.include(
                                BsonConstants.BSON_KEY_PV_NAME,
                                BsonConstants.BSON_KEY_BUCKET_FIRST_TIME_SECS,
                                BsonConstants.BSON_KEY_BUCKET_LAST_TIME_SECS),
                        Projections.computed(PROJECTED_HAS_DATA_COLUMN, new Document("$cond",
                                Arrays.asList(
                                        new Document("$ifNull",
                                                Arrays.asList("$" + FIELD_DATA_COLUMN, false)),
                                        true, false))),
                        Projections.computed(PROJECTED_HAS_DATA_TIMESTAMPS, new Document("$cond",
                                Arrays.asList(
                                        new Document("$ifNull",
                                                Arrays.asList("$" + FIELD_DATA_TIMESTAMPS, false)),
                                        true, false))))));

        return bucketsCollection.aggregate(pipeline).first();
    }

    /**
     * Interprets a scan hit as a malformed bucket, or returns null if it is well-formed (and
     * therefore matched on the span predicate instead).
     */
    private static CorruptBucket asCorruptBucket(Document offender) {

        final boolean hasDataColumn = Boolean.TRUE.equals(offender.getBoolean(PROJECTED_HAS_DATA_COLUMN));
        final boolean hasDataTimestamps =
                Boolean.TRUE.equals(offender.getBoolean(PROJECTED_HAS_DATA_TIMESTAMPS));

        if (hasDataColumn && hasDataTimestamps) {
            return null;
        }

        final Object id = offender.get("_id");
        final String missingField = !hasDataColumn && !hasDataTimestamps
                ? FIELD_DATA_COLUMN + " and " + FIELD_DATA_TIMESTAMPS
                : (!hasDataColumn ? FIELD_DATA_COLUMN : FIELD_DATA_TIMESTAMPS);

        return new CorruptBucket(
                id == null ? null : id.toString(),
                offender.getString(BsonConstants.BSON_KEY_PV_NAME),
                missingField);
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
