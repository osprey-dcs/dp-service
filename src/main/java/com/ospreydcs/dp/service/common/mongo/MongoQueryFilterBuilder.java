package com.ospreydcs.dp.service.common.mongo;

import com.mongodb.client.model.Filters;
import com.ospreydcs.dp.service.common.bson.BsonConstants;
import org.bson.conversions.Bson;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;

/**
 * Shared, proto-neutral builders for the MongoDB filter fragments used by both the annotation
 * metadata/activation queries (V1) and the Query API V2 planner. Each method takes neutral inputs
 * (name lists, tag lists, attribute key/values, {@link Instant}s) and returns a {@link Bson}, so a
 * single implementation of each mapping serves every caller and the two APIs cannot drift.
 *
 * <p>The methods are behavior-preserving relative to the previously inline logic in
 * {@code MongoSyncAnnotationClient}: they emit byte-identical {@code Bson} for the same inputs.
 */
public class MongoQueryFilterBuilder {

    /**
     * Builds a name/alias match filter combining exact, prefix, and contains matches with OR.
     *
     * <p>Exact matches become a single {@code in(field, exact)}. Each prefix becomes
     * {@code regex(field, "^" + Pattern.quote(prefix))} and each contains becomes
     * {@code regex(field, ".*" + Pattern.quote(contains) + ".*")} — the substrings are treated as
     * literals (regex-special characters are escaped by {@link Pattern#quote}).
     *
     * @return the combined filter, or {@code null} if all three lists are empty (the caller decides
     *     whether to add it to a filter list, preserving the "only add if non-empty" behavior).
     */
    public static Bson nameMatchFilter(
            String field, List<String> exact, List<String> prefix, List<String> contains) {

        final List<Bson> nameFilters = new ArrayList<>();
        if (exact != null && !exact.isEmpty()) {
            nameFilters.add(Filters.in(field, exact));
        }
        if (prefix != null) {
            for (String p : prefix) {
                nameFilters.add(Filters.regex(field, "^" + Pattern.quote(p)));
            }
        }
        if (contains != null) {
            for (String c : contains) {
                nameFilters.add(Filters.regex(field, ".*" + Pattern.quote(c) + ".*"));
            }
        }
        if (nameFilters.isEmpty()) {
            return null;
        }
        return nameFilters.size() == 1 ? nameFilters.get(0) : Filters.or(nameFilters);
    }

    /**
     * Builds a tags membership filter: {@code in(BSON_KEY_TAGS, values)}.
     */
    public static Bson tagsFilter(List<String> values) {
        return Filters.in(BsonConstants.BSON_KEY_TAGS, values);
    }

    /**
     * Builds an attribute filter for the {@code attributes.<key>} map field. With no values, matches
     * documents that have the key at all ({@code exists}); otherwise matches any of the given values
     * ({@code in}).
     */
    public static Bson attributeFilter(String key, List<String> values) {
        final String mapKey = BsonConstants.BSON_KEY_ATTRIBUTES + "." + key;
        if (values == null || values.isEmpty()) {
            return Filters.exists(mapKey);
        }
        return Filters.in(mapKey, values);
    }

    /**
     * Builds the filter for activations whose {@code [startTime, endTime)} interval contains the
     * given instant: {@code startTime <= ts AND (endTime absent OR endTime > ts)}.
     */
    public static Bson activationContainsInstantFilter(Instant ts) {
        return Filters.and(
                Filters.lte(BsonConstants.BSON_KEY_ACTIVATION_START_TIME, ts),
                Filters.or(
                        Filters.exists(BsonConstants.BSON_KEY_ACTIVATION_END_TIME, false),
                        Filters.gt(BsonConstants.BSON_KEY_ACTIVATION_END_TIME, ts)));
    }

    /**
     * Builds the filter for activations whose {@code [startTime, endTime)} interval overlaps the
     * half-open range {@code [rangeStart, rangeEnd)}:
     * {@code startTime < rangeEnd AND (endTime absent OR endTime > rangeStart)}.
     *
     * <p>Operates on the {@code configurationActivations} time fields. This is NOT the bucket overlap
     * predicate — see {@link #bucketOverlapsRangeFilter} for that (different fields, different
     * collection, {@code lastTime >=} inclusive semantics).
     */
    public static Bson activationOverlapsRangeFilter(Instant rangeStart, Instant rangeEnd) {
        return Filters.and(
                Filters.lt(BsonConstants.BSON_KEY_ACTIVATION_START_TIME, rangeEnd),
                Filters.or(
                        Filters.exists(BsonConstants.BSON_KEY_ACTIVATION_END_TIME, false),
                        Filters.gt(BsonConstants.BSON_KEY_ACTIVATION_END_TIME, rangeStart)));
    }

    /**
     * Builds the bucket overlap predicate selecting buckets whose {@code [firstTime, lastTime]} data
     * span intersects the half-open query range {@code [begin, end)}:
     * {@code firstTime < end AND lastTime >= begin} (with {@code (seconds, nanos)} lexicographic
     * comparison), plus the index-enabling bounds on {@code firstTime.seconds} from
     * {@link #bucketFirstTimeSecondsIndexBounds}:
     * {@code beginSeconds - maxBucketSpanSeconds <= firstTime.seconds <= endSeconds}. This is the
     * single source for the overlap condition shared by the V1 data/table retrieval path
     * ({@code executeBucketDocumentQuery}) and the Query API V2 {@code $or} configuration-fragment
     * retrieval, so the two cannot drift.
     *
     * <p>The two bounds are what turn the compound {@code (pvName, firstTime)} index scan into a
     * scan of the window: the overlap predicate's own halves are {@code (seconds, nanos)}
     * {@code $or}s the planner cannot use as index bounds, so without them the scan is open-ended
     * on both sides of the window. The lower bound keeps it from starting at the beginning of each
     * PV's history; the upper bound (#271, implied by {@code firstTime < end}) keeps it from
     * running on to the end, which on a long-lived PV is most of its archive whenever the planner
     * picks the single-scan plan. The caller supplies {@code maxBucketSpanSeconds} from the
     * {@code pvStats} collection as the largest {@code lastTime.seconds - firstTime.seconds} over
     * every bucket ever stored for the PVs the query names (#232, plan D1/D3). That seconds-field
     * difference is exactly the quantity the bound consumes: any bucket overlapping the range has
     * {@code lastTime.seconds >= beginSeconds}, hence {@code firstTime.seconds >= beginSeconds - span},
     * so the bound excludes no overlapping bucket as long as the supplied span is at least every
     * such bucket's seconds-field difference. A span of zero (no {@code pvStats} document for any
     * named PV, plan D6) yields {@code firstTime.seconds >= beginSeconds}, which is correct for PVs
     * with no stored buckets.
     *
     * <p>Distinct from {@link #activationOverlapsRangeFilter}: this operates on the buckets
     * collection's {@code dataTimestamps.firstTime}/{@code lastTime} fields, and the lower bound is
     * inclusive ({@code lastTime >= begin}) matching the long-standing V1 semantics.
     *
     * @param maxBucketSpanSeconds largest {@code lastTime.seconds - firstTime.seconds} over the
     *                             buckets the query can match; must be non-negative
     * @throws IllegalArgumentException if {@code maxBucketSpanSeconds} is negative, which no stored
     *                                  statistic can be, and which would tighten the bound past
     *                                  {@code begin} and silently drop overlapping buckets
     */
    public static Bson bucketOverlapsRangeFilter(
            long beginSeconds, long beginNanos, long endSeconds, long endNanos,
            long maxBucketSpanSeconds) {

        if (maxBucketSpanSeconds < 0) {
            throw new IllegalArgumentException(
                    "maxBucketSpanSeconds must be non-negative: " + maxBucketSpanSeconds);
        }

        // firstTime < end : firstSecs < endSecs OR (firstSecs == endSecs AND firstNanos < endNanos)
        final Bson endTimeFilter = Filters.or(
                Filters.lt(BsonConstants.BSON_KEY_BUCKET_FIRST_TIME_SECS, endSeconds),
                Filters.and(
                        Filters.eq(BsonConstants.BSON_KEY_BUCKET_FIRST_TIME_SECS, endSeconds),
                        Filters.lt(BsonConstants.BSON_KEY_BUCKET_FIRST_TIME_NANOS, endNanos)));

        // lastTime >= begin : lastSecs > beginSecs OR (lastSecs == beginSecs AND lastNanos >= beginNanos)
        final Bson startTimeFilter = Filters.or(
                Filters.gt(BsonConstants.BSON_KEY_BUCKET_LAST_TIME_SECS, beginSeconds),
                Filters.and(
                        Filters.eq(BsonConstants.BSON_KEY_BUCKET_LAST_TIME_SECS, beginSeconds),
                        Filters.gte(BsonConstants.BSON_KEY_BUCKET_LAST_TIME_NANOS, beginNanos)));

        final List<Bson> parts = new ArrayList<>(
                bucketFirstTimeSecondsIndexBounds(beginSeconds, endSeconds, maxBucketSpanSeconds));
        parts.add(endTimeFilter);
        parts.add(startTimeFilter);
        return Filters.and(parts);
    }

    /**
     * The index-bound predicates on {@code firstTime.seconds} for buckets overlapping
     * {@code [begin, end)}: {@code >= beginSeconds - maxBucketSpanSeconds} and
     * {@code <= endSeconds}, as plain top-level range predicates the planner turns into one
     * interval on the compound {@code (pvName, firstTime.seconds, ...)} index. Both are implied by
     * the overlap predicate -- they exclude no overlapping bucket -- and exist only so the scan
     * has an index bound on each side. {@link #bucketOverlapsRangeFilter} applies them to one
     * window; the Query API V2 fragment {@code $or} applies them once, hoisted above the
     * {@code $or} over the fragments' extreme begin and end (#271), because a bound inside an
     * {@code $or} branch is not an index bound for the single-scan plan the planner prefers.
     *
     * <p>The lower bound: any bucket with {@code lastTime >= begin} has
     * {@code firstTime.seconds >= beginSeconds - span}, the span being the largest seconds-field
     * difference over the buckets in play (see {@link #bucketOverlapsRangeFilter}). Its
     * subtraction saturates rather than wraps: query time ranges are not validated for a lower
     * bound, so a begin time near {@code Long.MIN_VALUE} reaches it, and wrapping would produce a
     * large POSITIVE lower bound that excludes every bucket, turning an over-wide query into a
     * silent empty result. Omitting only the lower bound makes it a no-op instead, which is the
     * correct meaning: a query starting at the dawn of time has no useful lower bound.
     *
     * <p>The upper bound: {@code firstTime < end} implies {@code firstTime.seconds <= endSeconds}.
     * It cannot overflow and is always applied.
     *
     * @return one or two predicates, the lower bound first when present
     */
    public static List<Bson> bucketFirstTimeSecondsIndexBounds(
            long beginSeconds, long endSeconds, long maxBucketSpanSeconds) {

        if (maxBucketSpanSeconds < 0) {
            throw new IllegalArgumentException(
                    "maxBucketSpanSeconds must be non-negative: " + maxBucketSpanSeconds);
        }

        final List<Bson> bounds = new ArrayList<>(2);
        try {
            bounds.add(Filters.gte(
                    BsonConstants.BSON_KEY_BUCKET_FIRST_TIME_SECS,
                    Math.subtractExact(beginSeconds, maxBucketSpanSeconds)));
        } catch (ArithmeticException ex) {
            // underflow: no lower bound (see the Javadoc)
        }
        bounds.add(Filters.lte(BsonConstants.BSON_KEY_BUCKET_FIRST_TIME_SECS, endSeconds));
        return bounds;
    }
}
