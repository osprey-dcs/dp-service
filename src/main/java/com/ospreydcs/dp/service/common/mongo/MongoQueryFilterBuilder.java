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
     * <p>Also the natural building block for a Query API V2 configuration-fragment overlap predicate.
     */
    public static Bson activationOverlapsRangeFilter(Instant rangeStart, Instant rangeEnd) {
        return Filters.and(
                Filters.lt(BsonConstants.BSON_KEY_ACTIVATION_START_TIME, rangeEnd),
                Filters.or(
                        Filters.exists(BsonConstants.BSON_KEY_ACTIVATION_END_TIME, false),
                        Filters.gt(BsonConstants.BSON_KEY_ACTIVATION_END_TIME, rangeStart)));
    }
}
