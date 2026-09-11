package com.ospreydcs.dp.service.common.mongo;

import com.mongodb.client.model.Filters;
import com.ospreydcs.dp.service.common.bson.BsonConstants;
import org.bson.BsonDocument;
import org.bson.codecs.configuration.CodecRegistry;
import org.bson.conversions.Bson;
import org.junit.Test;

import java.time.Instant;
import java.util.Collections;
import java.util.List;
import java.util.regex.Pattern;

import static com.mongodb.MongoClientSettings.getDefaultCodecRegistry;
import static org.junit.Assert.*;

/**
 * Unit tests for {@link MongoQueryFilterBuilder}. The primary goal is to prove the extracted
 * helpers emit BSON that is byte-identical to the previously inline logic in
 * {@code MongoSyncAnnotationClient} (the Q6 behavior-preserving acceptance bar), plus edge cases.
 */
public class MongoQueryFilterBuilderTest {

    private static final CodecRegistry REGISTRY = getDefaultCodecRegistry();

    /** Render a Bson filter to a canonical BsonDocument for equality comparison. */
    private static BsonDocument render(Bson filter) {
        return filter.toBsonDocument(BsonDocument.class, REGISTRY);
    }

    private static void assertSameBson(Bson expected, Bson actual) {
        assertEquals(render(expected), render(actual));
    }

    private static final String FIELD = BsonConstants.BSON_KEY_PV_METADATA_PV_NAME;

    // -----------------------------------------------------------------------
    // nameMatchFilter
    // -----------------------------------------------------------------------

    @Test
    public void testNameMatchFilter_allEmptyReturnsNull() {
        assertNull(MongoQueryFilterBuilder.nameMatchFilter(
                FIELD, List.of(), List.of(), List.of()));
    }

    @Test
    public void testNameMatchFilter_nullListsReturnNull() {
        assertNull(MongoQueryFilterBuilder.nameMatchFilter(FIELD, null, null, null));
    }

    @Test
    public void testNameMatchFilter_exactOnly_singleFilterNotWrappedInOr() {
        final List<String> exact = List.of("pv1", "pv2");
        final Bson actual = MongoQueryFilterBuilder.nameMatchFilter(
                FIELD, exact, List.of(), List.of());
        // single filter => returned directly, not wrapped in $or
        assertSameBson(Filters.in(FIELD, exact), actual);
    }

    @Test
    public void testNameMatchFilter_prefixEscapedWithPatternQuote() {
        final String prefix = "a.b*c"; // regex-special chars must be quoted
        final Bson actual = MongoQueryFilterBuilder.nameMatchFilter(
                FIELD, List.of(), List.of(prefix), List.of());
        assertSameBson(Filters.regex(FIELD, "^" + Pattern.quote(prefix)), actual);
    }

    @Test
    public void testNameMatchFilter_containsEscapedWithPatternQuote() {
        final String contains = "x+y(z)";
        final Bson actual = MongoQueryFilterBuilder.nameMatchFilter(
                FIELD, List.of(), List.of(), List.of(contains));
        assertSameBson(Filters.regex(FIELD, ".*" + Pattern.quote(contains) + ".*"), actual);
    }

    @Test
    public void testNameMatchFilter_combinedWrappedInOr() {
        // exact + prefix + contains => three filters combined with $or, in this order
        final List<String> exact = List.of("e1");
        final String prefix = "pre";
        final String contains = "con";
        final Bson actual = MongoQueryFilterBuilder.nameMatchFilter(
                FIELD, exact, List.of(prefix), List.of(contains));
        final Bson expected = Filters.or(
                Filters.in(FIELD, exact),
                Filters.regex(FIELD, "^" + Pattern.quote(prefix)),
                Filters.regex(FIELD, ".*" + Pattern.quote(contains) + ".*"));
        assertSameBson(expected, actual);
    }

    @Test
    public void testNameMatchFilter_respectsFieldArgument() {
        // aliases criterion uses the same logic against a different field
        final Bson actual = MongoQueryFilterBuilder.nameMatchFilter(
                BsonConstants.BSON_KEY_PV_METADATA_ALIASES, List.of("alias1"), List.of(), List.of());
        assertSameBson(Filters.in(BsonConstants.BSON_KEY_PV_METADATA_ALIASES, List.of("alias1")), actual);
    }

    // -----------------------------------------------------------------------
    // tagsFilter
    // -----------------------------------------------------------------------

    @Test
    public void testTagsFilter() {
        final List<String> values = List.of("tag1", "tag2");
        assertSameBson(Filters.in(BsonConstants.BSON_KEY_TAGS, values),
                MongoQueryFilterBuilder.tagsFilter(values));
    }

    // -----------------------------------------------------------------------
    // attributeFilter
    // -----------------------------------------------------------------------

    @Test
    public void testAttributeFilter_keyOnlyUsesExists() {
        final String key = "sector";
        final String mapKey = BsonConstants.BSON_KEY_ATTRIBUTES + "." + key;
        assertSameBson(Filters.exists(mapKey),
                MongoQueryFilterBuilder.attributeFilter(key, Collections.emptyList()));
    }

    @Test
    public void testAttributeFilter_nullValuesUsesExists() {
        final String key = "sector";
        final String mapKey = BsonConstants.BSON_KEY_ATTRIBUTES + "." + key;
        assertSameBson(Filters.exists(mapKey),
                MongoQueryFilterBuilder.attributeFilter(key, null));
    }

    @Test
    public void testAttributeFilter_withValuesUsesIn() {
        final String key = "sector";
        final List<String> values = List.of("A", "B");
        final String mapKey = BsonConstants.BSON_KEY_ATTRIBUTES + "." + key;
        assertSameBson(Filters.in(mapKey, values),
                MongoQueryFilterBuilder.attributeFilter(key, values));
    }

    // -----------------------------------------------------------------------
    // activationContainsInstantFilter
    // -----------------------------------------------------------------------

    @Test
    public void testActivationContainsInstantFilter() {
        final Instant ts = Instant.ofEpochSecond(1_700_000_000L, 123);
        final Bson expected = Filters.and(
                Filters.lte(BsonConstants.BSON_KEY_ACTIVATION_START_TIME, ts),
                Filters.or(
                        Filters.exists(BsonConstants.BSON_KEY_ACTIVATION_END_TIME, false),
                        Filters.gt(BsonConstants.BSON_KEY_ACTIVATION_END_TIME, ts)));
        assertSameBson(expected, MongoQueryFilterBuilder.activationContainsInstantFilter(ts));
    }

    // -----------------------------------------------------------------------
    // activationOverlapsRangeFilter
    // -----------------------------------------------------------------------

    @Test
    public void testActivationOverlapsRangeFilter() {
        final Instant start = Instant.ofEpochSecond(1_700_000_000L);
        final Instant end = Instant.ofEpochSecond(1_700_000_100L);
        final Bson expected = Filters.and(
                Filters.lt(BsonConstants.BSON_KEY_ACTIVATION_START_TIME, end),
                Filters.or(
                        Filters.exists(BsonConstants.BSON_KEY_ACTIVATION_END_TIME, false),
                        Filters.gt(BsonConstants.BSON_KEY_ACTIVATION_END_TIME, start)));
        assertSameBson(expected, MongoQueryFilterBuilder.activationOverlapsRangeFilter(start, end));
    }

    // -----------------------------------------------------------------------
    // bucketOverlapsRangeFilter
    // -----------------------------------------------------------------------

    /** A representative per-query span, as the query client resolves it from pvStats (#232). */
    private static final long SPAN_SECONDS = 86_400L;

    /** {@code firstTime < end} as the builder renders it: seconds/nanos lexicographic compare. */
    private static Bson expectedEndTimeFilter(long endSecs, long endNanos) {
        return Filters.or(
                Filters.lt(BsonConstants.BSON_KEY_BUCKET_FIRST_TIME_SECS, endSecs),
                Filters.and(
                        Filters.eq(BsonConstants.BSON_KEY_BUCKET_FIRST_TIME_SECS, endSecs),
                        Filters.lt(BsonConstants.BSON_KEY_BUCKET_FIRST_TIME_NANOS, endNanos)));
    }

    /** {@code lastTime >= begin} as the builder renders it: seconds/nanos lexicographic compare. */
    private static Bson expectedStartTimeFilter(long beginSecs, long beginNanos) {
        return Filters.or(
                Filters.gt(BsonConstants.BSON_KEY_BUCKET_LAST_TIME_SECS, beginSecs),
                Filters.and(
                        Filters.eq(BsonConstants.BSON_KEY_BUCKET_LAST_TIME_SECS, beginSecs),
                        Filters.gte(BsonConstants.BSON_KEY_BUCKET_LAST_TIME_NANOS, beginNanos)));
    }

    @Test
    public void testBucketOverlapsRangeFilter_includesSpanLowerBound() {
        final long beginSecs = 1_781_701_200L;
        final long beginNanos = 0L;
        final long endSecs = 1_781_701_201L;
        final long endNanos = 0L;

        // The lower bound keeps the compound index scan from starting at the beginning of each
        // PV's history: firstTime.seconds >= beginSeconds - maxBucketSpanSeconds, with the span
        // supplied by the caller from pvStats.
        final Bson spanLowerBoundFilter = Filters.gte(
                BsonConstants.BSON_KEY_BUCKET_FIRST_TIME_SECS, beginSecs - SPAN_SECONDS);
        final Bson expected = Filters.and(
                spanLowerBoundFilter,
                expectedEndTimeFilter(endSecs, endNanos),
                expectedStartTimeFilter(beginSecs, beginNanos));

        assertSameBson(expected, MongoQueryFilterBuilder.bucketOverlapsRangeFilter(
                beginSecs, beginNanos, endSecs, endNanos, SPAN_SECONDS));
    }

    /**
     * A span of zero is what the query client resolves when none of the named PVs has a pvStats
     * document (plan D6): the bound collapses to {@code firstTime.seconds >= beginSeconds}, which is
     * correct for PVs with no stored buckets. It must be applied, not omitted.
     */
    @Test
    public void testBucketOverlapsRangeFilter_zeroSpanBoundsAtBegin() {
        final long beginSecs = 1_781_701_200L;
        final long beginNanos = 500L;
        final long endSecs = 1_781_701_201L;
        final long endNanos = 0L;

        final Bson expected = Filters.and(
                Filters.gte(BsonConstants.BSON_KEY_BUCKET_FIRST_TIME_SECS, beginSecs),
                expectedEndTimeFilter(endSecs, endNanos),
                expectedStartTimeFilter(beginSecs, beginNanos));

        assertSameBson(expected, MongoQueryFilterBuilder.bucketOverlapsRangeFilter(
                beginSecs, beginNanos, endSecs, endNanos, 0L));
    }

    /**
     * No stored statistic can be negative, and a negative span would tighten the bound past
     * {@code begin} and silently drop overlapping buckets, so the builder rejects it outright
     * rather than emitting a wrong-answer filter.
     */
    @Test
    public void testBucketOverlapsRangeFilter_rejectsNegativeSpan() {
        try {
            MongoQueryFilterBuilder.bucketOverlapsRangeFilter(
                    1_781_701_200L, 0L, 1_781_701_201L, 0L, -1L);
            fail("expected IllegalArgumentException for a negative span");
        } catch (IllegalArgumentException ex) {
            assertTrue(ex.getMessage(), ex.getMessage().contains("non-negative"));
        }
    }

    /**
     * Query time ranges carry no validated lower bound, so a begin time near {@code Long.MIN_VALUE}
     * reaches the {@code beginSeconds - maxBucketSpanSeconds} subtraction. Wrapping would yield a
     * large POSITIVE lower bound that excludes every stored bucket, turning an over-wide query into
     * a silent empty result; only the lower bound must be omitted instead.
     */
    @Test
    public void testBucketOverlapsRangeFilter_omitsSpanLowerBoundOnUnderflow() {
        final long beginSecs = Long.MIN_VALUE;
        final long beginNanos = 0L;
        final long endSecs = 1_781_701_201L;
        final long endNanos = 0L;

        final Bson expected = Filters.and(
                expectedEndTimeFilter(endSecs, endNanos),
                expectedStartTimeFilter(beginSecs, beginNanos));

        assertSameBson(expected, MongoQueryFilterBuilder.bucketOverlapsRangeFilter(
                beginSecs, beginNanos, endSecs, endNanos, SPAN_SECONDS));
    }

    /**
     * A begin time just above the underflow threshold still gets the bound, so the guard does not
     * disable the optimization for ordinary queries, and its value is the exact (non-wrapped)
     * difference.
     */
    @Test
    public void testBucketOverlapsRangeFilter_keepsSpanLowerBoundJustAboveUnderflow() {
        final long beginSecs = Long.MIN_VALUE + SPAN_SECONDS;
        final long beginNanos = 0L;
        final long endSecs = 1_781_701_201L;
        final long endNanos = 0L;

        final Bson expected = Filters.and(
                Filters.gte(BsonConstants.BSON_KEY_BUCKET_FIRST_TIME_SECS, Long.MIN_VALUE),
                expectedEndTimeFilter(endSecs, endNanos),
                expectedStartTimeFilter(beginSecs, beginNanos));

        assertSameBson(expected, MongoQueryFilterBuilder.bucketOverlapsRangeFilter(
                beginSecs, beginNanos, endSecs, endNanos, SPAN_SECONDS));
    }
}
