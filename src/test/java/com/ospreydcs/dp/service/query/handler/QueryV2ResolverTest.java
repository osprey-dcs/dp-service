package com.ospreydcs.dp.service.query.handler;

import com.mongodb.client.MongoCursor;
import com.ospreydcs.dp.grpc.v1.common.TimeRange;
import com.ospreydcs.dp.grpc.v1.common.Timestamp;
import com.ospreydcs.dp.grpc.v1.query.ConfigurationSelector;
import com.ospreydcs.dp.grpc.v1.query.ExecutionOptions;
import com.ospreydcs.dp.grpc.v1.query.PvNameList;
import com.ospreydcs.dp.grpc.v1.query.PvSelector;
import com.ospreydcs.dp.grpc.v1.query.QuerySpec;
import com.ospreydcs.dp.grpc.v1.query.ResultRepresentation;
import com.ospreydcs.dp.grpc.v1.query.SampleStatusSelector;
import com.ospreydcs.dp.service.common.bson.PvMetadataQueryResultDocument;
import com.ospreydcs.dp.service.common.bson.ProviderDocument;
import com.ospreydcs.dp.service.common.bson.ProviderMetadataQueryResultDocument;
import com.ospreydcs.dp.service.common.bson.bucket.BucketDocument;
import com.ospreydcs.dp.service.common.bson.dataset.DataBlockDocument;
import com.ospreydcs.dp.grpc.v1.query.QueryDataRequest;
import com.ospreydcs.dp.grpc.v1.query.QueryProviderStatsRequest;
import com.ospreydcs.dp.grpc.v1.query.QueryProvidersRequest;
import com.ospreydcs.dp.grpc.v1.query.QueryPvStatsRequest;
import com.ospreydcs.dp.grpc.v1.query.QueryTableRequest;
import com.ospreydcs.dp.service.query.handler.model.KeysetPosition;
import com.ospreydcs.dp.service.query.handler.model.ResolutionResult;
import com.ospreydcs.dp.service.query.handler.model.ResolvedQuery;
import com.ospreydcs.dp.service.query.handler.model.ResolvedStatusFilter;
import com.ospreydcs.dp.service.query.handler.model.TimeInterval;
import com.ospreydcs.dp.service.query.handler.mongo.client.MongoQueryClientInterface;
import com.ospreydcs.dp.service.query.handler.paging.PageToken;
import org.bson.conversions.Bson;
import org.junit.Test;

import java.util.Collection;
import java.util.List;

import static org.junit.Assert.*;

/**
 * Unit tests for {@link QueryV2Resolver} focused on the pure validation and paging-normalization
 * paths (§6 invariants, Q7 clamping, token mode/kind checks). PV-list resolution needs no DB round
 * trip, so the happy path is exercised here too via a stub client; DB-backed pattern/metadata/config
 * resolution is integration-tested in the queryBuckets step.
 */
public class QueryV2ResolverTest {

    private static final int DEFAULT_PAGE_SIZE = 10_000;
    private static final int MAX_PAGE_SIZE = 100_000;
    private static final int MAX_RESOLVED_PVS = 5;

    /** Stub client: no method should be called for the pure PV-list validation paths under test. */
    private static class StubClient implements MongoQueryClientInterface {
        @Override public boolean init() { return true; }
        @Override public boolean fini() { return true; }
        @Override public MongoCursor<BucketDocument> executeDataBlockQuery(DataBlockDocument d) { return null; }
        @Override public MongoCursor<BucketDocument> executeQueryData(QueryDataRequest.QuerySpec q) { return null; }
        @Override public MongoCursor<BucketDocument> executeQueryTable(QueryTableRequest r) { return null; }
        @Override public MongoCursor<PvMetadataQueryResultDocument> executeQueryPvStats(QueryPvStatsRequest r) { return null; }
        @Override public MongoCursor<PvMetadataQueryResultDocument> executeQueryPvStats(Collection<String> l) { return null; }
        @Override public MongoCursor<PvMetadataQueryResultDocument> executeQueryPvStats(String p) { return null; }
        @Override public Collection<String> executeQueryPvExistence(Collection<String> l) { return null; }
        @Override public List<String> resolvePvNamesByPattern(String p) { return List.of(); }
        @Override public List<String> resolvePvNamesByMetadata(List<Bson> f) { return List.of(); }
        @Override public List<TimeInterval> resolveConfigurationIntervals(List<Bson> f) { return List.of(); }
        @Override public MongoCursor<BucketDocument> executeQueryBucketsV2(ResolvedQuery q) { return null; }
        @Override public MongoCursor<BucketDocument> executeQueryBucketsV2Stream(ResolvedQuery q) { return null; }
        @Override public MongoCursor<BucketDocument> executeQuerySamplesV2(ResolvedQuery q, long bs, long bn) { return null; }
        @Override public java.util.Map<String, java.util.Set<Long>> resolveSampleStatusTimestamps(ResolvedQuery q, long bs, long bn) { return java.util.Map.of(); }
        @Override public MongoCursor<ProviderDocument> executeQueryProviders(QueryProvidersRequest r) { return null; }
        @Override public MongoCursor<ProviderMetadataQueryResultDocument> executeQueryProviderStats(QueryProviderStatsRequest r) { return null; }
        @Override public MongoCursor<ProviderMetadataQueryResultDocument> executeQueryProviderStats(String id) { return null; }
    }

    private QueryV2Resolver resolver() {
        return new QueryV2Resolver(new StubClient(), DEFAULT_PAGE_SIZE, MAX_PAGE_SIZE, MAX_RESOLVED_PVS);
    }

    private QueryV2Resolver resolver(MongoQueryClientInterface client) {
        return new QueryV2Resolver(client, DEFAULT_PAGE_SIZE, MAX_PAGE_SIZE, MAX_RESOLVED_PVS);
    }

    /** Stub variant returning a fixed set of activation intervals for configuration resolution. */
    private static class IntervalStubClient extends StubClient {
        private final List<TimeInterval> intervals;
        IntervalStubClient(List<TimeInterval> intervals) { this.intervals = intervals; }
        @Override public List<TimeInterval> resolveConfigurationIntervals(List<Bson> f) { return intervals; }
    }

    private static ConfigurationSelector.Criterion configNameCriterion(String... names) {
        return ConfigurationSelector.Criterion.newBuilder()
                .setConfigurationNameCriterion(ConfigurationSelector.Criterion.ConfigurationNameCriterion
                        .newBuilder().addAllValues(List.of(names)))
                .build();
    }

    private static Timestamp ts(long secs, long nanos) {
        return Timestamp.newBuilder().setEpochSeconds(secs).setNanoseconds(nanos).build();
    }

    private static QuerySpec.Builder specWithTimeRange(long beginSecs, long endSecs) {
        return QuerySpec.newBuilder().setTimeRange(TimeRange.newBuilder()
                .setBeginTime(ts(beginSecs, 0))
                .setEndTime(ts(endSecs, 0)));
    }

    private static PvSelector pvList(String... names) {
        return PvSelector.newBuilder()
                .setPvNameList(PvNameList.newBuilder().addAllPvNames(List.of(names)))
                .build();
    }

    private ResolutionResult resolve(QuerySpec spec, ExecutionOptions opts, boolean streaming) {
        return resolver().resolve(
                spec, opts, ResultRepresentation.getDefaultInstance(),
                ResolvedQuery.ResultMode.BUCKET, streaming);
    }

    // -----------------------------------------------------------------------
    // TimeRange validation
    // -----------------------------------------------------------------------

    @Test
    public void testMissingTimeRangeRejected() {
        final QuerySpec spec = QuerySpec.newBuilder().setPvSelector(pvList("pv1")).build();
        final ResolutionResult r = resolve(spec, ExecutionOptions.getDefaultInstance(), false);
        assertTrue(r.isError());
        assertTrue(r.getErrorStatus().msg.contains("timeRange"));
    }

    @Test
    public void testEndBeforeBeginRejected() {
        final QuerySpec spec = specWithTimeRange(100, 50).setPvSelector(pvList("pv1")).build();
        final ResolutionResult r = resolve(spec, ExecutionOptions.getDefaultInstance(), false);
        assertTrue(r.isError());
        assertTrue(r.getErrorStatus().msg.contains("endTime must be > beginTime"));
    }

    @Test
    public void testEqualBeginEndRejected() {
        final QuerySpec spec = specWithTimeRange(100, 100).setPvSelector(pvList("pv1")).build();
        final ResolutionResult r = resolve(spec, ExecutionOptions.getDefaultInstance(), false);
        assertTrue(r.isError());
    }

    // -----------------------------------------------------------------------
    // PvSelector validation
    // -----------------------------------------------------------------------

    @Test
    public void testMissingPvSelectorRejected() {
        final QuerySpec spec = specWithTimeRange(0, 100).build();
        final ResolutionResult r = resolve(spec, ExecutionOptions.getDefaultInstance(), false);
        assertTrue(r.isError());
        assertTrue(r.getErrorStatus().msg.contains("pvSelector"));
    }

    @Test
    public void testEmptyPvNameListRejected() {
        final QuerySpec spec = specWithTimeRange(0, 100)
                .setPvSelector(PvSelector.newBuilder()
                        .setPvNameList(PvNameList.getDefaultInstance()))
                .build();
        final ResolutionResult r = resolve(spec, ExecutionOptions.getDefaultInstance(), false);
        assertTrue(r.isError());
    }

    // -----------------------------------------------------------------------
    // Happy path + interval defaulting
    // -----------------------------------------------------------------------

    @Test
    public void testHappyPathPvListSortedAndWholeRangeInterval() {
        final QuerySpec spec = specWithTimeRange(10, 20).setPvSelector(pvList("pvC", "pvA", "pvB")).build();
        final ResolutionResult r = resolve(spec, ExecutionOptions.getDefaultInstance(), false);
        assertFalse(r.isError());
        final ResolvedQuery rq = r.getResolvedQuery();
        assertEquals(List.of("pvA", "pvB", "pvC"), rq.getPvNames()); // sorted
        assertEquals(1, rq.getRetrievalIntervals().size());
        assertEquals(new TimeInterval(10, 0, 20, 0), rq.getRetrievalIntervals().get(0));
        assertEquals(DEFAULT_PAGE_SIZE, rq.getPageSize());
        assertNull(rq.getPageStart());
        assertFalse(rq.isEmptyResult());
    }

    @Test
    public void testDuplicatePvNamesDeduped() {
        final QuerySpec spec = specWithTimeRange(0, 100).setPvSelector(pvList("pv1", "pv1", "pv2")).build();
        final ResolutionResult r = resolve(spec, ExecutionOptions.getDefaultInstance(), false);
        assertFalse(r.isError());
        assertEquals(List.of("pv1", "pv2"), r.getResolvedQuery().getPvNames());
    }

    @Test
    public void testResolvedPvCountCapExceededRejected() {
        final QuerySpec spec = specWithTimeRange(0, 100)
                .setPvSelector(pvList("a", "b", "c", "d", "e", "f")) // 6 > MAX_RESOLVED_PVS (5)
                .build();
        final ResolutionResult r = resolve(spec, ExecutionOptions.getDefaultInstance(), false);
        assertTrue(r.isError());
        assertTrue(r.getErrorStatus().msg.contains("exceeding the maximum"));
    }

    // -----------------------------------------------------------------------
    // Paging normalization (Q7)
    // -----------------------------------------------------------------------

    @Test
    public void testLimitZeroUsesDefault() {
        final QuerySpec spec = specWithTimeRange(0, 100).setPvSelector(pvList("pv1")).build();
        final ResolutionResult r = resolve(spec, ExecutionOptions.newBuilder().setLimit(0).build(), false);
        assertEquals(DEFAULT_PAGE_SIZE, r.getResolvedQuery().getPageSize());
    }

    @Test
    public void testLimitAboveMaxSilentlyClamped() {
        final QuerySpec spec = specWithTimeRange(0, 100).setPvSelector(pvList("pv1")).build();
        final ResolutionResult r = resolve(
                spec, ExecutionOptions.newBuilder().setLimit(MAX_PAGE_SIZE + 5_000).build(), false);
        assertFalse(r.isError()); // clamp, not reject
        assertEquals(MAX_PAGE_SIZE, r.getResolvedQuery().getPageSize());
    }

    @Test
    public void testLimitWithinRangeHonored() {
        final QuerySpec spec = specWithTimeRange(0, 100).setPvSelector(pvList("pv1")).build();
        final ResolutionResult r = resolve(spec, ExecutionOptions.newBuilder().setLimit(500).build(), false);
        assertEquals(500, r.getResolvedQuery().getPageSize());
    }

    // -----------------------------------------------------------------------
    // Page token handling
    // -----------------------------------------------------------------------

    @Test
    public void testStreamingWithTokenRejected() {
        final String token = PageToken.encode(KeysetPosition.ofBucket("pv1", 1, 2));
        final QuerySpec spec = specWithTimeRange(0, 100).setPvSelector(pvList("pv1")).build();
        final ResolutionResult r = resolve(spec, ExecutionOptions.newBuilder().setPageToken(token).build(), true);
        assertTrue(r.isError());
        assertTrue(r.getErrorStatus().msg.contains("streaming"));
    }

    @Test
    public void testMalformedTokenRejected() {
        final QuerySpec spec = specWithTimeRange(0, 100).setPvSelector(pvList("pv1")).build();
        final ResolutionResult r = resolve(
                spec, ExecutionOptions.newBuilder().setPageToken("!!!bad!!!").build(), false);
        assertTrue(r.isError());
        assertTrue(r.getErrorStatus().msg.contains("invalid pageToken"));
    }

    @Test
    public void testWrongKindTokenRejected() {
        // a SAMPLE token supplied to a BUCKET-mode query
        final String sampleToken = PageToken.encode(KeysetPosition.ofSample(1, 2));
        final QuerySpec spec = specWithTimeRange(0, 100).setPvSelector(pvList("pv1")).build();
        final ResolutionResult r = resolve(
                spec, ExecutionOptions.newBuilder().setPageToken(sampleToken).build(), false);
        assertTrue(r.isError());
        assertTrue(r.getErrorStatus().msg.contains("not valid for this query type"));
    }

    @Test
    public void testValidBucketTokenDecoded() {
        final KeysetPosition pos = KeysetPosition.ofBucket("pvX", 42, 7);
        final String token = PageToken.encode(pos);
        final QuerySpec spec = specWithTimeRange(0, 100).setPvSelector(pvList("pv1")).build();
        final ResolutionResult r = resolve(
                spec, ExecutionOptions.newBuilder().setPageToken(token).build(), false);
        assertFalse(r.isError());
        assertEquals(pos, r.getResolvedQuery().getPageStart());
    }

    // -----------------------------------------------------------------------
    // ConfigurationSelector resolution (Q3): malformed rejects; well-formed
    // matching nothing is an empty result, not an error
    // -----------------------------------------------------------------------

    @Test
    public void testConfigSelectorEmptyCriteriaListRejected() {
        // a present ConfigurationSelector with no criteria is a malformed request (mirrors
        // queryProviders "criteria list must not be empty"), NOT a silent "match nothing"
        final QuerySpec spec = specWithTimeRange(0, 100)
                .setPvSelector(pvList("pv1"))
                .setConfigurationSelector(ConfigurationSelector.getDefaultInstance())
                .build();
        final ResolutionResult r = resolve(spec, ExecutionOptions.getDefaultInstance(), false);
        assertTrue(r.isError());
        assertTrue(r.getErrorStatus().msg.contains("criteria list must not be empty"));
    }

    @Test
    public void testConfigSelectorUnsetCriterionArmRejected() {
        // a criterion with no arm set violates exactly-one-criterion → reject (client build error),
        // rather than being silently skipped and collapsing to an empty result
        final QuerySpec spec = specWithTimeRange(0, 100)
                .setPvSelector(pvList("pv1"))
                .setConfigurationSelector(ConfigurationSelector.newBuilder()
                        .addCriteria(ConfigurationSelector.Criterion.getDefaultInstance()))
                .build();
        final ResolutionResult r = resolve(spec, ExecutionOptions.getDefaultInstance(), false);
        assertTrue(r.isError());
        assertTrue(r.getErrorStatus().msg.contains("must set exactly one arm"));
    }

    @Test
    public void testConfigSelectorWellFormedMatchingNothingIsEmptyNotError() {
        // a well-formed selector that matches no activations in range → empty result, NOT a reject
        // (distinct from the malformed cases above). Stub returns no intervals.
        final QuerySpec spec = specWithTimeRange(0, 100)
                .setPvSelector(pvList("pv1"))
                .setConfigurationSelector(ConfigurationSelector.newBuilder()
                        .addCriteria(configNameCriterion("no-such-config")))
                .build();
        final ResolutionResult r = resolver(new IntervalStubClient(List.of())).resolve(
                spec, ExecutionOptions.getDefaultInstance(), ResultRepresentation.getDefaultInstance(),
                ResolvedQuery.ResultMode.BUCKET, false);
        assertFalse("well-formed selector matching nothing must not be an error", r.isError());
        final ResolvedQuery rq = r.getResolvedQuery();
        assertTrue(rq.getRetrievalIntervals().isEmpty());
        assertTrue(rq.isEmptyResult()); // no intervals → empty (empty payload, not exceptional)
    }

    @Test
    public void testConfigSelectorWellFormedMatchingIntervalsResolves() {
        // a well-formed selector matching activations → intervals intersected with the query range
        final QuerySpec spec = specWithTimeRange(0, 100)
                .setPvSelector(pvList("pv1"))
                .setConfigurationSelector(ConfigurationSelector.newBuilder()
                        .addCriteria(configNameCriterion("cfgA")))
                .build();
        // activation [10,50) lies within the query range [0,100) → the effective interval
        final ResolutionResult r = resolver(new IntervalStubClient(List.of(new TimeInterval(10, 0, 50, 0))))
                .resolve(spec, ExecutionOptions.getDefaultInstance(), ResultRepresentation.getDefaultInstance(),
                        ResolvedQuery.ResultMode.BUCKET, false);
        assertFalse(r.isError());
        final ResolvedQuery rq = r.getResolvedQuery();
        assertEquals(1, rq.getRetrievalIntervals().size());
        assertEquals(new TimeInterval(10, 0, 50, 0), rq.getRetrievalIntervals().get(0));
        assertFalse(rq.isEmptyResult());
    }

    // -----------------------------------------------------------------------
    // SampleStatusSelector validation: sample-oriented methods only; domain and
    // mode required; layers/statusCodes are wildcards
    // -----------------------------------------------------------------------

    private static SampleStatusSelector.Builder statusSelector(SampleStatusSelector.Mode mode) {
        return SampleStatusSelector.newBuilder().setDomain("data_quality").setMode(mode);
    }

    private ResolutionResult resolveSample(QuerySpec spec) {
        return resolver().resolve(
                spec, ExecutionOptions.getDefaultInstance(), ResultRepresentation.getDefaultInstance(),
                ResolvedQuery.ResultMode.SAMPLE, false);
    }

    @Test
    public void testStatusSelectorMissingDomainRejected() {
        final QuerySpec spec = specWithTimeRange(0, 100)
                .setPvSelector(pvList("pv1"))
                .setSampleStatusSelector(
                        statusSelector(SampleStatusSelector.Mode.MODE_INCLUDE_MATCHING).setDomain(""))
                .build();
        final ResolutionResult r = resolveSample(spec);
        assertTrue(r.isError());
        assertTrue(r.getErrorStatus().msg.contains("sampleStatusSelector.domain must be specified"));
    }

    @Test
    public void testStatusSelectorModeUnspecifiedRejected() {
        final QuerySpec spec = specWithTimeRange(0, 100)
                .setPvSelector(pvList("pv1"))
                .setSampleStatusSelector(statusSelector(SampleStatusSelector.Mode.MODE_UNSPECIFIED))
                .build();
        final ResolutionResult r = resolveSample(spec);
        assertTrue(r.isError());
        assertTrue(r.getErrorStatus().msg.contains("sampleStatusSelector.mode must be specified"));
    }

    @Test
    public void testStatusSelectorOnBucketModeRejected() {
        // bucket-oriented methods return storage buckets whole and cannot represent per-sample
        // filtering: a well-formed selector still rejects in BUCKET mode
        final QuerySpec spec = specWithTimeRange(0, 100)
                .setPvSelector(pvList("pv1"))
                .setSampleStatusSelector(statusSelector(SampleStatusSelector.Mode.MODE_INCLUDE_MATCHING))
                .build();
        final ResolutionResult r = resolve(spec, ExecutionOptions.getDefaultInstance(), false);
        assertTrue(r.isError());
        assertTrue(r.getErrorStatus().msg.contains("bucket-oriented methods do not support"));
    }

    @Test
    public void testStatusSelectorOnBucketStreamModeRejected() {
        final QuerySpec spec = specWithTimeRange(0, 100)
                .setPvSelector(pvList("pv1"))
                .setSampleStatusSelector(statusSelector(SampleStatusSelector.Mode.MODE_EXCLUDE_MATCHING))
                .build();
        final ResolutionResult r = resolve(spec, ExecutionOptions.getDefaultInstance(), true);
        assertTrue(r.isError());
        assertTrue(r.getErrorStatus().msg.contains("bucket-oriented methods do not support"));
    }

    @Test
    public void testStatusSelectorWellFormedResolves() {
        final QuerySpec spec = specWithTimeRange(0, 100)
                .setPvSelector(pvList("pv1"))
                .setSampleStatusSelector(statusSelector(SampleStatusSelector.Mode.MODE_INCLUDE_MATCHING)
                        .addLayers("ml_model_v1")
                        .addStatusCodes(3)
                        .addStatusCodes(7))
                .build();
        final ResolutionResult r = resolveSample(spec);
        assertFalse(r.isError());
        final ResolvedStatusFilter filter = r.getResolvedQuery().getStatusFilter();
        assertNotNull(filter);
        assertEquals("data_quality", filter.domain());
        assertEquals(List.of("ml_model_v1"), filter.layers());
        assertTrue(filter.includeMode());
        assertTrue(filter.matchesCode(3));
        assertTrue(filter.matchesCode(7));
        assertFalse(filter.matchesCode(4));
    }

    @Test
    public void testStatusSelectorEmptyCodesMatchesAnyCode() {
        // empty statusCodes = any code ("labeled at all"); empty layers = all layers in the domain
        final QuerySpec spec = specWithTimeRange(0, 100)
                .setPvSelector(pvList("pv1"))
                .setSampleStatusSelector(statusSelector(SampleStatusSelector.Mode.MODE_EXCLUDE_MATCHING))
                .build();
        final ResolutionResult r = resolveSample(spec);
        assertFalse(r.isError());
        final ResolvedStatusFilter filter = r.getResolvedQuery().getStatusFilter();
        assertNotNull(filter);
        assertTrue(filter.layers().isEmpty());
        assertFalse(filter.includeMode());
        assertTrue(filter.matchesCode(0));
        assertTrue(filter.matchesCode(-42));
    }

    @Test
    public void testNoStatusSelectorResolvesWithNullFilter() {
        final QuerySpec spec = specWithTimeRange(0, 100)
                .setPvSelector(pvList("pv1"))
                .build();
        final ResolutionResult r = resolveSample(spec);
        assertFalse(r.isError());
        assertNull(r.getResolvedQuery().getStatusFilter());
    }
}
