package com.ospreydcs.dp.service.query.handler.mongo;

import com.mongodb.client.result.InsertManyResult;
import com.ospreydcs.dp.grpc.v1.common.ColumnMetadata;
import com.ospreydcs.dp.grpc.v1.common.DataBucket;
import com.ospreydcs.dp.grpc.v1.common.DataColumn;
import com.ospreydcs.dp.grpc.v1.common.DataValue;
import com.ospreydcs.dp.grpc.v1.common.SamplingClock;
import com.ospreydcs.dp.grpc.v1.common.TimeRange;
import com.ospreydcs.dp.grpc.v1.common.Timestamp;
import com.ospreydcs.dp.grpc.v1.query.ExecutionOptions;
import com.ospreydcs.dp.grpc.v1.query.PvNameList;
import com.ospreydcs.dp.grpc.v1.query.PvSelector;
import com.ospreydcs.dp.grpc.v1.query.QueryBucketsRequest;
import com.ospreydcs.dp.grpc.v1.query.QueryBucketsResponse;
import com.ospreydcs.dp.grpc.v1.query.QuerySpec;
import com.ospreydcs.dp.grpc.v1.query.ResultRepresentation;
import com.ospreydcs.dp.service.common.bson.bucket.BucketDocument;
import com.ospreydcs.dp.service.common.bson.bucket.BucketUtility;
import com.ospreydcs.dp.service.common.bson.DataTimestampsDocument;
import com.ospreydcs.dp.service.common.bson.column.DataColumnDocument;
import com.ospreydcs.dp.service.common.mongo.MongoTestClient;
import com.ospreydcs.dp.service.common.protobuf.TimestampUtility;
import com.ospreydcs.dp.service.query.handler.QueryV2Resolver;
import com.ospreydcs.dp.service.query.handler.model.ResolutionResult;
import com.ospreydcs.dp.service.query.handler.model.ResolvedQuery;
import com.ospreydcs.dp.service.query.handler.mongo.client.MongoSyncQueryClient;
import com.ospreydcs.dp.service.query.handler.mongo.dispatch.QueryBucketsUnaryDispatcher;
import com.ospreydcs.dp.service.query.handler.mongo.job.QueryV2Job;
import io.grpc.stub.StreamObserver;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.*;

/**
 * End-to-end tests for Query API V2 {@code queryBuckets} unary against real MongoDB: resolution
 * (step 2) + keyset paging (Q2) + {@code $or} fragmentation (Q3) + representation flags (Q5/Q8),
 * driven through the shared {@link QueryV2Job} and {@link QueryBucketsUnaryDispatcher}.
 *
 * <p>Seeded data (from {@link MongoQueryHandlerTestBase}): 5 PVs {@code testpv_1..5}, each with 10
 * one-second buckets at seconds {@code [startSeconds .. startSeconds+9]}, 10 samples/bucket.
 */
public class MongoSyncQueryBucketsV2Test extends MongoQueryHandlerTestBase {

    private static final int DEFAULT_PAGE_SIZE = 10_000;
    private static final int MAX_PAGE_SIZE = 100_000;
    private static final int MAX_RESOLVED_PVS = 10_000;

    // a PV carrying column metadata, seeded separately to exercise excludeColumnMetadata
    private static final String META_PV_NAME = "metapv_1";

    protected static class TestSyncClient extends MongoSyncQueryClient implements TestClientInterface {
        @Override
        protected String getCollectionNameBuckets() {
            return getTestCollectionNameBuckets();
        }

        @Override
        protected String getCollectionNameRequestStatus() {
            return getTestCollectionNameRequestStatus();
        }

        public int insertBucketDocuments(List<BucketDocument> documentList) {
            InsertManyResult result = mongoCollectionBuckets.insertMany(documentList);
            return result.getInsertedIds().size();
        }
    }

    @BeforeClass
    public static void setUp() throws Exception {
        MongoTestClient.prepareTestDatabase();
        TestSyncClient testClient = new TestSyncClient();
        MongoQueryHandler handler = new MongoQueryHandler(testClient);
        setUp(handler, testClient);

        // seed one extra bucket for META_PV_NAME carrying ColumnMetadata (the base's buckets have none)
        final List<BucketDocument> metaBuckets = new ArrayList<>();
        metaBuckets.add(buildMetadataBucket(startSeconds));
        assertEquals(metaBuckets.size(),
                ((TestClientInterface) clientTestInterface).insertBucketDocuments(metaBuckets));
    }

    @AfterClass
    public static void tearDown() throws Exception {
        MongoQueryHandlerTestBase.tearDown();
    }

    // -----------------------------------------------------------------------
    // helpers
    // -----------------------------------------------------------------------

    private static BucketDocument buildMetadataBucket(long firstSeconds) {
        final BucketDocument bucket = new BucketDocument();
        bucket.setId(META_PV_NAME + "-" + firstSeconds + "-0");
        bucket.setPvName(META_PV_NAME);

        // attach column metadata IN the DataColumn so it is embedded in the stored bytes — the legacy
        // toDataColumn() path (used by addColumnToBucket) emits exactly what is in the bytes.
        final ColumnMetadata columnMetadata = ColumnMetadata.newBuilder()
                .addTags("beamline")
                .build();
        final DataColumn.Builder dataColumnBuilder = DataColumn.newBuilder()
                .setName(META_PV_NAME)
                .setMetadata(columnMetadata);
        for (int i = 0; i < 10; i++) {
            dataColumnBuilder.addDataValues(DataValue.newBuilder().setDoubleValue(i).build());
        }
        final DataColumnDocument columnDocument = DataColumnDocument.fromDataColumn(dataColumnBuilder.build());
        bucket.setDataColumn(columnDocument);

        final Timestamp startTime = TimestampUtility.timestampFromSeconds(firstSeconds, 0);
        final SamplingClock samplingClock = SamplingClock.newBuilder()
                .setStartTime(startTime).setPeriodNanos(100_000_000L).setCount(10).build();
        bucket.setDataTimestamps(DataTimestampsDocument.fromDataTimestamps(
                com.ospreydcs.dp.grpc.v1.common.DataTimestamps.newBuilder()
                        .setSamplingClock(samplingClock).build()));
        return bucket;
    }

    private QueryV2Resolver resolver() {
        return new QueryV2Resolver(
                clientTestInterface, DEFAULT_PAGE_SIZE, MAX_PAGE_SIZE, MAX_RESOLVED_PVS);
    }

    private static Timestamp ts(long secs, long nanos) {
        return Timestamp.newBuilder().setEpochSeconds(secs).setNanoseconds(nanos).build();
    }

    private static QueryBucketsRequest bucketsRequest(
            List<String> pvNames, long beginSecs, long endSecs,
            int limit, String pageToken, boolean excludeMetadata) {

        final QuerySpec.Builder spec = QuerySpec.newBuilder()
                .setTimeRange(TimeRange.newBuilder().setBeginTime(ts(beginSecs, 0)).setEndTime(ts(endSecs, 0)))
                .setPvSelector(PvSelector.newBuilder()
                        .setPvNameList(PvNameList.newBuilder().addAllPvNames(pvNames)));

        final ExecutionOptions.Builder opts = ExecutionOptions.newBuilder().setLimit(limit);
        if (pageToken != null) {
            opts.setPageToken(pageToken);
        }

        return QueryBucketsRequest.newBuilder()
                .setQuerySpec(spec)
                .setExecutionOptions(opts)
                .setResultRepresentation(ResultRepresentation.newBuilder()
                        .setExcludeColumnMetadata(excludeMetadata))
                .build();
    }

    /** Resolve + run the job synchronously; return the single collected response. */
    private QueryBucketsResponse runQueryBuckets(QueryBucketsRequest request) {
        final ResolutionResult resolution = resolver().resolve(
                request.getQuerySpec(), request.getExecutionOptions(), request.getResultRepresentation(),
                ResolvedQuery.ResultMode.BUCKET, false);
        assertFalse("unexpected resolution error: "
                + (resolution.isError() ? resolution.getErrorStatus().msg : ""), resolution.isError());
        return runResolved(resolution.getResolvedQuery());
    }

    private QueryBucketsResponse runResolved(ResolvedQuery resolvedQuery) {
        return runResolvedWithBudget(resolvedQuery, Long.MAX_VALUE);
    }

    private QueryBucketsResponse runResolvedWithBudget(ResolvedQuery resolvedQuery, long byteBudget) {
        final List<QueryBucketsResponse> responses = new ArrayList<>();
        final StreamObserver<QueryBucketsResponse> observer = new StreamObserver<>() {
            @Override public void onNext(QueryBucketsResponse r) { responses.add(r); }
            @Override public void onError(Throwable t) { }
            @Override public void onCompleted() { }
        };
        final QueryBucketsUnaryDispatcher dispatcher =
                new QueryBucketsUnaryDispatcher(observer, byteBudget);
        new QueryV2Job(resolvedQuery, dispatcher, clientTestInterface).execute();
        assertEquals("expected exactly one unary response", 1, responses.size());
        return responses.get(0);
    }

    private ResolvedQuery resolvedForPv(String pvName, int limit, String pageToken) {
        final QueryBucketsRequest request =
                bucketsRequest(List.of(pvName), startSeconds, startSeconds + 10, limit, pageToken, false);
        final ResolutionResult resolution = resolve(request);
        assertFalse(resolution.isError());
        return resolution.getResolvedQuery();
    }

    /** Resolve, returning the ResolutionResult (for error-path assertions). */
    private ResolutionResult resolve(QueryBucketsRequest request) {
        return resolver().resolve(
                request.getQuerySpec(), request.getExecutionOptions(), request.getResultRepresentation(),
                ResolvedQuery.ResultMode.BUCKET, false);
    }

    // -----------------------------------------------------------------------
    // happy path
    // -----------------------------------------------------------------------

    @Test
    public void testSinglePvWholeRange() {
        // testpv_1 has 10 buckets across [startSeconds, startSeconds+10)
        final QueryBucketsResponse response = runQueryBuckets(
                bucketsRequest(List.of(COL_1_NAME), startSeconds, startSeconds + 10, 0, null, false));

        assertTrue(response.hasBucketQueryResult());
        final QueryBucketsResponse.BucketQueryResult result = response.getBucketQueryResult();
        assertEquals(10, result.getDataBucketsCount());
        assertTrue("single full page => empty nextPageToken", result.getNextPageToken().isEmpty());
        // all buckets belong to testpv_1, sorted by firstTime
        long expectedSecond = startSeconds;
        for (DataBucket b : result.getDataBucketsList()) {
            assertEquals(COL_1_NAME, b.getPvName());
            assertEquals(expectedSecond, b.getDataTimestamps().getSamplingClock().getStartTime().getEpochSeconds());
            expectedSecond++;
        }
    }

    @Test
    public void testWholeBoundaryBucketsNotTrimmed() {
        // a narrow window inside one bucket still returns that WHOLE bucket (10 samples), no trimming
        final QueryBucketsResponse response = runQueryBuckets(
                bucketsRequest(List.of(COL_1_NAME), startSeconds, startSeconds + 1, 0, null, false));
        final QueryBucketsResponse.BucketQueryResult result = response.getBucketQueryResult();
        assertEquals(1, result.getDataBucketsCount());
        assertEquals(10, result.getDataBuckets(0).getDataValues().getDataColumn().getDataValuesCount());
    }

    @Test
    public void testMultiplePvsSortedByPvThenTime() {
        final QueryBucketsResponse response = runQueryBuckets(
                bucketsRequest(List.of(COL_1_NAME, COL_2_NAME), startSeconds, startSeconds + 10, 0, null, false));
        final QueryBucketsResponse.BucketQueryResult result = response.getBucketQueryResult();
        assertEquals(20, result.getDataBucketsCount());
        // first 10 are testpv_1, next 10 testpv_2 (sort is pvName, then firstTime)
        for (int i = 0; i < 10; i++) {
            assertEquals(COL_1_NAME, result.getDataBuckets(i).getPvName());
        }
        for (int i = 10; i < 20; i++) {
            assertEquals(COL_2_NAME, result.getDataBuckets(i).getPvName());
        }
    }

    @Test
    public void testEmptyResultIsEmptyPayloadNotExceptional() {
        // query a time window with no data (well before seeded data)
        final QueryBucketsResponse response = runQueryBuckets(
                bucketsRequest(List.of(COL_1_NAME), startSeconds - 1000, startSeconds - 900, 0, null, false));
        assertFalse("empty result must NOT be an ExceptionalResult", response.hasExceptionalResult());
        assertTrue(response.hasBucketQueryResult());
        assertEquals(0, response.getBucketQueryResult().getDataBucketsCount());
        assertTrue(response.getBucketQueryResult().getNextPageToken().isEmpty());
    }

    // -----------------------------------------------------------------------
    // keyset paging (Q2)
    // -----------------------------------------------------------------------

    @Test
    public void testKeysetPagingAcrossMultiplePages() {
        // page through testpv_1's 10 buckets, 4 per page => pages of 4,4,2
        final List<DataBucket> collected = new ArrayList<>();
        String token = null;
        int pages = 0;
        while (true) {
            final QueryBucketsResponse response = runQueryBuckets(
                    bucketsRequest(List.of(COL_1_NAME), startSeconds, startSeconds + 10, 4, token, false));
            final QueryBucketsResponse.BucketQueryResult result = response.getBucketQueryResult();
            collected.addAll(result.getDataBucketsList());
            pages++;
            token = result.getNextPageToken();
            if (token.isEmpty()) {
                break;
            }
            assertTrue("page must not exceed limit", result.getDataBucketsCount() <= 4);
            assertTrue("safety: too many pages", pages < 10);
        }
        assertEquals(3, pages);
        assertEquals(10, collected.size());
        // no duplicates / no gaps: firstTime seconds strictly increasing across the full sequence
        long expectedSecond = startSeconds;
        for (DataBucket b : collected) {
            assertEquals(expectedSecond, b.getDataTimestamps().getSamplingClock().getStartTime().getEpochSeconds());
            expectedSecond++;
        }
    }

    @Test
    public void testLastPageHasEmptyToken() {
        // exactly 10 buckets with limit 10 => one full page, no following page
        final QueryBucketsResponse response = runQueryBuckets(
                bucketsRequest(List.of(COL_1_NAME), startSeconds, startSeconds + 10, 10, null, false));
        final QueryBucketsResponse.BucketQueryResult result = response.getBucketQueryResult();
        assertEquals(10, result.getDataBucketsCount());
        assertTrue(result.getNextPageToken().isEmpty());
    }

    @Test
    public void testKeysetPagingSpansPvBoundary() {
        // 2 PVs x 10 buckets = 20; page size 7 => 7,7,6 crossing the pv boundary at index 10
        final List<DataBucket> collected = new ArrayList<>();
        String token = null;
        int pages = 0;
        while (true) {
            final QueryBucketsResponse response = runQueryBuckets(
                    bucketsRequest(List.of(COL_1_NAME, COL_2_NAME), startSeconds, startSeconds + 10, 7, token, false));
            final QueryBucketsResponse.BucketQueryResult result = response.getBucketQueryResult();
            collected.addAll(result.getDataBucketsList());
            pages++;
            token = result.getNextPageToken();
            if (token.isEmpty() || pages >= 10) {
                break;
            }
        }
        assertEquals(3, pages);
        assertEquals(20, collected.size());
        for (int i = 0; i < 10; i++) {
            assertEquals(COL_1_NAME, collected.get(i).getPvName());
        }
        for (int i = 10; i < 20; i++) {
            assertEquals(COL_2_NAME, collected.get(i).getPvName());
        }
    }

    // -----------------------------------------------------------------------
    // $or fragmentation (Q3) — exercised via multiple resolved intervals
    // -----------------------------------------------------------------------

    @Test
    public void testFragmentedIntervalsOrFilter() {
        // build a ResolvedQuery directly with two disjoint intervals: [start,start+2) and [start+5,start+7)
        // expect buckets at seconds start, start+1, start+5, start+6 (4 buckets)
        final ResolvedQuery resolved = new ResolvedQuery(
                List.of(COL_1_NAME),
                List.of(
                        new com.ospreydcs.dp.service.query.handler.model.TimeInterval(startSeconds, 0, startSeconds + 2, 0),
                        new com.ospreydcs.dp.service.query.handler.model.TimeInterval(startSeconds + 5, 0, startSeconds + 7, 0)),
                DEFAULT_PAGE_SIZE, null, false, false,
                ResolvedQuery.ResultMode.BUCKET, false);

        final QueryBucketsResponse response = runResolved(resolved);
        final QueryBucketsResponse.BucketQueryResult result = response.getBucketQueryResult();
        assertEquals(4, result.getDataBucketsCount());
        final List<Long> seconds = new ArrayList<>();
        for (DataBucket b : result.getDataBucketsList()) {
            seconds.add(b.getDataTimestamps().getSamplingClock().getStartTime().getEpochSeconds());
        }
        assertEquals(List.of(startSeconds, startSeconds + 1, startSeconds + 5, startSeconds + 6), seconds);
    }

    // -----------------------------------------------------------------------
    // representation flags
    // -----------------------------------------------------------------------

    @Test
    public void testExcludeColumnMetadataDefaultIncludes() {
        final QueryBucketsResponse response = runQueryBuckets(
                bucketsRequest(List.of(META_PV_NAME), startSeconds, startSeconds + 1, 0, null, false));
        final DataBucket bucket = response.getBucketQueryResult().getDataBuckets(0);
        assertTrue("default should include column metadata",
                bucket.getDataValues().getDataColumn().hasMetadata());
        assertTrue(bucket.getDataValues().getDataColumn().getMetadata().getTagsList().contains("beamline"));
    }

    @Test
    public void testExcludeColumnMetadataSuppresses() {
        final QueryBucketsResponse response = runQueryBuckets(
                bucketsRequest(List.of(META_PV_NAME), startSeconds, startSeconds + 1, 0, null, true));
        final DataBucket bucket = response.getBucketQueryResult().getDataBuckets(0);
        assertFalse("excludeColumnMetadata=true should suppress column metadata",
                bucket.getDataValues().getDataColumn().hasMetadata());
    }

    // -----------------------------------------------------------------------
    // byte-budget page boundary + indivisible-oversized (Q7)
    // -----------------------------------------------------------------------

    @Test
    public void testByteBudgetSplitsPageAndResumes() {
        // Determine one bucket's serialized size, then set a budget that fits ~1 bucket per page so
        // the byte guard (not the count limit) ends each page. Count limit is high (100) so it never
        // fires; the split is driven purely by the byte budget.
        final long oneBucketBytes = firstBucketSerializedSize(COL_1_NAME);
        // budget large enough for exactly one bucket but not two
        final long budget = oneBucketBytes + (oneBucketBytes / 2);

        final List<DataBucket> collected = new ArrayList<>();
        String token = null;
        int pages = 0;
        while (true) {
            final ResolvedQuery resolved = resolvedForPv(COL_1_NAME, 100, token);
            final QueryBucketsResponse response = runResolvedWithBudget(resolved, budget);
            final QueryBucketsResponse.BucketQueryResult result = response.getBucketQueryResult();
            assertFalse(response.hasExceptionalResult());
            assertEquals("byte budget should limit each page to one bucket",
                    1, result.getDataBucketsCount());
            collected.add(result.getDataBuckets(0));
            pages++;
            token = result.getNextPageToken();
            if (token.isEmpty() || pages >= 20) {
                break;
            }
        }
        assertEquals("10 buckets, ~1 per page", 10, pages);
        assertEquals(10, collected.size());
        // strictly increasing firstTime => no dup / no gap across the byte-split boundaries
        long expectedSecond = startSeconds;
        for (DataBucket b : collected) {
            assertEquals(expectedSecond, b.getDataTimestamps().getSamplingClock().getStartTime().getEpochSeconds());
            expectedSecond++;
        }
    }

    @Test
    public void testIndivisibleOversizedBucketErrors() {
        // a budget smaller than a single bucket cannot page out of the first bucket => error
        final long oneBucketBytes = firstBucketSerializedSize(COL_1_NAME);
        final long tinyBudget = Math.max(1, oneBucketBytes - 1);

        final ResolvedQuery resolved = resolvedForPv(COL_1_NAME, 100, null);
        final QueryBucketsResponse response = runResolvedWithBudget(resolved, tinyBudget);
        assertTrue("single oversized bucket must be an ExceptionalResult", response.hasExceptionalResult());
        assertEquals(com.ospreydcs.dp.grpc.v1.common.ExceptionalResult.ExceptionalResultStatus.RESULT_STATUS_ERROR,
                response.getExceptionalResult().getExceptionalResultStatus());
    }

    /** Serialized size of the first emitted bucket for a PV (matches the dispatcher's measurement). */
    private long firstBucketSerializedSize(String pvName) {
        final ResolvedQuery resolved = resolvedForPv(pvName, 100, null);
        final QueryBucketsResponse response = runResolvedWithBudget(resolved, Long.MAX_VALUE);
        return response.getBucketQueryResult().getDataBuckets(0).getSerializedSize();
    }

    // -----------------------------------------------------------------------
    // §6 validation rejections
    // -----------------------------------------------------------------------

    @Test
    public void testMissingTimeRangeRejected() {
        final QueryBucketsRequest request = QueryBucketsRequest.newBuilder()
                .setQuerySpec(QuerySpec.newBuilder()
                        .setPvSelector(PvSelector.newBuilder()
                                .setPvNameList(PvNameList.newBuilder().addPvNames(COL_1_NAME))))
                .build();
        final ResolutionResult r = resolve(request);
        assertTrue(r.isError());
        assertTrue(r.getErrorStatus().msg.contains("timeRange"));
    }

    @Test
    public void testMissingPvSelectorRejected() {
        final QueryBucketsRequest request = QueryBucketsRequest.newBuilder()
                .setQuerySpec(QuerySpec.newBuilder()
                        .setTimeRange(TimeRange.newBuilder()
                                .setBeginTime(ts(startSeconds, 0)).setEndTime(ts(startSeconds + 1, 0))))
                .build();
        final ResolutionResult r = resolve(request);
        assertTrue(r.isError());
        assertTrue(r.getErrorStatus().msg.contains("pvSelector"));
    }
}
