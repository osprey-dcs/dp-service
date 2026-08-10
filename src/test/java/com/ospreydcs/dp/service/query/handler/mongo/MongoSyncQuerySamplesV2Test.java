package com.ospreydcs.dp.service.query.handler.mongo;

import com.mongodb.client.result.InsertManyResult;
import com.ospreydcs.dp.grpc.v1.common.ArrayDimensions;
import com.ospreydcs.dp.grpc.v1.common.DataColumn;
import com.ospreydcs.dp.grpc.v1.common.DataTimestamps;
import com.ospreydcs.dp.grpc.v1.common.DataValue;
import com.ospreydcs.dp.grpc.v1.common.DoubleArrayColumn;
import com.ospreydcs.dp.grpc.v1.common.SamplingClock;
import com.ospreydcs.dp.grpc.v1.common.TimeRange;
import com.ospreydcs.dp.grpc.v1.common.Timestamp;
import com.ospreydcs.dp.grpc.v1.query.ColumnTable;
import com.ospreydcs.dp.grpc.v1.query.ExecutionOptions;
import com.ospreydcs.dp.grpc.v1.query.PvNameList;
import com.ospreydcs.dp.grpc.v1.query.PvSelector;
import com.ospreydcs.dp.grpc.v1.query.QuerySamplesRequest;
import com.ospreydcs.dp.grpc.v1.query.QuerySamplesResponse;
import com.ospreydcs.dp.grpc.v1.query.QuerySpec;
import com.ospreydcs.dp.grpc.v1.query.ResultRepresentation;
import com.ospreydcs.dp.service.common.bson.DataTimestampsDocument;
import com.ospreydcs.dp.service.common.bson.bucket.BucketDocument;
import com.ospreydcs.dp.service.common.bson.column.DataColumnDocument;
import com.ospreydcs.dp.service.common.bson.column.DoubleArrayColumnDocument;
import com.ospreydcs.dp.service.common.exception.DpException;
import com.ospreydcs.dp.service.common.mongo.MongoTestClient;
import com.ospreydcs.dp.service.common.protobuf.TimestampUtility;
import com.ospreydcs.dp.service.query.handler.QueryV2Resolver;
import com.ospreydcs.dp.service.query.handler.model.ResolutionResult;
import com.ospreydcs.dp.service.query.handler.model.ResolvedQuery;
import com.ospreydcs.dp.service.query.handler.model.TimeInterval;
import com.ospreydcs.dp.service.query.handler.mongo.client.MongoSyncQueryClient;
import com.ospreydcs.dp.service.query.handler.mongo.dispatch.QuerySamplesUnaryDispatcher;
import com.ospreydcs.dp.service.query.handler.mongo.job.QueryV2Job;
import io.grpc.stub.StreamObserver;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.*;

/**
 * End-to-end tests for Query API V2 {@code querySamples} unary against real MongoDB: union-axis
 * alignment with multi-rate PVs + missing values, half-open trimming, timestamp-advanced paging
 * (Q1) with a seam invariant (no dropped/duplicated timestamp), column seeding (Q9), non-scalar
 * reject (Q4), and useSerializedColumns (Q5).
 *
 * <p>Seeds its own PVs on a time base well after the base class's data:
 * <ul>
 *   <li>{@code spv_a}: 10 Hz (100ms period), seconds [B, B+3) → nanos 0,1e8..9e8 each second</li>
 *   <li>{@code spv_b}: 5 Hz (200ms period), seconds [B, B+3) → nanos 0,2e8,4e8,6e8,8e8 each second</li>
 *   <li>{@code sarr}: a non-scalar double-array PV at second B (for the Q4 reject)</li>
 * </ul>
 */
public class MongoSyncQuerySamplesV2Test extends MongoQueryHandlerTestBase {

    private static final int DEFAULT_PAGE_SIZE = 10_000;
    private static final int MAX_PAGE_SIZE = 100_000;
    private static final int MAX_RESOLVED_PVS = 10_000;

    private static final long B = startSeconds + 100_000L; // isolated time base
    private static final int NUM_SECONDS = 3;
    private static final String PV_A = "spv_a"; // 10 Hz
    private static final String PV_B = "spv_b"; // 5 Hz
    private static final String PV_ARR = "sarr"; // non-scalar

    // #207 fragment-gap fixture, on its own time base clear of the other PVs
    private static final String PV_SPAN = "spanpv";
    private static final long SPAN_B = startSeconds + 200_000L;
    private static final int SPAN_SECONDS = 10;

    protected static class TestSyncClient extends MongoSyncQueryClient implements TestClientInterface {
        @Override protected String getCollectionNameBuckets() { return getTestCollectionNameBuckets(); }
        @Override protected String getCollectionNameRequestStatus() { return getTestCollectionNameRequestStatus(); }
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

        final List<BucketDocument> buckets = new ArrayList<>();
        for (int s = 0; s < NUM_SECONDS; s++) {
            buckets.add(scalarBucket(PV_A, B + s, 100_000_000L, 10)); // 10 Hz
            buckets.add(scalarBucket(PV_B, B + s, 200_000_000L, 5));  // 5 Hz
        }
        buckets.add(arrayBucket(PV_ARR, B));
        // #207 fixture: ONE bucket covering the whole span, at 1 Hz. Deliberately a single bucket --
        // the same data stored as per-second buckets is filtered correctly by the database alone and
        // would not exercise the sample-level fragment trim.
        buckets.add(spanningBucket(PV_SPAN, SPAN_B, SPAN_SECONDS));
        assertEquals(buckets.size(),
                ((TestClientInterface) clientTestInterface).insertBucketDocuments(buckets));
    }

    @AfterClass
    public static void tearDown() throws Exception {
        MongoQueryHandlerTestBase.tearDown();
    }

    // -----------------------------------------------------------------------
    // seeding helpers
    // -----------------------------------------------------------------------

    private static BucketDocument scalarBucket(String pvName, long second, long periodNanos, int count) {
        final BucketDocument bucket = new BucketDocument();
        bucket.setId(pvName + "-" + second + "-0");
        bucket.setPvName(pvName);

        final DataColumn.Builder columnBuilder = DataColumn.newBuilder().setName(pvName);
        for (int i = 0; i < count; i++) {
            columnBuilder.addDataValues(DataValue.newBuilder().setDoubleValue(second * 1000 + i).build());
        }
        bucket.setDataColumn(DataColumnDocument.fromDataColumn(columnBuilder.build()));

        final SamplingClock clock = SamplingClock.newBuilder()
                .setStartTime(TimestampUtility.timestampFromSeconds(second, 0))
                .setPeriodNanos(periodNanos)
                .setCount(count)
                .build();
        bucket.setDataTimestamps(DataTimestampsDocument.fromDataTimestamps(
                DataTimestamps.newBuilder().setSamplingClock(clock).build()));
        return bucket;
    }

    private static BucketDocument arrayBucket(String pvName, long second) throws DpException {
        final BucketDocument bucket = new BucketDocument();
        bucket.setId(pvName + "-" + second + "-0");
        bucket.setPvName(pvName);

        // one sample of a length-2 double array
        final DoubleArrayColumn arrayColumn = DoubleArrayColumn.newBuilder()
                .setName(pvName)
                .setDimensions(ArrayDimensions.newBuilder().addDims(2))
                .addValues(1.0).addValues(2.0)
                .build();
        bucket.setDataColumn(DoubleArrayColumnDocument.fromDoubleArrayColumn(arrayColumn));

        final SamplingClock clock = SamplingClock.newBuilder()
                .setStartTime(TimestampUtility.timestampFromSeconds(second, 0))
                .setPeriodNanos(100_000_000L)
                .setCount(1)
                .build();
        bucket.setDataTimestamps(DataTimestampsDocument.fromDataTimestamps(
                DataTimestamps.newBuilder().setSamplingClock(clock).build()));
        return bucket;
    }

    /** A single bucket holding {@code count} 1 Hz samples starting at {@code startSecond}. */
    private static BucketDocument spanningBucket(String pvName, long startSecond, int count) {
        final BucketDocument bucket = new BucketDocument();
        bucket.setId(pvName + "-span-" + startSecond);
        bucket.setPvName(pvName);

        final DataColumn.Builder columnBuilder = DataColumn.newBuilder().setName(pvName);
        for (int i = 0; i < count; i++) {
            columnBuilder.addDataValues(DataValue.newBuilder().setDoubleValue(i).build());
        }
        bucket.setDataColumn(DataColumnDocument.fromDataColumn(columnBuilder.build()));

        final SamplingClock clock = SamplingClock.newBuilder()
                .setStartTime(TimestampUtility.timestampFromSeconds(startSecond, 0))
                .setPeriodNanos(1_000_000_000L)
                .setCount(count)
                .build();
        bucket.setDataTimestamps(DataTimestampsDocument.fromDataTimestamps(
                DataTimestamps.newBuilder().setSamplingClock(clock).build()));
        return bucket;
    }

    // -----------------------------------------------------------------------
    // request + run helpers
    // -----------------------------------------------------------------------

    private QueryV2Resolver resolver() {
        return new QueryV2Resolver(clientTestInterface, DEFAULT_PAGE_SIZE, MAX_PAGE_SIZE, MAX_RESOLVED_PVS);
    }

    private static Timestamp ts(long secs, long nanos) {
        return Timestamp.newBuilder().setEpochSeconds(secs).setNanoseconds(nanos).build();
    }

    private static QuerySamplesRequest samplesRequest(
            List<String> pvNames, long beginSecs, long beginNanos, long endSecs, long endNanos,
            int limit, String pageToken, boolean useSerialized) {

        final QuerySpec.Builder spec = QuerySpec.newBuilder()
                .setTimeRange(TimeRange.newBuilder()
                        .setBeginTime(ts(beginSecs, beginNanos)).setEndTime(ts(endSecs, endNanos)))
                .setPvSelector(PvSelector.newBuilder()
                        .setPvNameList(PvNameList.newBuilder().addAllPvNames(pvNames)));

        final ExecutionOptions.Builder opts = ExecutionOptions.newBuilder().setLimit(limit);
        if (pageToken != null) {
            opts.setPageToken(pageToken);
        }

        return QuerySamplesRequest.newBuilder()
                .setQuerySpec(spec)
                .setExecutionOptions(opts)
                .setResultRepresentation(ResultRepresentation.newBuilder().setUseSerializedColumns(useSerialized))
                .build();
    }

    private ResolutionResult resolve(QuerySamplesRequest request) {
        return resolver().resolve(
                request.getQuerySpec(), request.getExecutionOptions(), request.getResultRepresentation(),
                ResolvedQuery.ResultMode.SAMPLE, false);
    }

    private QuerySamplesResponse runSamples(QuerySamplesRequest request) {
        return runSamplesWithBudget(request, Long.MAX_VALUE);
    }

    private QuerySamplesResponse runSamplesWithBudget(QuerySamplesRequest request, long byteBudget) {
        final ResolutionResult resolution = resolve(request);
        assertFalse("unexpected resolution error: "
                + (resolution.isError() ? resolution.getErrorStatus().msg : ""), resolution.isError());

        final List<QuerySamplesResponse> responses = new ArrayList<>();
        final StreamObserver<QuerySamplesResponse> observer = new StreamObserver<>() {
            @Override public void onNext(QuerySamplesResponse r) { responses.add(r); }
            @Override public void onError(Throwable t) { }
            @Override public void onCompleted() { }
        };
        final QuerySamplesUnaryDispatcher dispatcher = new QuerySamplesUnaryDispatcher(observer, byteBudget);
        new QueryV2Job(resolution.getResolvedQuery(), dispatcher, clientTestInterface).execute();
        assertEquals("expected exactly one unary response", 1, responses.size());
        return responses.get(0);
    }

    // ---- streaming helpers ----

    private static final class StreamOutcome {
        final List<QuerySamplesResponse> messages = new ArrayList<>();
        boolean completed = false;
        boolean errored = false;
    }

    private StreamOutcome runSamplesStream(QuerySamplesRequest request, long byteBudget) {
        final ResolutionResult resolution = resolver().resolve(
                request.getQuerySpec(), request.getExecutionOptions(), request.getResultRepresentation(),
                ResolvedQuery.ResultMode.SAMPLE, true /* streaming */);
        assertFalse("unexpected streaming resolution error: "
                + (resolution.isError() ? resolution.getErrorStatus().msg : ""), resolution.isError());

        final StreamOutcome outcome = new StreamOutcome();
        final StreamObserver<QuerySamplesResponse> observer = new StreamObserver<>() {
            @Override public void onNext(QuerySamplesResponse r) { outcome.messages.add(r); }
            @Override public void onError(Throwable t) { outcome.errored = true; }
            @Override public void onCompleted() { outcome.completed = true; }
        };
        final com.ospreydcs.dp.service.query.handler.mongo.dispatch.QuerySamplesStreamDispatcher dispatcher =
                new com.ospreydcs.dp.service.query.handler.mongo.dispatch.QuerySamplesStreamDispatcher(
                        observer, byteBudget);
        new QueryV2Job(resolution.getResolvedQuery(), dispatcher, clientTestInterface).execute();
        return outcome;
    }

    private static DataColumn columnByName(ColumnTable table, String pvName) {
        for (DataColumn c : table.getDataColumnsList()) {
            if (c.getName().equals(pvName)) {
                return c;
            }
        }
        fail("column not found: " + pvName);
        return null;
    }

    // -----------------------------------------------------------------------
    // union axis + missing values + column seeding
    // -----------------------------------------------------------------------

    @Test
    public void testUnionAxisMultiRateWithMissingValues() {
        // one second window [B, B+1): spv_a has 10 rows, spv_b has 5 (present at even nanos)
        final QuerySamplesResponse response =
                runSamples(samplesRequest(List.of(PV_A, PV_B), B, 0, B + 1, 0, 0, null, false));

        assertTrue(response.hasSampleQueryResult());
        final ColumnTable table = response.getSampleQueryResult().getColumnTable();

        assertEquals("union axis = spv_a's 10 timestamps", 10, table.getTimestampList().getTimestampsCount());
        // both resolved PVs get a column, sorted by name (Q9)
        assertEquals(2, table.getDataColumnsCount());
        assertEquals(PV_A, table.getDataColumns(0).getName());
        assertEquals(PV_B, table.getDataColumns(1).getName());

        final DataColumn colB = columnByName(table, PV_B);
        assertEquals(10, colB.getDataValuesCount());
        // spv_b present at even indices (0,2,4,6,8), unset at odd
        int present = 0;
        for (int i = 0; i < 10; i++) {
            final boolean hasValue = colB.getDataValues(i).getValueCase() != DataValue.ValueCase.VALUE_NOT_SET;
            if (i % 2 == 0) {
                assertTrue("spv_b should have a value at row " + i, hasValue);
                present++;
            } else {
                assertFalse("spv_b should be unset at row " + i, hasValue);
            }
        }
        assertEquals(5, present);
    }

    @Test
    public void testEveryResolvedPvGetsColumnIncludingAllUnset() {
        // query only PV_A's rate window but include PV_B AND a resolved PV with no data in-range.
        // Use a sub-window where spv_b HAS data but restrict... simplest: window [B,B+1) already covers
        // both; here assert PV_B column exists even if we shrink to a nanos slice where spv_b is absent.
        // Window [B+1e8, B+2e8): only spv_a has a sample (at B+1e8); spv_b absent entirely => all-unset.
        final QuerySamplesResponse response = runSamples(
                samplesRequest(List.of(PV_A, PV_B), B, 100_000_000L, B, 200_000_000L, 0, null, false));
        final ColumnTable table = response.getSampleQueryResult().getColumnTable();

        assertEquals(1, table.getTimestampList().getTimestampsCount()); // just B+1e8
        assertEquals(2, table.getDataColumnsCount());
        final DataColumn colB = columnByName(table, PV_B);
        assertEquals(1, colB.getDataValuesCount());
        assertEquals("spv_b has no sample in this slice => all-unset column",
                DataValue.ValueCase.VALUE_NOT_SET, colB.getDataValues(0).getValueCase());
    }

    // -----------------------------------------------------------------------
    // half-open trimming
    // -----------------------------------------------------------------------

    @Test
    public void testHalfOpenTrimmingDropsBoundarySamples() {
        // window [B, B+1e8) is half-open: includes B (nano 0) but excludes B+1e8. Only 1 timestamp.
        final QuerySamplesResponse response = runSamples(
                samplesRequest(List.of(PV_A), B, 0, B, 100_000_000L, 0, null, false));
        final ColumnTable table = response.getSampleQueryResult().getColumnTable();
        assertEquals(1, table.getTimestampList().getTimestampsCount());
        assertEquals(0, table.getTimestampList().getTimestamps(0).getNanoseconds());
    }

    // -----------------------------------------------------------------------
    // timestamp paging (Q1) — seam invariant
    // -----------------------------------------------------------------------

    @Test
    public void testTimestampPagingSeamNoGapNoOverlap() {
        // full 3-second window for spv_a = 30 timestamps; page by 7 => 7,7,7,7,2
        final List<Timestamp> collected = new ArrayList<>();
        String token = null;
        int pages = 0;
        while (true) {
            final QuerySamplesResponse response = runSamples(
                    samplesRequest(List.of(PV_A), B, 0, B + NUM_SECONDS, 0, 7, token, false));
            final QuerySamplesResponse.SampleQueryResult result = response.getSampleQueryResult();
            collected.addAll(result.getColumnTable().getTimestampList().getTimestampsList());
            pages++;
            token = result.getNextPageToken();
            if (token.isEmpty() || pages >= 20) {
                break;
            }
            assertTrue("page must not exceed limit", result.getColumnTable().getTimestampList().getTimestampsCount() <= 7);
        }
        assertEquals(5, pages);
        assertEquals(30, collected.size());
        // seam invariant: strictly increasing, no dup, no gap (spv_a is a regular 100ms grid)
        for (int i = 1; i < collected.size(); i++) {
            final long prev = collected.get(i - 1).getEpochSeconds() * 1_000_000_000L + collected.get(i - 1).getNanoseconds();
            final long cur = collected.get(i).getEpochSeconds() * 1_000_000_000L + collected.get(i).getNanoseconds();
            assertEquals("consecutive timestamps must be exactly one 100ms step apart at the seam",
                    100_000_000L, cur - prev);
        }
    }

    @Test
    public void testLastPageEmptyToken() {
        // exactly 10 timestamps in [B,B+1) with limit 10 => one page, empty token
        final QuerySamplesResponse response = runSamples(
                samplesRequest(List.of(PV_A), B, 0, B + 1, 0, 10, null, false));
        final QuerySamplesResponse.SampleQueryResult result = response.getSampleQueryResult();
        assertEquals(10, result.getColumnTable().getTimestampList().getTimestampsCount());
        assertTrue(result.getNextPageToken().isEmpty());
    }

    @Test
    public void testByteBudgetPagingSeamNoGapNoOverlap() {
        // Force byte-driven page boundaries with a small budget, then page the full spv_a window and
        // verify the seam invariant holds across byte-driven cuts (the last, possibly-incomplete
        // timestamp of each page is dropped and re-queried, so no gap and no duplicate).
        // Size one row: run one page unbounded, measure a single-row column table's per-row cost via
        // the serialized size of the whole 30-row result divided out is fragile; instead pick a tiny
        // absolute budget that admits only a few rows and rely on truncation correctness.
        final long smallBudget = 40; // bytes — admits a couple of rows per page

        final List<Timestamp> collected = new ArrayList<>();
        String token = null;
        int pages = 0;
        while (true) {
            final QuerySamplesResponse response = runSamplesWithBudget(
                    samplesRequest(List.of(PV_A), B, 0, B + NUM_SECONDS, 0, 10_000, token, false),
                    smallBudget);
            final QuerySamplesResponse.SampleQueryResult result = response.getSampleQueryResult();
            assertFalse(response.hasExceptionalResult());
            collected.addAll(result.getColumnTable().getTimestampList().getTimestampsList());
            pages++;
            token = result.getNextPageToken();
            if (token.isEmpty() || pages >= 100) {
                break;
            }
            // each byte-bounded page must make progress (>= 1 row) to avoid an infinite loop
            assertTrue("byte-bounded page must emit at least one row",
                    result.getColumnTable().getTimestampList().getTimestampsCount() >= 1);
        }
        assertTrue("should terminate well within the safety cap", pages < 100);
        assertEquals(30, collected.size());
        for (int i = 1; i < collected.size(); i++) {
            final long prev = collected.get(i - 1).getEpochSeconds() * 1_000_000_000L + collected.get(i - 1).getNanoseconds();
            final long cur = collected.get(i).getEpochSeconds() * 1_000_000_000L + collected.get(i).getNanoseconds();
            assertEquals("no gap/overlap across byte-driven seam", 100_000_000L, cur - prev);
        }
    }

    // -----------------------------------------------------------------------
    // non-scalar reject (Q4)
    // -----------------------------------------------------------------------

    @Test
    public void testNonScalarPvRejected() {
        final QuerySamplesResponse response = runSamples(
                samplesRequest(List.of(PV_ARR), B, 0, B + 1, 0, 0, null, false));
        assertTrue(response.hasExceptionalResult());
        assertEquals(com.ospreydcs.dp.grpc.v1.common.ExceptionalResult.ExceptionalResultStatus.RESULT_STATUS_REJECT,
                response.getExceptionalResult().getExceptionalResultStatus());
        final String msg = response.getExceptionalResult().getMessage();
        assertTrue("reject must name the PV", msg.contains(PV_ARR));
        assertTrue("reject must point at queryBuckets", msg.contains("queryBuckets"));
    }

    // -----------------------------------------------------------------------
    // useSerializedColumns (Q5)
    // -----------------------------------------------------------------------

    @Test
    public void testUseSerializedColumnsPopulatesSerializedListOnly() {
        final QuerySamplesResponse response = runSamples(
                samplesRequest(List.of(PV_A, PV_B), B, 0, B + 1, 0, 0, null, true));
        final ColumnTable table = response.getSampleQueryResult().getColumnTable();
        assertEquals("dataColumns must be empty when serialized requested", 0, table.getDataColumnsCount());
        assertEquals("serializedDataColumns must carry all resolved PVs", 2, table.getSerializedDataColumnsCount());
        // payload must parse back to a DataColumn with the expected name
        try {
            final DataColumn parsed = DataColumn.parseFrom(table.getSerializedDataColumns(0).getPayload());
            assertEquals(PV_A, parsed.getName());
            assertEquals(10, parsed.getDataValuesCount());
        } catch (Exception e) {
            fail("serialized payload should parse as DataColumn: " + e.getMessage());
        }
    }

    // -----------------------------------------------------------------------
    // empty result
    // -----------------------------------------------------------------------

    @Test
    public void testEmptyResultIsEmptyColumnTable() {
        final QuerySamplesResponse response = runSamples(
                samplesRequest(List.of(PV_A), B - 1000, 0, B - 900, 0, 0, null, false));
        assertFalse(response.hasExceptionalResult());
        assertTrue(response.hasSampleQueryResult());
        assertEquals(0, response.getSampleQueryResult().getColumnTable().getTimestampList().getTimestampsCount());
        assertTrue(response.getSampleQueryResult().getNextPageToken().isEmpty());
    }

    // -----------------------------------------------------------------------
    // streaming (step 6) — querySamplesStream
    // -----------------------------------------------------------------------

    @Test
    public void testStreamRowChunkingAlignedAcrossChunks() {
        // 3-second spv_a window = 30 rows; chunk (limit) 7 => messages of 7,7,7,7,2
        final StreamOutcome outcome = runSamplesStream(
                samplesRequest(List.of(PV_A, PV_B), B, 0, B + NUM_SECONDS, 0, 7, null, false), Long.MAX_VALUE);

        assertTrue(outcome.completed);
        assertFalse(outcome.errored);
        assertEquals(5, outcome.messages.size());

        int totalRows = 0;
        long prevNanoTotal = -1;
        for (QuerySamplesResponse r : outcome.messages) {
            assertTrue(r.hasSampleQueryResult());
            final QuerySamplesResponse.SampleQueryResult result = r.getSampleQueryResult();
            assertTrue("streamed token must be empty", result.getNextPageToken().isEmpty());
            final ColumnTable table = result.getColumnTable();
            // column set stable across chunks: both PVs, sorted, timestamp/column row counts aligned
            assertEquals(2, table.getDataColumnsCount());
            assertEquals(PV_A, table.getDataColumns(0).getName());
            assertEquals(PV_B, table.getDataColumns(1).getName());
            final int rows = table.getTimestampList().getTimestampsCount();
            assertTrue(rows <= 7);
            assertEquals(rows, table.getDataColumns(0).getDataValuesCount());
            assertEquals(rows, table.getDataColumns(1).getDataValuesCount());
            totalRows += rows;
            // seam: timestamps strictly increasing across chunk boundaries
            for (Timestamp t : table.getTimestampList().getTimestampsList()) {
                final long nanoTotal = t.getEpochSeconds() * 1_000_000_000L + t.getNanoseconds();
                assertTrue("timestamps strictly increasing across chunks", nanoTotal > prevNanoTotal);
                prevNanoTotal = nanoTotal;
            }
        }
        assertEquals(30, totalRows);
    }

    @Test
    public void testStreamSingleFullChunk() {
        final StreamOutcome outcome = runSamplesStream(
                samplesRequest(List.of(PV_A), B, 0, B + 1, 0, 1000, null, false), Long.MAX_VALUE);
        assertTrue(outcome.completed);
        assertEquals(1, outcome.messages.size());
        assertEquals(10, outcome.messages.get(0).getSampleQueryResult().getColumnTable()
                .getTimestampList().getTimestampsCount());
    }

    @Test
    public void testStreamEmptyResultSingleEmptyMessage() {
        final StreamOutcome outcome = runSamplesStream(
                samplesRequest(List.of(PV_A), B - 1000, 0, B - 900, 0, 10, null, false), Long.MAX_VALUE);
        assertTrue(outcome.completed);
        assertEquals(1, outcome.messages.size());
        assertFalse(outcome.messages.get(0).hasExceptionalResult());
        assertEquals(0, outcome.messages.get(0).getSampleQueryResult().getColumnTable()
                .getTimestampList().getTimestampsCount());
    }

    @Test
    public void testStreamRejectsNonEmptyPageToken() {
        final String token = com.ospreydcs.dp.service.query.handler.paging.PageToken.encode(
                com.ospreydcs.dp.service.query.handler.model.KeysetPosition.ofSample(B, 0));
        final QuerySamplesRequest request =
                samplesRequest(List.of(PV_A), B, 0, B + NUM_SECONDS, 0, 5, token, false);
        final ResolutionResult resolution = resolver().resolve(
                request.getQuerySpec(), request.getExecutionOptions(), request.getResultRepresentation(),
                ResolvedQuery.ResultMode.SAMPLE, true);
        assertTrue(resolution.isError());
        assertTrue(resolution.getErrorStatus().msg.contains("streaming"));
    }

    @Test
    public void testStreamByteBudgetChunkFlush() {
        // small budget forces multiple chunks even though limit is huge; verify every row delivered
        // once. The budget must clear the conservative single-row estimate (timestamp + one labeled
        // double column + framing, ~52 bytes) but not two rows, so each chunk carries ~one row.
        final StreamOutcome outcome = runSamplesStream(
                samplesRequest(List.of(PV_A), B, 0, B + NUM_SECONDS, 0, 10_000, null, false), 60);
        assertTrue(outcome.completed);
        assertTrue("byte budget should force multiple chunks", outcome.messages.size() > 1);
        int totalRows = 0;
        long prevNanoTotal = -1;
        for (QuerySamplesResponse r : outcome.messages) {
            final ColumnTable table = r.getSampleQueryResult().getColumnTable();
            totalRows += table.getTimestampList().getTimestampsCount();
            for (Timestamp t : table.getTimestampList().getTimestampsList()) {
                final long nanoTotal = t.getEpochSeconds() * 1_000_000_000L + t.getNanoseconds();
                assertTrue(nanoTotal > prevNanoTotal); // no dup / no gap-reorder across byte-flushed chunks
                prevNanoTotal = nanoTotal;
            }
        }
        assertEquals(30, totalRows);
    }

    @Test
    public void testStreamNonScalarRejected() {
        final StreamOutcome outcome = runSamplesStream(
                samplesRequest(List.of(PV_ARR), B, 0, B + 1, 0, 10, null, false), Long.MAX_VALUE);
        assertFalse(outcome.messages.isEmpty());
        final QuerySamplesResponse last = outcome.messages.get(outcome.messages.size() - 1);
        assertTrue(last.hasExceptionalResult());
        assertEquals(com.ospreydcs.dp.grpc.v1.common.ExceptionalResult.ExceptionalResultStatus.RESULT_STATUS_REJECT,
                last.getExceptionalResult().getExceptionalResultStatus());
        assertTrue(last.getExceptionalResult().getMessage().contains(PV_ARR));
    }

    // -----------------------------------------------------------------------
    // indivisible-oversized single row (byte budget below one row's cost)
    // -----------------------------------------------------------------------

    @Test
    public void testUnaryOversizedSingleTimestampErrors() {
        // When the byte-driven boundary would drop the ONLY assembled timestamp (a bucket that clips
        // to a single distinct timestamp in the window, tripping the size limit), keepCount becomes 0.
        // Dropping it and resuming there would re-assemble the identical row on the next page and hit
        // the same boundary forever (zero-progress infinite loop of empty pages). The dispatcher must
        // ERROR instead. Window [B, B+1@1ns) clips spv_a to its single first sample at B.0.
        final QuerySamplesResponse response = runSamplesWithBudget(
                samplesRequest(List.of(PV_A), B, 0, B, 1, 10_000, null, false), 1);
        assertTrue(response.hasExceptionalResult());
        assertEquals(com.ospreydcs.dp.grpc.v1.common.ExceptionalResult.ExceptionalResultStatus.RESULT_STATUS_ERROR,
                response.getExceptionalResult().getExceptionalResultStatus());
        assertTrue(response.getExceptionalResult().getMessage().contains("exceeds the outgoing message size limit"));
    }

    @Test
    public void testStreamIndivisibleOversizedRowErrors() {
        // Streaming analog: a single row larger than the whole budget cannot be chunked. Rather than
        // emit an over-limit message that gRPC would abort the stream on, the dispatcher errors out
        // naming the timestamp (mirrors the buckets streaming isIndivisibleOversized guard).
        final StreamOutcome outcome = runSamplesStream(
                samplesRequest(List.of(PV_A), B, 0, B + NUM_SECONDS, 0, 10_000, null, false), 1);
        assertFalse(outcome.messages.isEmpty());
        final QuerySamplesResponse last = outcome.messages.get(outcome.messages.size() - 1);
        assertTrue(last.hasExceptionalResult());
        assertEquals(com.ospreydcs.dp.grpc.v1.common.ExceptionalResult.ExceptionalResultStatus.RESULT_STATUS_ERROR,
                last.getExceptionalResult().getExceptionalResultStatus());
        assertTrue(last.getExceptionalResult().getMessage().contains("exceeds the outgoing message size limit"));
    }

    // -----------------------------------------------------------------------
    // #207: fragmented retrieval intervals (ConfigurationSelector) must be enforced at SAMPLE
    // granularity, not just per-bucket by the database query.
    // -----------------------------------------------------------------------

    /**
     * Two disjoint fragments [SPAN_B, SPAN_B+2) and [SPAN_B+8, SPAN_B+10); seconds +2..+7 are the gap.
     * Builds the ResolvedQuery directly rather than going through the resolver, so the fragmented
     * shape is exercised without seeding configuration activations.
     */
    private static ResolvedQuery fragmentedSpanQuery(boolean streaming) {
        final List<TimeInterval> intervals = new ArrayList<>();
        intervals.add(new TimeInterval(SPAN_B, 0, SPAN_B + 2, 0));
        intervals.add(new TimeInterval(SPAN_B + 8, 0, SPAN_B + 10, 0));
        return new ResolvedQuery(
                List.of(PV_SPAN), intervals, DEFAULT_PAGE_SIZE, null, false, false,
                ResolvedQuery.ResultMode.SAMPLE, streaming);
    }

    /** Offsets from SPAN_B of every timestamp in the table. */
    private static List<Long> spanOffsets(ColumnTable table) {
        final List<Long> offsets = new ArrayList<>();
        for (Timestamp t : table.getTimestampList().getTimestampsList()) {
            offsets.add(t.getEpochSeconds() - SPAN_B);
        }
        return offsets;
    }

    @Test
    public void testFragmentGapExcludedUnary() {
        final List<QuerySamplesResponse> responses = new ArrayList<>();
        final StreamObserver<QuerySamplesResponse> observer = new StreamObserver<>() {
            @Override public void onNext(QuerySamplesResponse r) { responses.add(r); }
            @Override public void onError(Throwable t) { }
            @Override public void onCompleted() { }
        };
        new QueryV2Job(
                fragmentedSpanQuery(false),
                new QuerySamplesUnaryDispatcher(observer, Long.MAX_VALUE),
                clientTestInterface).execute();

        assertEquals(1, responses.size());
        final ColumnTable table = responses.get(0).getSampleQueryResult().getColumnTable();
        assertEquals("only in-fragment samples expected", List.of(0L, 1L, 8L, 9L), spanOffsets(table));

        // values must stay aligned with their timestamps after the gap is dropped
        final DataColumn column = columnByName(table, PV_SPAN);
        assertEquals(4, column.getDataValuesCount());
        assertEquals(0.0, column.getDataValues(0).getDoubleValue(), 0.0);
        assertEquals(1.0, column.getDataValues(1).getDoubleValue(), 0.0);
        assertEquals(8.0, column.getDataValues(2).getDoubleValue(), 0.0);
        assertEquals(9.0, column.getDataValues(3).getDoubleValue(), 0.0);
    }

    @Test
    public void testFragmentGapExcludedStreaming() {
        final StreamOutcome outcome = new StreamOutcome();
        final StreamObserver<QuerySamplesResponse> observer = new StreamObserver<>() {
            @Override public void onNext(QuerySamplesResponse r) { outcome.messages.add(r); }
            @Override public void onError(Throwable t) { outcome.errored = true; }
            @Override public void onCompleted() { outcome.completed = true; }
        };
        new QueryV2Job(
                fragmentedSpanQuery(true),
                new com.ospreydcs.dp.service.query.handler.mongo.dispatch.QuerySamplesStreamDispatcher(
                        observer, Long.MAX_VALUE),
                clientTestInterface).execute();

        assertFalse(outcome.errored);
        assertTrue(outcome.completed);

        final List<Long> allOffsets = new ArrayList<>();
        for (QuerySamplesResponse r : outcome.messages) {
            allOffsets.addAll(spanOffsets(r.getSampleQueryResult().getColumnTable()));
        }
        assertEquals("only in-fragment samples expected", List.of(0L, 1L, 8L, 9L), allOffsets);
    }

    @Test
    public void testFragmentGapExcludedAcrossPages() throws Exception {
        // pageSize=1 forces a page boundary at every row, including across the gap seam, so the
        // resume-clamped retention intervals are exercised on each continuation page (#207).
        final List<Long> collected = new ArrayList<>();
        String pageToken = null;
        for (int page = 0; page < 20; page++) {
            final List<TimeInterval> intervals = new ArrayList<>();
            intervals.add(new TimeInterval(SPAN_B, 0, SPAN_B + 2, 0));
            intervals.add(new TimeInterval(SPAN_B + 8, 0, SPAN_B + 10, 0));
            final ResolvedQuery rq = new ResolvedQuery(
                    List.of(PV_SPAN), intervals, 1,
                    (pageToken == null || pageToken.isEmpty())
                            ? null
                            : com.ospreydcs.dp.service.query.handler.paging.PageToken.decode(pageToken),
                    false, false, ResolvedQuery.ResultMode.SAMPLE, false);

            final List<QuerySamplesResponse> responses = new ArrayList<>();
            final StreamObserver<QuerySamplesResponse> observer = new StreamObserver<>() {
                @Override public void onNext(QuerySamplesResponse r) { responses.add(r); }
                @Override public void onError(Throwable t) { }
                @Override public void onCompleted() { }
            };
            new QueryV2Job(rq, new QuerySamplesUnaryDispatcher(observer, Long.MAX_VALUE),
                    clientTestInterface).execute();
            assertEquals(1, responses.size());
            final QuerySamplesResponse.SampleQueryResult result = responses.get(0).getSampleQueryResult();
            collected.addAll(spanOffsets(result.getColumnTable()));
            pageToken = result.getNextPageToken();
            if (pageToken == null || pageToken.isEmpty()) {
                break;
            }
        }
        assertEquals("paged traversal must visit each in-fragment sample exactly once",
                List.of(0L, 1L, 8L, 9L), collected);
    }
}
