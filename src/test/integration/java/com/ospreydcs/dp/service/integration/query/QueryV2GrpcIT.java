package com.ospreydcs.dp.service.integration.query;

import com.ospreydcs.dp.grpc.v1.common.DataBucket;
import com.ospreydcs.dp.grpc.v1.common.ExceptionalResult;
import com.ospreydcs.dp.grpc.v1.common.Timestamp;
import com.ospreydcs.dp.grpc.v1.query.ColumnTable;
import com.ospreydcs.dp.grpc.v1.query.QueryBucketsRequest;
import com.ospreydcs.dp.grpc.v1.query.QueryBucketsResponse;
import com.ospreydcs.dp.grpc.v1.query.QuerySamplesRequest;
import com.ospreydcs.dp.grpc.v1.query.QuerySamplesResponse;
import com.ospreydcs.dp.grpc.v1.query.QuerySpec;
import com.ospreydcs.dp.service.integration.GrpcIntegrationTestBase;
import com.ospreydcs.dp.service.integration.ingest.GrpcIntegrationIngestionServiceWrapper;
import com.ospreydcs.dp.service.query.QueryTestBase;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.time.Instant;
import java.util.List;

import static org.junit.Assert.*;

/**
 * Full gRPC-channel end-to-end integration test for all four Query API V2 RPCs
 * (queryBuckets / queryBucketsStream / querySamples / querySamplesStream). Data is ingested through
 * the real ingestion service, then queried through the in-process gRPC channel so the complete path
 * is exercised: client stub → QueryServiceImpl override → MongoQueryHandler resolver + worker queue →
 * dispatcher → response stream. The dispatcher-level tests
 * (MongoSyncQueryBucketsV2Test / MongoSyncQuerySamplesV2Test) cover the assembly/paging logic in
 * detail; this test confirms the wiring and stub contracts hold over the wire.
 *
 * <p>Seeded scenario ({@code simpleIngestionScenario}): PVs {@code S01-GCC01..S10-BPM03}, each with 10
 * one-second buckets (10 samples/bucket) over {@code [startSeconds, startSeconds+10)}.
 */
@RunWith(JUnit4.class)
public class QueryV2GrpcIT extends GrpcIntegrationTestBase {

    private static final String PV_1 = "S01-GCC01";
    private static final String PV_2 = "S01-BPM01";

    @Before
    public void setUp() throws Exception {
        super.setUp();
    }

    @After
    public void tearDown() {
        super.tearDown();
    }

    @Test
    public void testQueryV2EndToEnd() {

        final long startSeconds = Instant.now().getEpochSecond();
        final GrpcIntegrationIngestionServiceWrapper.IngestionScenarioResult ingestionScenarioResult =
                ingestionServiceWrapper.simpleIngestionScenario(startSeconds, false);
        assertNotNull(ingestionScenarioResult);

        final long begin = startSeconds;
        final long end = startSeconds + 10; // whole 10-bucket window

        // ---- queryBuckets (unary), single page ----
        {
            final QuerySpec spec = QueryTestBase.buildV2QuerySpecPvNameList(List.of(PV_1), begin, 0, end, 0);
            final QueryBucketsRequest request = QueryTestBase.buildQueryBucketsRequest(spec, 0, null, false, false);
            final QueryBucketsResponse response = queryServiceWrapper.sendQueryBuckets(request);
            assertTrue(response.hasBucketQueryResult());
            assertEquals(10, response.getBucketQueryResult().getDataBucketsCount());
            assertTrue(response.getBucketQueryResult().getNextPageToken().isEmpty());
            for (DataBucket b : response.getBucketQueryResult().getDataBucketsList()) {
                assertEquals(PV_1, b.getPvName());
            }
        }

        // ---- queryBuckets (unary), multi-page keyset continuation over 2 PVs ----
        {
            final QuerySpec spec = QueryTestBase.buildV2QuerySpecPvNameList(List.of(PV_1, PV_2), begin, 0, end, 0);
            final List<DataBucket> all = queryServiceWrapper.queryBucketsAllPages(spec, 7); // 20 buckets, pages of 7
            assertEquals(20, all.size());
            // sorted by pvName then firstTime: 10 of PV_2 (BPM sorts before GCC? no: BPM < GCC) ...
            // assert each PV contributes 10 buckets regardless of order
            long pv1 = all.stream().filter(b -> b.getPvName().equals(PV_1)).count();
            long pv2 = all.stream().filter(b -> b.getPvName().equals(PV_2)).count();
            assertEquals(10, pv1);
            assertEquals(10, pv2);
        }

        // ---- queryBucketsStream ----
        {
            final QuerySpec spec = QueryTestBase.buildV2QuerySpecPvNameList(List.of(PV_1), begin, 0, end, 0);
            final QueryBucketsRequest request = QueryTestBase.buildQueryBucketsRequest(spec, 4, null, false, false);
            final List<QueryBucketsResponse> messages = queryServiceWrapper.sendQueryBucketsStream(request);
            int total = 0;
            for (QueryBucketsResponse r : messages) {
                assertTrue(r.hasBucketQueryResult());
                assertTrue(r.getBucketQueryResult().getNextPageToken().isEmpty()); // empty on every stream msg
                assertTrue(r.getBucketQueryResult().getDataBucketsCount() <= 4);
                total += r.getBucketQueryResult().getDataBucketsCount();
            }
            assertEquals(10, total);
            assertTrue("chunking should produce multiple messages", messages.size() >= 3);
        }

        // ---- queryBucketsStream rejects a non-empty pageToken ----
        {
            final QuerySpec spec = QueryTestBase.buildV2QuerySpecPvNameList(List.of(PV_1), begin, 0, end, 0);
            final QueryBucketsRequest request =
                    QueryTestBase.buildQueryBucketsRequest(spec, 4, "some-token", false, false);
            final List<QueryBucketsResponse> messages = queryServiceWrapper.sendQueryBucketsStream(request);
            assertFalse(messages.isEmpty());
            final QueryBucketsResponse last = messages.get(messages.size() - 1);
            assertTrue(last.hasExceptionalResult());
            assertEquals(ExceptionalResult.ExceptionalResultStatus.RESULT_STATUS_REJECT,
                    last.getExceptionalResult().getExceptionalResultStatus());
        }

        // ---- querySamples (unary), single page ----
        {
            final QuerySpec spec = QueryTestBase.buildV2QuerySpecPvNameList(List.of(PV_1, PV_2), begin, 0, end, 0);
            final QuerySamplesRequest request = QueryTestBase.buildQuerySamplesRequest(spec, 0, null, false);
            final QuerySamplesResponse response = queryServiceWrapper.sendQuerySamples(request);
            assertTrue(response.hasSampleQueryResult());
            final ColumnTable table = response.getSampleQueryResult().getColumnTable();
            // 10 buckets x 10 samples over the 10-second window = 100 timestamps, same grid for both PVs
            assertEquals(100, table.getTimestampList().getTimestampsCount());
            assertEquals(2, table.getDataColumnsCount());
            for (var col : table.getDataColumnsList()) {
                assertEquals(100, col.getDataValuesCount());
            }
            assertTrue(response.getSampleQueryResult().getNextPageToken().isEmpty());
        }

        // ---- querySamples (unary), multi-page timestamp continuation seam ----
        {
            final QuerySpec spec = QueryTestBase.buildV2QuerySpecPvNameList(List.of(PV_1), begin, 0, end, 0);
            final List<Timestamp> timestamps = queryServiceWrapper.querySamplesAllPageTimestamps(spec, 30);
            assertEquals(100, timestamps.size());
            // regular 100ms grid: consecutive timestamps exactly one step apart across page seams
            for (int i = 1; i < timestamps.size(); i++) {
                final long prev = timestamps.get(i - 1).getEpochSeconds() * 1_000_000_000L + timestamps.get(i - 1).getNanoseconds();
                final long cur = timestamps.get(i).getEpochSeconds() * 1_000_000_000L + timestamps.get(i).getNanoseconds();
                assertEquals(100_000_000L, cur - prev);
            }
        }

        // ---- querySamplesStream ----
        {
            final QuerySpec spec = QueryTestBase.buildV2QuerySpecPvNameList(List.of(PV_1, PV_2), begin, 0, end, 0);
            final QuerySamplesRequest request = QueryTestBase.buildQuerySamplesRequest(spec, 25, null, false);
            final List<QuerySamplesResponse> messages = queryServiceWrapper.sendQuerySamplesStream(request);
            int totalRows = 0;
            for (QuerySamplesResponse r : messages) {
                assertTrue(r.hasSampleQueryResult());
                assertTrue(r.getSampleQueryResult().getNextPageToken().isEmpty());
                final ColumnTable table = r.getSampleQueryResult().getColumnTable();
                assertEquals(2, table.getDataColumnsCount()); // stable column set across chunks
                final int rows = table.getTimestampList().getTimestampsCount();
                assertTrue(rows <= 25);
                assertEquals(rows, table.getDataColumns(0).getDataValuesCount());
                totalRows += rows;
            }
            assertEquals(100, totalRows);
            assertTrue("chunking should produce multiple messages", messages.size() >= 4);
        }
    }
}
