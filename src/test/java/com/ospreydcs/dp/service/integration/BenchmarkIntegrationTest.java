package com.ospreydcs.dp.service.integration;

import com.ospreydcs.dp.grpc.v1.common.DataColumn;
import com.ospreydcs.dp.grpc.v1.common.DataTable;
import com.ospreydcs.dp.grpc.v1.common.DataValue;
import com.ospreydcs.dp.grpc.v1.common.Timestamp;
import com.ospreydcs.dp.grpc.v1.ingestion.IngestionRequest;
import com.ospreydcs.dp.grpc.v1.ingestion.IngestionResponse;
import com.ospreydcs.dp.grpc.v1.query.QueryRequest;
import com.ospreydcs.dp.grpc.v1.query.QueryResponse;
import com.ospreydcs.dp.service.common.bson.BucketDocument;
import com.ospreydcs.dp.service.common.bson.RequestStatusDocument;
import com.ospreydcs.dp.service.common.model.BenchmarkScenarioResult;
import com.ospreydcs.dp.service.ingest.benchmark.BenchmarkStreamingIngestion;
import com.ospreydcs.dp.service.ingest.benchmark.IngestionBenchmarkBase;
import com.ospreydcs.dp.service.query.benchmark.*;
import io.grpc.Channel;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.time.Instant;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import static org.junit.Assert.*;

@RunWith(JUnit4.class)
public class BenchmarkIntegrationTest extends GrpcIntegrationTestBase {

    // static variables
    private static final Logger logger = LogManager.getLogger();
    private static IntegrationTestIngestionGrpcClient ingestionGrpcClient;
    private static IntegrationTestQueryGrpcClient queryGrpcClient;

    // ingestion constants
    private static final int INGESTION_NUM_PVS = 4000;
    private static final int INGESTION_NUM_THREADS = 7;
    private static final int INGESTION_NUM_STREAMS = 20;
    private static final int INGESTION_NUM_ROWS = 1000;
    private static final int INGESTION_NUM_SECONDS = 60;

    // query constants
    private static final int QUERY_NUM_PVS = 1000;
    private static final int QUERY_NUM_PVS_PER_REQUEST = 10;
    private static final int QUERY_NUM_THREADS = 7;
    private static final int QUERY_SINGLE_NUM_PVS = 10;
    private static final int QUERY_SINGLE_NUM_PVS_PER_REQUEST = 1;
    private static final int QUERY_TABLE_NUM_PVS = 50;
    private static final int QUERY_TABLE_NUM_PVS_PER_REQUEST = 5;


    private static class IntegrationTestStreamingIngestionApp extends BenchmarkStreamingIngestion {

        private static class IntegrationTestIngestionRequestInfo {
            public final int providerId;
            public final long startSeconds;
            public boolean responseReceived = false;
            public IntegrationTestIngestionRequestInfo(int providerId, long startSeconds) {
                this.providerId = providerId;
                this.startSeconds = startSeconds;
            }
        }

        private static class IntegrationTestIngestionTask
                extends BenchmarkStreamingIngestion.StreamingIngestionTask
        {
            // instance variables
            private Map<String, IntegrationTestIngestionRequestInfo> requestValidationMap = new TreeMap<>();
            private final ReentrantReadWriteLock rwLock = new ReentrantReadWriteLock();
            private final Lock readLock = rwLock.readLock();
            private final Lock writeLock = rwLock.writeLock();
            private final AtomicInteger responseCount = new AtomicInteger(0);
            private final AtomicInteger dbBucketCount = new AtomicInteger(0);

            public IntegrationTestIngestionTask(
                    IngestionBenchmarkBase.IngestionTaskParams params,
                    DataTable.Builder templateDataTable,
                    Channel channel) {

                super(params, templateDataTable, channel);
            }

            @Override
            protected void onRequest(IngestionRequest request) {

                logger.trace("onRequest stream: " + this.params.streamNumber);

                // acquire writeLock for updating map
                writeLock.lock();
                try {
                    // add an entry for the request to the validation map
                    requestValidationMap.put(
                            request.getClientRequestId(),
                            new IntegrationTestIngestionRequestInfo(
                                    request.getProviderId(),
                                    request
                                            .getDataTable()
                                            .getDataTimeSpec()
                                            .getFixedIntervalTimestampSpec()
                                            .getStartTime()
                                            .getEpochSeconds()));

                } finally {
                    // using try...finally to make sure we unlock!
                    writeLock.unlock();
                }
            }

            @Override
            protected void onResponse(IngestionResponse response) {

                logger.trace("onResponse stream: " + this.params.streamNumber);
                responseCount.incrementAndGet();

                // acquire writeLock for updating map
                writeLock.lock();
                try {
                    final String responseRequestId = response.getClientRequestId();
                    IntegrationTestIngestionRequestInfo requestInfo =
                            requestValidationMap.get(responseRequestId);

                    // check that requestId in response matches a request
                    if (requestInfo == null) {
                        fail("response contains unexpected requestId: " + responseRequestId);
                        return;
                    }

                    // check that provider in response matches request
                    final int responseProviderId = response.getProviderId();
                    if (responseProviderId != requestInfo.providerId) {
                        fail("response provider id: " + responseProviderId
                                + " mismatch request: " + requestInfo.providerId);
                        return;
                    }

                    // validate dimensions in ack
                    assertEquals(this.params.numRows, response.getAckDetails().getNumRows());
                    assertEquals(this.params.numColumns, response.getAckDetails().getNumColumns());

                    // set validation flag for request
                    requestInfo.responseReceived = true;

                } finally {
                    // using try...finally to make sure we unlock!
                    writeLock.unlock();
                }

            }

            private void verifyRequestDbArtifacts(
                    String requestId, IntegrationTestIngestionRequestInfo requestInfo
            ) {
                // verify request status
                RequestStatusDocument statusDocument =
                        mongoClient.findRequestStatus(requestInfo.providerId, requestId);
                assertEquals(params.numColumns, statusDocument.getIdsCreated().size());

                // verify buckets
                for (int colIndex = params.firstColumnIndex; colIndex <= params.lastColumnIndex; colIndex++) {
                    final String columnName = NAME_COLUMN_BASE + colIndex;
                    final String bucketId = columnName + "-" + requestInfo.startSeconds + "-0";
                    assertTrue(
                            "providerId: " + requestInfo.providerId
                                    + " requestId: " + requestId
                                    + " bucketId: " + bucketId,
                            statusDocument.getIdsCreated().contains(bucketId));
                    BucketDocument bucketDocument = mongoClient.findBucket(bucketId);
                    assertNotNull("bucketId: " + bucketId, bucketDocument);
                    assertEquals(columnName, bucketDocument.getColumnName());
                    assertEquals(bucketId, bucketDocument.getId());
                    assertEquals(params.numRows, bucketDocument.getNumSamples());
                    assertEquals(1000000, bucketDocument.getSampleFrequency());
                    assertEquals(requestInfo.startSeconds, bucketDocument.getFirstSeconds());
                    assertEquals(0, bucketDocument.getFirstNanos());
                    assertEquals(
                            Date.from(Instant.ofEpochSecond(requestInfo.startSeconds, 0L)),
                            bucketDocument.getFirstTime());
                    assertEquals(requestInfo.startSeconds, bucketDocument.getLastSeconds());
                    assertEquals(999000000L, bucketDocument.getLastNanos());
                    assertEquals(
                            Date.from(Instant.ofEpochSecond(requestInfo.startSeconds, 999000000L)),
                            bucketDocument.getLastTime());
                    // TODO: why does getDataType() return null? can't find any details about it
                    //  assertEquals("DOUBLE", bucketDocument.getDataType());
                    assertEquals("calibration test", bucketDocument.getEventDescription());
                    assertEquals(params.startSeconds, bucketDocument.getEventSeconds());
                    assertEquals(0, bucketDocument.getEventNanos());
                    assertTrue(bucketDocument.getAttributeMap().get("sector").equals("07"));
                    assertTrue(bucketDocument.getAttributeMap().get("subsystem").equals("vacuum"));
                    assertEquals(params.numRows, bucketDocument.getColumnDataList().size());
                    // verify each value
                    for (int valIndex = 0 ; valIndex < bucketDocument.getNumSamples() ; ++valIndex) {
                        final double expectedValue = valIndex + (double) valIndex / bucketDocument.getNumSamples();
                        assertEquals(expectedValue, bucketDocument.getColumnDataList().get(valIndex));
                    }
                    dbBucketCount.incrementAndGet();
                }
            }

            @Override
            protected void onCompleted() {

                logger.trace("onCompleted stream: " + this.params.streamNumber);

                readLock.lock();
                try {
                    // iterate through requestMap and make sure all requests were acked/verified
                    for (var entry : requestValidationMap.entrySet()) {
                        String requestId = entry.getKey();
                        IntegrationTestIngestionRequestInfo requestInfo = entry.getValue();
                        if (!requestInfo.responseReceived) {
                            fail("did not receive ack for request: " + entry.getKey());
                        }
                        verifyRequestDbArtifacts(requestId, requestInfo);
                    }

                } finally {
                    readLock.unlock();
                }

                logger.debug(
                        "stream: {} ingestion task verified {} IngestionResponse messages, {} mongodb buckets",
                        params.streamNumber, responseCount.get(), dbBucketCount.get());

            }

        }

        protected StreamingIngestionTask newIngestionTask(
                IngestionTaskParams params, DataTable.Builder templateDataTable, Channel channel
        ) {
            return new IntegrationTestIngestionTask(params, templateDataTable, channel);
        }
    }

    protected static class IntegrationTestIngestionGrpcClient {

        final private Channel channel;

        public IntegrationTestIngestionGrpcClient(Channel channel) {
            this.channel = channel;
        }

        private void runStreamingIngestionScenario() {

            final int numColumnsPerStream = INGESTION_NUM_PVS / INGESTION_NUM_STREAMS;

            System.out.println();
            System.out.println("========== running ingestion scenario ==========");
            System.out.println("number of PVs: " + INGESTION_NUM_PVS);
            System.out.println("number of seconds (one bucket per PV per second): " + INGESTION_NUM_SECONDS);
            System.out.println("sampling interval (Hz): " + INGESTION_NUM_ROWS);
            System.out.println("number of ingestion API streams: " + INGESTION_NUM_STREAMS);
            System.out.println("number of PVs per stream: " + numColumnsPerStream);
            System.out.println("executorService thread pool size: " + INGESTION_NUM_THREADS);

            IntegrationTestStreamingIngestionApp ingestionApp = new IntegrationTestStreamingIngestionApp();
            BenchmarkScenarioResult scenarioResult = ingestionApp.ingestionScenario(
                    channel,
                    INGESTION_NUM_THREADS,
                    INGESTION_NUM_STREAMS,
                    INGESTION_NUM_ROWS,
                    numColumnsPerStream,
                    INGESTION_NUM_SECONDS
            );
            assertTrue(scenarioResult.success);

            System.out.println("========== ingestion scenario completed ==========");
            System.out.println();
        }
    }

    private static class IntegrationTestQueryTaskValidationHelper {

        // instance variables
        final QueryBenchmarkBase.QueryTaskParams params;
        final Map<String,boolean[]> columnBucketMap = new TreeMap<>();
        private final ReentrantReadWriteLock rwLock = new ReentrantReadWriteLock();
        private final Lock readLock = rwLock.readLock();
        private final Lock writeLock = rwLock.writeLock();
        private final AtomicInteger responseCount = new AtomicInteger(0);

        public IntegrationTestQueryTaskValidationHelper(QueryBenchmarkBase.QueryTaskParams params) {
            this.params = params;
        }

        protected void onRequest(QueryRequest request) {

            writeLock.lock();
            try {
                // add data structure for tracking expected buckets for each column in request
                assertTrue(request.hasQuerySpec());
                assertTrue(request.getQuerySpec().getColumnNamesCount() > 0);
                for (String columnName : request.getQuerySpec().getColumnNamesList()) {
                    assertNotNull(request.getQuerySpec().getStartTime());
                    final long startSeconds = request.getQuerySpec().getStartTime().getEpochSeconds();
                    assertNotNull(request.getQuerySpec().getEndTime());
                    final long endSeconds = request.getQuerySpec().getEndTime().getEpochSeconds();
                    final int numSeconds = (int) (endSeconds - startSeconds);
                    assertTrue(numSeconds > 0);
                    final boolean[] columnBucketArray = new boolean[numSeconds];
                    for (int i = 0 ; i < numSeconds ; i++) {
                        columnBucketArray[i] = false;
                    }
                    columnBucketMap.put(columnName, columnBucketArray);
                }

            } finally {
                writeLock.unlock();
            }
        }

        protected void onResponse(QueryResponse response) {

            assertNotNull(response.getResponseTime() != null);
            assertTrue(response.getResponseTime().getEpochSeconds() > 0);
            assertTrue(response.hasQueryReport());
            final QueryResponse.QueryReport report = response.getQueryReport();
            assertTrue(report.hasBucketData());
            final QueryResponse.QueryReport.BucketData queryData = report.getBucketData();

            responseCount.incrementAndGet();

            writeLock.lock();
            try {
                // verify buckets in response
                assertTrue(queryData.getDataBucketsCount() > 0);
                for (QueryResponse.QueryReport.BucketData.DataBucket bucket : queryData.getDataBucketsList()) {

                    assertTrue(bucket.hasDataColumn());
                    final DataColumn dataColumn = bucket.getDataColumn();
                    final String columnName = dataColumn.getName();
                    assertNotNull(columnName);
                    assertNotNull(dataColumn.getDataValuesList());
                    assertTrue(dataColumn.getDataValuesCount() == INGESTION_NUM_ROWS);
                    for (int i = 0 ; i < INGESTION_NUM_ROWS ; ++i) {
                        final DataValue dataValue = dataColumn.getDataValues(i);
                        assertNotNull(dataValue);
                        final double actualValue = dataValue.getFloatValue();
                        assertNotNull(actualValue);
                        final double expectedValue = i + (double) i / INGESTION_NUM_ROWS;
                        assertEquals(
                                "value mismatch: " + dataValue + " expected: " + actualValue,
                                expectedValue, actualValue, 0.0);
                    }
                    assertNotNull(bucket.getSamplingInterval());
                    assertNotNull(bucket.getSamplingInterval().getStartTime());
                    assertTrue(bucket.getSamplingInterval().getStartTime().getEpochSeconds() > 0);
                    assertNotNull(bucket.getSamplingInterval().getSampleIntervalNanos());
                    assertTrue(bucket.getSamplingInterval().getSampleIntervalNanos() > 0);
                    assertNotNull(bucket.getSamplingInterval().getNumSamples());
                    assertTrue(bucket.getSamplingInterval().getNumSamples() == INGESTION_NUM_ROWS);
                    final long bucketSeconds = bucket.getSamplingInterval().getStartTime().getEpochSeconds();
                    final int bucketIndex = (int) (bucketSeconds - params.startSeconds);
                    final boolean[] columnBucketArray = columnBucketMap.get(columnName);
                    assertNotNull("response contains unexpected bucket", columnBucketArray);

                    // mark bucket as received in tracking data structure
                    columnBucketArray[bucketIndex] = true;
                }

            } finally {
                writeLock.unlock();
            }
        }

        protected void onCompleted() {

            readLock.lock();
            try {
                // check that we recevied all expected buckets for each column
                for (var entry : columnBucketMap.entrySet()) {
                    final String columnName = entry.getKey();
                    final boolean[] columnBucketArray = entry.getValue();
                    for (int secondOffset = 0 ; secondOffset < columnBucketArray.length ; ++secondOffset) {
                        assertTrue(
                                "no bucket received column: " + columnName + " secondOffset: " + secondOffset,
                                columnBucketArray[secondOffset]);
                    }
                }

            } finally {
                readLock.unlock();
            }

            logger.debug("stream: {} validation helper verified {} QueryResponse messages",
                    params.streamNumber, responseCount.get());
        }

    }

    private static class IntegrationTestQueryResponseCursorApp extends BenchmarkQueryResponseCursor {

        private static class IntegrationTestQueryResponseCursorTask
                extends BenchmarkQueryResponseCursor.QueryResponseCursorTask
        {
            final private IntegrationTestQueryTaskValidationHelper helper;

            public IntegrationTestQueryResponseCursorTask(Channel channel, QueryTaskParams params
            ) {
                super(channel, params);
                helper = new IntegrationTestQueryTaskValidationHelper(params);
            }

            @Override
            protected void onRequest(QueryRequest request) {
                helper.onRequest(request);
            }

            @Override
            protected void onResponse(QueryResponse response) {
                helper.onResponse(response);
            }

            @Override
            protected void onCompleted() {
                helper.onCompleted();
            }

        }

        @Override
        protected QueryResponseCursorTask newQueryTask(
                Channel channel, QueryBenchmarkBase.QueryTaskParams params
        ) {
            return new IntegrationTestQueryResponseCursorTask(channel, params);
        }

    }

    private static class IntegrationTestQueryResponseStreamApp extends BenchmarkQueryResponseStream {

        private static class IntegrationTestQueryResponseStreamTask
                extends BenchmarkQueryResponseStream.QueryResponseStreamTask {

            final private IntegrationTestQueryTaskValidationHelper helper;

            public IntegrationTestQueryResponseStreamTask(Channel channel, QueryTaskParams params) {
                super(channel, params);
                helper = new IntegrationTestQueryTaskValidationHelper(params);
            }

            @Override
            protected void onRequest(QueryRequest request) {
                helper.onRequest(request);
            }

            @Override
            protected void onResponse(QueryResponse response) {
                helper.onResponse(response);
            }

            @Override
            protected void onCompleted() {
                helper.onCompleted();
            }

        }

        @Override
        protected QueryResponseStreamTask newQueryTask(
                Channel channel, QueryBenchmarkBase.QueryTaskParams params
        ) {
            return new IntegrationTestQueryResponseStreamTask(channel, params);
        }
    }

    private static class IntegrationTestQueryResponseSingleApp extends BenchmarkQueryResponseSingle {

        private static class IntegrationTestQueryResponseSingleTask
                extends BenchmarkQueryResponseSingle.QueryResponseSingleTask {

            final private IntegrationTestQueryTaskValidationHelper helper;

            public IntegrationTestQueryResponseSingleTask(Channel channel, QueryTaskParams params) {
                super(channel, params);
                helper = new IntegrationTestQueryTaskValidationHelper(params);
            }

            @Override
            protected void onRequest(QueryRequest request) {
                helper.onRequest(request);
            }

            @Override
            protected void onResponse(QueryResponse response) {
                helper.onResponse(response);
            }

            @Override
            protected void onCompleted() {
                helper.onCompleted();
            }
        }

        @Override
        protected IntegrationTestQueryResponseSingleTask newQueryTask(
                Channel channel, QueryBenchmarkBase.QueryTaskParams params
        ) {
            return new IntegrationTestQueryResponseSingleTask(channel, params);
        }
    }

    private static class IntegrationTestQueryResponseTableApp extends BenchmarkQueryResponseTable {

        private static class IntegrationTestQueryResponseTableTask
                extends BenchmarkQueryResponseTable.QueryResponseTableTask {

            public IntegrationTestQueryResponseTableTask(Channel channel, QueryTaskParams params) {
                super(channel, params);
            }

            @Override
            protected void onRequest(QueryRequest request) {
                // nothing to do for validating requests since we are maintaining a data structure with request info
            }

            @Override
            protected void onResponse(QueryResponse response) {

                // check that message contains a table response
                assertNotNull(response.getResponseTime() != null);
                assertTrue(response.getResponseTime().getEpochSeconds() > 0);
                assertTrue(response.hasQueryReport());
                final QueryResponse.QueryReport report = response.getQueryReport();
                assertTrue(report.hasDataTable());
                final DataTable dataTable = report.getDataTable();
                final List<String> columnNames = params.columnNames;
                assertEquals(columnNames.size(), dataTable.getDataColumnsCount());

                // validate timestamp list
                long secondsCurrent = params.startSeconds;
                long nanosCurrent = 0L;
                final List<Timestamp> timestampList = dataTable.getDataTimeSpec().getTimestampList().getTimestampsList();


                for (Timestamp timestamp : timestampList) {

                    final long timestampSeconds = timestamp.getEpochSeconds();
                    final long timestampNanos = timestamp.getNanoseconds();
                    assertEquals(secondsCurrent, timestampSeconds);
                    assertEquals(nanosCurrent, timestampNanos);

                    nanosCurrent = nanosCurrent + 1_000_000;
                    if (nanosCurrent == 1_000_000_000L) {
                        // increment seconds if we hit a billion nanos
                        nanosCurrent = 0;
                        secondsCurrent = secondsCurrent + 1;
                    }
                }

                // validate columns
                int valueIndex = 0;
                for (DataColumn dataColumn : dataTable.getDataColumnsList()) {
                    assertTrue(columnNames.contains(dataColumn.getName()));
                    for (DataValue dataValue : dataColumn.getDataValuesList()) {
                        final double expectedValue = valueIndex + (double) valueIndex / INGESTION_NUM_ROWS;
                        assertEquals(expectedValue, dataValue.getFloatValue(), 0.0);
                        valueIndex = valueIndex + 1;
                        if (valueIndex == 1000) {
                            // column values start over at 0 every second (every 1000 values)
                            valueIndex = 0;
                        }
                    }
                }
            }

            @Override
            protected void onCompleted() {
                // nothing to do when task completes since we only receive a single response and have already validated it
            }
        }

        @Override
        protected IntegrationTestQueryResponseTableTask newQueryTask(
                Channel channel, QueryBenchmarkBase.QueryTaskParams params
        ) {
            return new IntegrationTestQueryResponseTableTask(channel, params);
        }
    }

    protected static class IntegrationTestQueryGrpcClient {

        // instance variables
        final private Channel channel;

        public IntegrationTestQueryGrpcClient(Channel channel) {
            this.channel = channel;
        }

        private void runQueryResponseCursorScenario() {

            System.out.println();
            System.out.println("========== running queryResponseCursor scenario ==========");
            System.out.println("number of PVs: " + QUERY_NUM_PVS);
            System.out.println("number of PVs per request: " + QUERY_NUM_PVS_PER_REQUEST);
            System.out.println("number of threads: " + QUERY_NUM_THREADS);

            final long startSeconds = configMgr().getConfigLong(
                    IngestionBenchmarkBase.CFG_KEY_START_SECONDS,
                    IngestionBenchmarkBase.DEFAULT_START_SECONDS);

            IntegrationTestQueryResponseCursorApp queryResponseCursorApp =
                    new IntegrationTestQueryResponseCursorApp();
            BenchmarkScenarioResult scenarioResult = queryResponseCursorApp.queryScenario(
                    channel, QUERY_NUM_PVS, QUERY_NUM_PVS_PER_REQUEST, QUERY_NUM_THREADS, startSeconds);
            assertTrue(scenarioResult.success);

            System.out.println("========== queryResponseCursor scenario completed ==========");
            System.out.println();
        }

        private void runQueryResponseStreamScenario() {

            System.out.println();
            System.out.println("========== running queryResponseStream scenario ==========");
            System.out.println("number of PVs: " + QUERY_NUM_PVS);
            System.out.println("number of PVs per request: " + QUERY_NUM_PVS_PER_REQUEST);
            System.out.println("number of threads: " + QUERY_NUM_THREADS);

            final long startSeconds = configMgr().getConfigLong(
                    IngestionBenchmarkBase.CFG_KEY_START_SECONDS,
                    IngestionBenchmarkBase.DEFAULT_START_SECONDS);

            IntegrationTestQueryResponseStreamApp queryResponseStreamApp =
                    new IntegrationTestQueryResponseStreamApp();
            BenchmarkScenarioResult scenarioResult = queryResponseStreamApp.queryScenario(
                    channel, QUERY_NUM_PVS, QUERY_NUM_PVS_PER_REQUEST, QUERY_NUM_THREADS, startSeconds);
            assertTrue(scenarioResult.success);

            System.out.println("========== queryResponseStream scenario completed ==========");
            System.out.println();
        }

        private void runQueryResponseSingleScenario() {

            System.out.println();
            System.out.println("========== running queryResponseSingle scenario ==========");
            System.out.println("number of PVs: " + QUERY_SINGLE_NUM_PVS);
            System.out.println("number of PVs per request: " + QUERY_SINGLE_NUM_PVS_PER_REQUEST);
            System.out.println("number of threads: " + QUERY_NUM_THREADS);

            final long startSeconds = configMgr().getConfigLong(
                    IngestionBenchmarkBase.CFG_KEY_START_SECONDS,
                    IngestionBenchmarkBase.DEFAULT_START_SECONDS);

            IntegrationTestQueryResponseSingleApp queryResponseSingleApp =
                    new IntegrationTestQueryResponseSingleApp();
            BenchmarkScenarioResult scenarioResult = queryResponseSingleApp.queryScenario(
                    channel, QUERY_SINGLE_NUM_PVS, QUERY_SINGLE_NUM_PVS_PER_REQUEST, QUERY_NUM_THREADS, startSeconds);
            assertTrue(scenarioResult.success);

            System.out.println("========== queryResponseSingle scenario completed ==========");
            System.out.println();
        }

        private void runQueryResponseTableScenario() {

            System.out.println();
            System.out.println("========== running queryResponseTable scenario ==========");
            System.out.println("number of PVs: " + QUERY_TABLE_NUM_PVS);
            System.out.println("number of PVs per request: " + QUERY_TABLE_NUM_PVS_PER_REQUEST);
            System.out.println("number of threads: " + QUERY_NUM_THREADS);

            final long startSeconds = configMgr().getConfigLong(
                    IngestionBenchmarkBase.CFG_KEY_START_SECONDS,
                    IngestionBenchmarkBase.DEFAULT_START_SECONDS);

            IntegrationTestQueryResponseTableApp queryResponseTableApp =
                    new IntegrationTestQueryResponseTableApp();
            BenchmarkScenarioResult scenarioResult = queryResponseTableApp.queryScenario(
                    channel, QUERY_TABLE_NUM_PVS, QUERY_TABLE_NUM_PVS_PER_REQUEST, QUERY_NUM_THREADS, startSeconds);
            assertTrue(scenarioResult.success);

            System.out.println("========== queryResponseTable scenario completed ==========");
            System.out.println();
        }

    }

    @BeforeClass
    public static void setUp() throws Exception {

        GrpcIntegrationTestBase.setUp();

        // Create a grpcClient using the in-process channel;
        ingestionGrpcClient = new IntegrationTestIngestionGrpcClient(ingestionChannel);

        // Create a grpcClient using the in-process channel;
        queryGrpcClient = new IntegrationTestQueryGrpcClient(queryChannel);
    }

    @AfterClass
    public static void tearDown() {
        GrpcIntegrationTestBase.tearDown();
        ingestionGrpcClient = null;
        queryGrpcClient = null;
    }

    /**
     * Provides test coverage for a valid ingestion request stream.
     */
    @Test
    public void runIntegrationTestScenarios() {

        // run and verify ingestion scenario
        ingestionGrpcClient.runStreamingIngestionScenario();

        // run and verify bidirectional stream query api scenario
        queryGrpcClient.runQueryResponseCursorScenario();

        // run and verify server-streaming query api scenario
        queryGrpcClient.runQueryResponseStreamScenario();

        // run and verify single response query api scenario
        queryGrpcClient.runQueryResponseSingleScenario();

        // run and verify table response query api scenario
        queryGrpcClient.runQueryResponseTableScenario();
    }

}
