package com.ospreydcs.dp.service.integration;

import com.ospreydcs.dp.common.config.ConfigurationManager;
import com.ospreydcs.dp.grpc.v1.common.*;
import com.ospreydcs.dp.grpc.v1.ingestion.DpIngestionServiceGrpc;
import com.ospreydcs.dp.grpc.v1.ingestion.IngestionRequest;
import com.ospreydcs.dp.grpc.v1.ingestion.IngestionResponse;
import com.ospreydcs.dp.grpc.v1.query.DpQueryServiceGrpc;
import com.ospreydcs.dp.grpc.v1.query.QueryRequest;
import com.ospreydcs.dp.grpc.v1.query.QueryResponse;
import com.ospreydcs.dp.service.common.bson.BucketDocument;
import com.ospreydcs.dp.service.common.bson.RequestStatusDocument;
import com.ospreydcs.dp.service.common.mongo.MongoSyncClient;
import com.ospreydcs.dp.service.ingest.IngestionTestBase;
import com.ospreydcs.dp.service.ingest.handler.IngestionHandlerInterface;
import com.ospreydcs.dp.service.ingest.handler.mongo.MongoIngestionHandler;
import com.ospreydcs.dp.service.ingest.service.IngestionServiceImpl;
import com.ospreydcs.dp.service.query.QueryTestBase;
import com.ospreydcs.dp.service.query.handler.interfaces.QueryHandlerInterface;
import com.ospreydcs.dp.service.query.handler.mongo.MongoQueryHandler;
import com.ospreydcs.dp.service.query.service.QueryServiceImpl;
import io.grpc.ManagedChannel;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import io.grpc.stub.StreamObserver;
import io.grpc.testing.GrpcCleanupRule;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.bson.conversions.Bson;
import org.junit.ClassRule;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.time.Instant;
import java.util.*;

import static com.mongodb.client.model.Filters.and;
import static com.mongodb.client.model.Filters.eq;
import static org.junit.Assert.*;
import static org.mockito.AdditionalAnswers.delegatesTo;
import static org.mockito.Mockito.mock;

@RunWith(JUnit4.class)
public class GrpcIntegrationTestBase {

    // static variables
    private static final Logger logger = LogManager.getLogger();
    /**
     * This rule manages automatic graceful shutdown for the registered servers and channels at the
     * end of test.
     */
    @ClassRule
    public static final GrpcCleanupRule grpcCleanup = new GrpcCleanupRule();
    protected static IntegrationTestMongoClient mongoClient;

    // ingestion service instance variables
    private static IngestionServiceImpl ingestionService;
    private static IngestionServiceImpl ingestionServiceMock;
    protected static ManagedChannel ingestionChannel;

    // query service instance variables
    private static QueryServiceImpl queryService;
    private static QueryServiceImpl queryServiceMock;
    protected static ManagedChannel queryChannel;

    protected static ConfigurationManager configMgr() {
        return ConfigurationManager.getInstance();
    }

    protected static class IntegrationTestMongoClient extends MongoSyncClient {

        private static final int MONGO_FIND_RETRY_COUNT = 300;
        private static final int MONGO_FIND_RETRY_INTERVAL_MILLIS = 100;

        public BucketDocument findBucket(String id) {
            for (int retryCount = 0 ; retryCount < MONGO_FIND_RETRY_COUNT ; ++retryCount){
                List<BucketDocument> matchingBuckets = new ArrayList<>();
                mongoCollectionBuckets.find(eq("_id", id)).into(matchingBuckets);
                if (matchingBuckets.size() > 0) {
                    return matchingBuckets.get(0);
                } else {
                    try {
                        logger.info("findBucket id: " + id + " retrying");
                        Thread.sleep(MONGO_FIND_RETRY_INTERVAL_MILLIS);
                    } catch (InterruptedException ex) {
                        // ignore and just retry
                    }
                }
            }
            return null;
        }

        public RequestStatusDocument findRequestStatus(Integer providerId, String requestId) {
            for (int retryCount = 0 ; retryCount < MONGO_FIND_RETRY_COUNT ; ++retryCount) {
                List<RequestStatusDocument> matchingDocuments = new ArrayList<>();
                Bson filter = and(eq("providerId", providerId), eq("requestId", requestId));
                mongoCollectionRequestStatus.find(filter).into(matchingDocuments);
                if (matchingDocuments.size() > 0) {
                    return matchingDocuments.get(0);
                } else {
                    try {
                        logger.info("findRequestStatus providerId: " + providerId
                                + " requestId: " + requestId
                                + " retrying");
                        Thread.sleep(MONGO_FIND_RETRY_INTERVAL_MILLIS);
                    } catch (InterruptedException ex) {
                        // ignore and just retry
                    }
                }
            }
            return null;
        }
    }

    protected static class IngestionBucketInfo {

        // instance variables
        public final long startSeconds;
        public final long startNanos;
        public final long endSeconds;
        public final long endNanos;
        public final int numValues;
        public final long intervalNanos;

        public IngestionBucketInfo(
                long startSeconds, long startNanos, long endSeconds, long endNanos, int numValues, long intervalNanos
        ) {
            this.startSeconds = startSeconds;
            this.startNanos = startNanos;
            this.endSeconds = endSeconds;
            this.endNanos = endNanos;
            this.numValues = numValues;
            this.intervalNanos = intervalNanos;
        }
    }

    protected static class IngestionStreamInfo {

        // instance variables
        final public Map<Long, IngestionBucketInfo> bucketInfoMap;
        final public Map<Long, Map<Long, Double>> valueMap;

        public IngestionStreamInfo(
                Map<Long, IngestionBucketInfo> bucketInfoMap, Map<Long, Map<Long, Double>> valueMap
        ) {
            this.bucketInfoMap = bucketInfoMap;
            this.valueMap = valueMap;
        }
    }

    public static void setUp() throws Exception {

        // init the mongo client interface for db verification
        mongoClient = new IntegrationTestMongoClient();
        mongoClient.init();

        // init ingestion service
        IngestionHandlerInterface ingestionHandler = MongoIngestionHandler.newMongoSyncIngestionHandler();
        ingestionService = new IngestionServiceImpl();
        if (!ingestionService.init(ingestionHandler)) {
            fail("IngestionServiceImpl.init failed");
        }
        ingestionServiceMock = mock(IngestionServiceImpl.class, delegatesTo(ingestionService));
        // Generate a unique in-process server name.
        String ingestionServerName = InProcessServerBuilder.generateName();
        // Create a server, add service, start, and register for automatic graceful shutdown.
        grpcCleanup.register(InProcessServerBuilder
                .forName(ingestionServerName).directExecutor().addService(ingestionServiceMock).build().start());
        // Create a client channel and register for automatic graceful shutdown.
        ingestionChannel = grpcCleanup.register(
                InProcessChannelBuilder.forName(ingestionServerName).directExecutor().build());

        // init query service
        QueryHandlerInterface queryHandler = MongoQueryHandler.newMongoSyncQueryHandler();
        queryService = new QueryServiceImpl();
        if (!queryService.init(queryHandler)) {
            fail("QueryServiceImpl.init failed");
        }
        queryServiceMock = mock(QueryServiceImpl.class, delegatesTo(queryService));
        // Generate a unique in-process server name.
        String queryServerName = InProcessServerBuilder.generateName();
        // Create a server, add service, start, and register for automatic graceful shutdown.
        grpcCleanup.register(InProcessServerBuilder
                .forName(queryServerName).directExecutor().addService(queryServiceMock).build().start());
        // Create a client channel and register for automatic graceful shutdown.
        queryChannel = grpcCleanup.register(
                InProcessChannelBuilder.forName(queryServerName).directExecutor().build());
    }

    public static void tearDown() {
        ingestionService.fini();
        mongoClient.fini();
        mongoClient = null;
        ingestionServiceMock = null;
    }

    protected List<IngestionResponse> sendIngestionRequests(List<IngestionRequest> requestList) {

        final DpIngestionServiceGrpc.DpIngestionServiceStub asyncStub =
                DpIngestionServiceGrpc.newStub(ingestionChannel);

        final IngestionTestBase.IngestionResponseObserver responseObserver =
                new IngestionTestBase.IngestionResponseObserver(requestList.size());

        StreamObserver<IngestionRequest> requestObserver = asyncStub.streamingIngestion(responseObserver);

        for (IngestionRequest request : requestList) {
            requestObserver.onNext(request);
        }

        responseObserver.await();
        requestObserver.onCompleted();

        if (responseObserver.isError()) {
            return new ArrayList<>();
        } else {
            return responseObserver.getResponseList();
        }
    }

    protected IngestionStreamInfo streamingIngestion(
            long startSeconds,
            long startNanos,
            int providerId,
            String requestIdBase,
            long measurementInterval,
            String columnName,
            int numBuckets,
            int numSecondsPerBucket
    ) {
        final int numSamplesPerSecond = ((int)(1_000_000_000 / measurementInterval));
        final int numSamplesPerBucket = numSamplesPerSecond * numSecondsPerBucket;

        // create data structures for later validation
        Map<Long, Map<Long, Double>> valueMap = new TreeMap<>();
        Map<Long, IngestionBucketInfo> bucketInfoMap = new TreeMap<>();

        // create requests
        final List<IngestionRequest> requestList = new ArrayList<>();
        long currentSeconds = startSeconds;
        int secondsCount = 0;
        for (int bucketIndex = 0 ; bucketIndex < numBuckets ; ++bucketIndex) {

            final String requestId = requestIdBase + bucketIndex;

            // create list of column data values for request
            final List<List<Object>> columnValues = new ArrayList<>();
            final List<Object> dataValuesList = new ArrayList<>();
            for (int secondIndex = 0 ; secondIndex < numSecondsPerBucket ; ++secondIndex) {
                long currentNanos = 0;
                Map<Long, Double> nanoMap = new TreeMap<>();
                valueMap.put(currentSeconds + secondIndex, nanoMap);

                for (int sampleIndex = 0 ; sampleIndex < numSamplesPerSecond ; ++sampleIndex) {
                    final double dataValue =
                            secondsCount + (double) sampleIndex / numSamplesPerSecond;
                    dataValuesList.add(dataValue);
                    nanoMap.put(currentNanos, dataValue);
                    currentNanos = currentNanos + measurementInterval;
                }
                secondsCount = secondsCount + 1;
            }
            columnValues.add(dataValuesList);

            // create request parameters
            final IngestionTestBase.IngestionRequestParams params =
                    new IngestionTestBase.IngestionRequestParams(
                    providerId,
                    requestId,
                    null,
                    null,
                    null,
                    null,
                    currentSeconds,
                    startNanos,
                    measurementInterval,
                    numSamplesPerBucket,
                    List.of(columnName),
                    IngestionTestBase.IngestionDataType.FLOAT,
                    columnValues);

            Instant startTimeInstant = Instant.ofEpochSecond(currentSeconds, startNanos);
            Instant endTimeInstant = startTimeInstant.plusNanos(measurementInterval * (numSamplesPerBucket-1));

            // capture data for later validation
            IngestionBucketInfo bucketInfo =
                    new IngestionBucketInfo(
                            currentSeconds,
                            startNanos,
                            endTimeInstant.getEpochSecond(),
                            endTimeInstant.getNano(),
                            numSamplesPerBucket,
                            measurementInterval);
            bucketInfoMap.put(currentSeconds, bucketInfo);

            // build request
            final IngestionRequest request = IngestionTestBase.buildIngestionRequest(params);
            requestList.add(request);

            currentSeconds = currentSeconds + numSecondsPerBucket;
        }

        // send requests
        List<IngestionResponse> responseList = sendIngestionRequests(requestList);
        assertEquals(requestList.size(), responseList.size());

        return new IngestionStreamInfo(bucketInfoMap, valueMap);
    }

    protected DataTable sendQueryResponseTable(QueryRequest request) {

        final DpQueryServiceGrpc.DpQueryServiceStub asyncStub = DpQueryServiceGrpc.newStub(queryChannel);

        final QueryTestBase.QueryResponseTableObserver responseObserver =
                new QueryTestBase.QueryResponseTableObserver();

        asyncStub.queryResponseTable(request, responseObserver);

        responseObserver.await();

        if (responseObserver.isError()) {
            return null;
        } else {
            final QueryResponse response = responseObserver.getQueryResponse();
            return response.getQueryReport().getDataTable();
        }
    }

    protected DataTable queryResponseTable(
            List<String> columnNames, long startSeconds, long startNanos, long endSeconds, long endNanos
    ) {
        final QueryTestBase.QueryRequestParams params =
                new QueryTestBase.QueryRequestParams(columnNames, startSeconds, startNanos, endSeconds, endNanos);
        final QueryRequest request = QueryTestBase.buildQueryRequest(params);
        return sendQueryResponseTable(request);
    }

    protected void sendAndVerifyTableQuery(
            int numRowsExpected,
            List<String> columnNames,
            long startSeconds,
            long startNanos,
            long endSeconds,
            long endNanos,
            Map<String, IngestionStreamInfo> validationMap
    ) {
        final DataTable table =
                queryResponseTable(
                        columnNames,
                        startSeconds,
                        startNanos,
                        endSeconds,
                        endNanos);

        // validate table contents
        final List<Timestamp> timestampList = table.getDataTimeSpec().getTimestampList().getTimestampsList();
        assertEquals(numRowsExpected, timestampList.size());
        assertEquals(columnNames.size(), table.getDataColumnsCount());
        int rowIndex = 0;
        for (Timestamp timestamp : timestampList) {
            final long timestampSeconds = timestamp.getEpochSeconds();
            final long timestampNanos = timestamp.getNanoseconds();
            for (DataColumn dataColumn : table.getDataColumnsList()) {
                // get column name and value from query result
                String columnName = dataColumn.getName();
                Double columnDataValue = dataColumn.getDataValues(rowIndex).getFloatValue();

                // get expected value from validation map
                final Map<Long, Map<Long, Double>> columnValueMap = validationMap.get(columnName).valueMap;
                final Map<Long, Double> columnSecondMap = columnValueMap.get(timestampSeconds);
                assertNotNull(columnSecondMap);
                Double expectedColumnDataValue = columnSecondMap.get(timestampNanos);
                if (expectedColumnDataValue != null) {
                    assertEquals(expectedColumnDataValue, columnDataValue, 0.0);
                } else {
                    assertEquals(0.0, columnDataValue, 0.0);
                }
            }
            rowIndex = rowIndex + 1;
        }
    }

    protected List<QueryResponse.QueryReport.BucketData.DataBucket> sendQueryResponseStream(QueryRequest request) {

        final DpQueryServiceGrpc.DpQueryServiceStub asyncStub = DpQueryServiceGrpc.newStub(queryChannel);

        final QueryTestBase.QueryResponseStreamObserver responseObserver =
                new QueryTestBase.QueryResponseStreamObserver();

        asyncStub.queryResponseStream(request, responseObserver);

        responseObserver.await();

        if (responseObserver.isError()) {
            return null;
        } else {
            return responseObserver.getDataBucketList();
        }
    }

    protected List<QueryResponse.QueryReport.BucketData.DataBucket> queryResponseStream(
            List<String> columnNames,
            long startSeconds,
            long startNanos,
            long endSeconds,
            long endNanos
    ) {
        final QueryTestBase.QueryRequestParams params =
                new QueryTestBase.QueryRequestParams(columnNames, startSeconds, startNanos, endSeconds, endNanos);
        final QueryRequest request = QueryTestBase.buildQueryRequest(params);
        return sendQueryResponseStream(request);
    }

    protected void sendAndVerifyBucketQuery(
            int numBucketsExpected,
            List<String> columnNames,
            long startSeconds,
            long startNanos,
            long endSeconds,
            long endNanos,
            Map<String, IngestionStreamInfo> validationMap
    ) {
        final List<QueryResponse.QueryReport.BucketData.DataBucket> dataBucketList =
                queryResponseStream(columnNames, startSeconds, startNanos, endSeconds, endNanos);

        // build map of buckets in query response for vallidation
        Map<String, Map<Long, Map<Long, QueryResponse.QueryReport.BucketData.DataBucket>>> responseBucketMap =
                new TreeMap<>();
        for (QueryResponse.QueryReport.BucketData.DataBucket dataBucket : dataBucketList) {
            final String bucketColumnName = dataBucket.getDataColumn().getName();
            final Timestamp bucketStartTimestamp = dataBucket.getSamplingInterval().getStartTime();
            final long bucketStartSeconds = bucketStartTimestamp.getEpochSeconds();
            final long bucketStartNanos = bucketStartTimestamp.getNanoseconds();
            Map<Long, Map<Long, QueryResponse.QueryReport.BucketData.DataBucket>> columnSecondMap =
                    responseBucketMap.get(bucketColumnName);
            if (columnSecondMap == null) {
                columnSecondMap = new TreeMap<>();
                responseBucketMap.put(bucketColumnName, columnSecondMap);
            }
            Map<Long, QueryResponse.QueryReport.BucketData.DataBucket> nanoMap =
                    columnSecondMap.get(bucketStartSeconds);
            if (nanoMap == null) {
                nanoMap = new TreeMap<>();
                columnSecondMap.put(bucketStartSeconds, nanoMap);
            }
            nanoMap.put(bucketStartNanos, dataBucket);
        }

        // iterate through the expected buckets for each column,
        // and validate them against the corresponding response bucket
        int validatedBuckets = 0;
        for (var validationMapEntry : validationMap.entrySet()) {
            final String columnName = validationMapEntry.getKey();
            final IngestionStreamInfo columnStreamInfo = validationMapEntry.getValue();
            for (var bucketInfoMapEntry : columnStreamInfo.bucketInfoMap.entrySet()) {
                final IngestionBucketInfo columnBucketInfo = bucketInfoMapEntry.getValue();

                // skip buckets outside the query range
                if ((columnBucketInfo.startSeconds > endSeconds)
                        || ((columnBucketInfo.startSeconds == endSeconds) && (columnBucketInfo.startNanos >= endNanos)))
                {
                    // bucket starts after query end time
                    continue;
                }
                if ((columnBucketInfo.endSeconds < startSeconds)
                        || ((columnBucketInfo.endSeconds == startSeconds) && (columnBucketInfo.endNanos < startNanos)))
                {
                    // bucket ends before query start time
                    continue;
                }

                // find the response bucket corresponding to the expected bucket
                final QueryResponse.QueryReport.BucketData.DataBucket responseBucket =
                        responseBucketMap.get(columnName).get(columnBucketInfo.startSeconds).get(columnBucketInfo.startNanos);

                assertEquals(columnBucketInfo.intervalNanos, responseBucket.getSamplingInterval().getSampleIntervalNanos());
                assertEquals(columnBucketInfo.numValues, responseBucket.getSamplingInterval().getNumSamples());

                // validate bucket data values
                final FixedIntervalTimestampSpec responseBucketTimeSpec = responseBucket.getSamplingInterval();
                long responseBucketSecond = responseBucketTimeSpec.getStartTime().getEpochSeconds();
                long responseBucketNano = responseBucketTimeSpec.getStartTime().getNanoseconds();
                final long responseBucketInterval = responseBucketTimeSpec.getSampleIntervalNanos();
                for (DataValue responseDataValue : responseBucket.getDataColumn().getDataValuesList()) {

                    // get value from query response
                    final double responseDataValueFloat = responseDataValue.getFloatValue();

                    // get expected value
                    final Map<Long, Double> columnSecondMap = columnStreamInfo.valueMap.get(responseBucketSecond);
                    final double expectedDataValue = columnSecondMap.get(responseBucketNano);

                    // compare
                    assertEquals(expectedDataValue, responseDataValueFloat, 0.0);

                    // increment seconds and nanos
                    responseBucketNano = responseBucketNano + responseBucketInterval;
                    if (responseBucketNano >= 1_000_000_000) {
                        responseBucketSecond = responseBucketSecond + 1;
                        responseBucketNano = responseBucketNano - 1_000_000_000;
                    }
                }

                validatedBuckets = validatedBuckets + 1;
            }
        }

        // check that we validated all buckets returned by the query, and that query returned expected number of buckets
        assertEquals(dataBucketList.size(), validatedBuckets);
        assertEquals(numBucketsExpected, dataBucketList.size());
    }

}
