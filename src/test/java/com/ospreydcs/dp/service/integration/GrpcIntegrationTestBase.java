package com.ospreydcs.dp.service.integration;

import com.ospreydcs.dp.grpc.v1.ingestion.IngestDataRequest;
import com.ospreydcs.dp.grpc.v1.ingestion.IngestDataResponse;
import com.ospreydcs.dp.grpc.v1.query.*;
import com.ospreydcs.dp.service.common.config.ConfigurationManager;
import com.ospreydcs.dp.grpc.v1.common.*;
import com.ospreydcs.dp.grpc.v1.ingestion.DpIngestionServiceGrpc;
import com.ospreydcs.dp.service.common.bson.bucket.BucketDocument;
import com.ospreydcs.dp.service.common.bson.RequestStatusDocument;
import com.ospreydcs.dp.service.common.model.TimestampMap;
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

import java.time.Instant;
import java.util.*;

import static com.mongodb.client.model.Filters.and;
import static com.mongodb.client.model.Filters.eq;
import static org.junit.Assert.*;
import static org.mockito.AdditionalAnswers.delegatesTo;
import static org.mockito.Mockito.mock;

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

    protected static class IngestionColumnInfo {

        // instance variables
        public final String columnName;
        public final String requestIdBase;
        public final long measurementInterval;
        public final int numBuckets;
        public final int numSecondsPerBucket;

        public IngestionColumnInfo(
                String columnName,
                String requestIdBase,
                long measurementInterval,
                int numBuckets,
                int numSecondsPerBucket
        ) {
            this.columnName = columnName;
            this.requestIdBase = requestIdBase;
            this.measurementInterval = measurementInterval;
            this.numBuckets = numBuckets;
            this.numSecondsPerBucket = numSecondsPerBucket;
        }
    }

    protected static class IngestionBucketInfo {

        // instance variables
        public final int providerId;
        public final String requestId;
        public final long startSeconds;
        public final long startNanos;
        public final long endSeconds;
        public final long endNanos;
        public final int numValues;
        public final long intervalNanos;
        public final List<Object> dataValues;

        public IngestionBucketInfo(
                int providerId,
                String requestId,
                long startSeconds,
                long startNanos,
                long endSeconds,
                long endNanos,
                int numValues,
                long intervalNanos,
                List<Object> dataValues
        ) {
            this.providerId = providerId;
            this.requestId = requestId;
            this.startSeconds = startSeconds;
            this.startNanos = startNanos;
            this.endSeconds = endSeconds;
            this.endNanos = endNanos;
            this.numValues = numValues;
            this.intervalNanos = intervalNanos;
            this.dataValues = dataValues;
        }
    }

    protected static class IngestionStreamInfo {

        // instance variables
        final public TimestampMap<IngestionBucketInfo> bucketInfoMap;
        final public TimestampMap<Double> valueMap;

        public IngestionStreamInfo(
                TimestampMap<IngestionBucketInfo> bucketInfoMap, TimestampMap<Double> valueMap
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

    protected List<IngestDataResponse> sendIngestDataStream(List<IngestDataRequest> requestList) {

        final DpIngestionServiceGrpc.DpIngestionServiceStub asyncStub =
                DpIngestionServiceGrpc.newStub(ingestionChannel);

        final IngestionTestBase.IngestionResponseObserver responseObserver =
                new IngestionTestBase.IngestionResponseObserver(requestList.size());

        StreamObserver<IngestDataRequest> requestObserver = asyncStub.ingestDataStream(responseObserver);

        for (IngestDataRequest request : requestList) {
            // send request in separate thread to better simulate out of process grpc,
            // otherwise service handles request in this thread
            new Thread(() -> {
                requestObserver.onNext(request);
            }).start();
        }

        responseObserver.await();
        requestObserver.onCompleted();

        if (responseObserver.isError()) {
            return new ArrayList<>();
        } else {
            return responseObserver.getResponseList();
        }
    }

    protected IngestionStreamInfo ingestDataStream(
            long startSeconds,
            long startNanos,
            int providerId,
            String requestIdBase,
            long measurementInterval,
            String columnName,
            int numBuckets,
            int numSecondsPerBucket
    ) {
        final int numSamplesPerSecond = ((int) (1_000_000_000 / measurementInterval));
        final int numSamplesPerBucket = numSamplesPerSecond * numSecondsPerBucket;

        // create data structures for later validation
        final TimestampMap<Double> valueMap = new TimestampMap<>();
        final TimestampMap<IngestionBucketInfo> bucketInfoMap = new TimestampMap<>();

        // create requests
        final List<IngestDataRequest> requestList = new ArrayList<>();
        long currentSeconds = startSeconds;
        int secondsCount = 0;
        for (int bucketIndex = 0; bucketIndex < numBuckets; ++bucketIndex) {

            final String requestId = requestIdBase + bucketIndex;

            // create list of column data values for request
            final List<List<Object>> columnValues = new ArrayList<>();
            final List<Object> dataValuesList = new ArrayList<>();
            for (int secondIndex = 0; secondIndex < numSecondsPerBucket; ++secondIndex) {
                long currentNanos = 0;

                for (int sampleIndex = 0; sampleIndex < numSamplesPerSecond; ++sampleIndex) {
                    final double dataValue =
                            secondsCount + (double) sampleIndex / numSamplesPerSecond;
                    dataValuesList.add(dataValue);
                    valueMap.put(currentSeconds + secondIndex, currentNanos, dataValue);
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
                            IngestionTestBase.IngestionDataType.DOUBLE,
                            columnValues);

            final Instant startTimeInstant = Instant.ofEpochSecond(currentSeconds, startNanos);
            final Instant endTimeInstant =
                    startTimeInstant.plusNanos(measurementInterval * (numSamplesPerBucket - 1));

            // capture data for later validation
            final IngestionBucketInfo bucketInfo =
                    new IngestionBucketInfo(
                            providerId,
                            requestId,
                            currentSeconds,
                            startNanos,
                            endTimeInstant.getEpochSecond(),
                            endTimeInstant.getNano(),
                            numSamplesPerBucket,
                            measurementInterval,
                            dataValuesList);
            bucketInfoMap.put(currentSeconds, startNanos, bucketInfo);

            // build request
            final IngestDataRequest request = IngestionTestBase.buildIngestionRequest(params);
            requestList.add(request);

            currentSeconds = currentSeconds + numSecondsPerBucket;
        }

        // send requests
        final List<IngestDataResponse> responseList = sendIngestDataStream(requestList);
        assertEquals(requestList.size(), responseList.size());
        for (IngestDataResponse response : responseList) {
            assertTrue(response.hasAckResult());
            final IngestDataResponse.AckResult ackResult = response.getAckResult();
            assertEquals(1, ackResult.getNumColumns());
            assertEquals(numSamplesPerBucket, ackResult.getNumRows());
        }

        return new IngestionStreamInfo(bucketInfoMap, valueMap);
    }

    protected Map<String, IngestionStreamInfo> ingestDataStreamFromColumn(
            List<IngestionColumnInfo> columnInfoList,
            long startSeconds,
            long startNanos,
            int providerId
    ) {
        // create data structure for validating query result
        Map<String, IngestionStreamInfo> validationMap = new TreeMap<>();

        for (IngestionColumnInfo columnInfo : columnInfoList) {
            final IngestionStreamInfo streamInfo =
                    ingestDataStream(
                            startSeconds,
                            startNanos,
                            providerId,
                            columnInfo.requestIdBase,
                            columnInfo.measurementInterval,
                            columnInfo.columnName,
                            columnInfo.numBuckets,
                            columnInfo.numSecondsPerBucket);
            validationMap.put(columnInfo.columnName, streamInfo);
        }

        return validationMap;
    }

    protected void verifyIngestionDbArtifacts(Map<String, IngestionStreamInfo> validationMap) {

        for (var columnIngestionStreamInfoEntry : validationMap.entrySet()) {
            final String columnName = columnIngestionStreamInfoEntry.getKey();
            final IngestionStreamInfo columnIngestionInfo = columnIngestionStreamInfoEntry.getValue();

            for (var bucketInfoMapEntry : columnIngestionInfo.bucketInfoMap.entrySet()) {
                final long second = bucketInfoMapEntry.getKey();
                final Map<Long, IngestionBucketInfo> nanoMap = bucketInfoMapEntry.getValue();
                for (var nanoMapEntry : nanoMap.entrySet()) {
                    final long nano = nanoMapEntry.getKey();
                    final IngestionBucketInfo requestBucketInfo = nanoMapEntry.getValue();

                    // validate request in database
                    RequestStatusDocument statusDocument =
                            mongoClient.findRequestStatus(requestBucketInfo.providerId, requestBucketInfo.requestId);
                    assertEquals(1, statusDocument.getIdsCreated().size());
                    final String bucketId = columnName + "-"
                            + requestBucketInfo.startSeconds
                            + "-"
                            + requestBucketInfo.startNanos;
                    assertTrue(statusDocument.getIdsCreated().contains(bucketId));

                    // validate bucket in database
                    BucketDocument bucketDocument = mongoClient.findBucket(bucketId);
                    assertNotNull("bucketId: " + bucketId, bucketDocument);
                    assertEquals(columnName, bucketDocument.getColumnName());
                    assertEquals(bucketId, bucketDocument.getId());
                    assertEquals(requestBucketInfo.numValues, bucketDocument.getNumSamples());
                    assertEquals(requestBucketInfo.intervalNanos, bucketDocument.getSampleFrequency());
                    assertEquals(requestBucketInfo.startSeconds, bucketDocument.getFirstSeconds());
                    assertEquals(requestBucketInfo.startNanos, bucketDocument.getFirstNanos());
                    assertEquals(
                            Date.from(Instant.ofEpochSecond(
                                    requestBucketInfo.startSeconds, requestBucketInfo.startNanos)),
                            bucketDocument.getFirstTime());
                    assertEquals(requestBucketInfo.endSeconds, bucketDocument.getLastSeconds());
                    assertEquals(requestBucketInfo.endNanos, bucketDocument.getLastNanos());
                    assertEquals(
                            Date.from(Instant.ofEpochSecond(
                                    requestBucketInfo.endSeconds, requestBucketInfo.endNanos)),
                            bucketDocument.getLastTime());
                    assertEquals(requestBucketInfo.numValues, bucketDocument.getColumnDataList().size());
                    // verify each value
                    for (int valIndex = 0 ; valIndex < bucketDocument.getNumSamples() ; ++valIndex) {
                        Object expectedValue = requestBucketInfo.dataValues.get(valIndex);
                        assertTrue(expectedValue instanceof Double);
                        final double expectedValueDouble = (Double) expectedValue;
                        assertEquals(expectedValueDouble, bucketDocument.getColumnDataList().get(valIndex));
                    }
                }
            }
        }
    }

    protected QueryTableResponse.TableResult sendQueryDataTable(QueryDataRequest request) {

        final DpQueryServiceGrpc.DpQueryServiceStub asyncStub = DpQueryServiceGrpc.newStub(queryChannel);

        final QueryTestBase.QueryResponseTableObserver responseObserver =
                new QueryTestBase.QueryResponseTableObserver();

        // send request in separate thread to better simulate out of process grpc,
        // otherwise service handles request in this thread
        new Thread(() -> {
            asyncStub.queryDataTable(request, responseObserver);
        }).start();

        responseObserver.await();

        if (responseObserver.isError()) {
            return null;
        } else {
            final QueryTableResponse response = responseObserver.getQueryResponse();
            return response.getTableResult();
        }
    }

    protected QueryTableResponse.TableResult queryDataTable(
            List<String> columnNames, long startSeconds, long startNanos, long endSeconds, long endNanos
    ) {
        final QueryTestBase.QueryDataRequestParams params =
                new QueryTestBase.QueryDataRequestParams(columnNames, startSeconds, startNanos, endSeconds, endNanos);
        final QueryDataRequest request = QueryTestBase.buildQueryDataRequest(params);
        return sendQueryDataTable(request);
    }

    protected void sendAndVerifyQueryDataTable(
            int numRowsExpected,
            List<String> columnNames,
            long startSeconds,
            long startNanos,
            long endSeconds,
            long endNanos,
            Map<String, IngestionStreamInfo> validationMap
    ) {
        final QueryTableResponse.TableResult table =
                queryDataTable(
                        columnNames,
                        startSeconds,
                        startNanos,
                        endSeconds,
                        endNanos);

        // validate table contents
        final List<Timestamp> timestampList = table.getDataTimestamps().getTimestampList().getTimestampsList();
        assertEquals(numRowsExpected, timestampList.size());
        assertEquals(columnNames.size(), table.getDataColumnsCount());
        int rowIndex = 0;
        for (Timestamp timestamp : timestampList) {
            final long timestampSeconds = timestamp.getEpochSeconds();
            final long timestampNanos = timestamp.getNanoseconds();
            for (DataColumn dataColumn : table.getDataColumnsList()) {
                // get column name and value from query result
                String columnName = dataColumn.getName();
                Double columnDataValue = dataColumn.getDataValues(rowIndex).getDoubleValue();

                // get expected value from validation map
                final TimestampMap<Double> columnValueMap = validationMap.get(columnName).valueMap;
                Double expectedColumnDataValue = columnValueMap.get(timestampSeconds, timestampNanos);
                if (expectedColumnDataValue != null) {
                    assertEquals(expectedColumnDataValue, columnDataValue, 0.0);
                } else {
                    assertEquals(0.0, columnDataValue, 0.0);
                }
            }
            rowIndex = rowIndex + 1;
        }
    }

    protected List<QueryDataResponse.QueryData.DataBucket> sendQueryDataStream(QueryDataRequest request) {

        final DpQueryServiceGrpc.DpQueryServiceStub asyncStub = DpQueryServiceGrpc.newStub(queryChannel);

        final QueryTestBase.QueryResponseStreamObserver responseObserver =
                new QueryTestBase.QueryResponseStreamObserver();

        // send request in separate thread to better simulate out of process grpc,
        // otherwise service handles request in this thread
        new Thread(() -> {
            asyncStub.queryDataStream(request, responseObserver);
        }).start();

        responseObserver.await();

        if (responseObserver.isError()) {
            return null;
        } else {
            return responseObserver.getDataBucketList();
        }
    }

    protected List<QueryDataResponse.QueryData.DataBucket> queryDataStream(
            List<String> pvNames,
            long startSeconds,
            long startNanos,
            long endSeconds,
            long endNanos
    ) {
        final QueryTestBase.QueryDataRequestParams params =
                new QueryTestBase.QueryDataRequestParams(pvNames, startSeconds, startNanos, endSeconds, endNanos);
        final QueryDataRequest request = QueryTestBase.buildQueryDataRequest(params);
        return sendQueryDataStream(request);
    }

    protected void sendAndVerifyQueryDataStream(
            int numBucketsExpected,
            List<String> pvNames,
            long startSeconds,
            long startNanos,
            long endSeconds,
            long endNanos,
            Map<String, IngestionStreamInfo> validationMap
    ) {
        final List<QueryDataResponse.QueryData.DataBucket> dataBucketList =
                queryDataStream(pvNames, startSeconds, startNanos, endSeconds, endNanos);

        // build map of buckets in query response for vallidation
        Map<String, TimestampMap<QueryDataResponse.QueryData.DataBucket>> responseBucketMap =
                new TreeMap<>();
        for (QueryDataResponse.QueryData.DataBucket dataBucket : dataBucketList) {
            final String bucketColumnName = dataBucket.getDataColumn().getName();
            final Timestamp bucketStartTimestamp = dataBucket.getDataTimestamps().getSamplingClock().getStartTime();
            final long bucketStartSeconds = bucketStartTimestamp.getEpochSeconds();
            final long bucketStartNanos = bucketStartTimestamp.getNanoseconds();
            TimestampMap<QueryDataResponse.QueryData.DataBucket> columnTimestampMap =
                    responseBucketMap.get(bucketColumnName);
            if (columnTimestampMap == null) {
                columnTimestampMap = new TimestampMap<>();
                responseBucketMap.put(bucketColumnName, columnTimestampMap);
            }
            columnTimestampMap.put(bucketStartSeconds, bucketStartNanos, dataBucket);
        }

        // iterate through the expected buckets for each column,
        // and validate them against the corresponding response bucket
        int validatedBuckets = 0;
        for (var validationMapEntry : validationMap.entrySet()) {
            final String columnName = validationMapEntry.getKey();
            final IngestionStreamInfo columnStreamInfo = validationMapEntry.getValue();
            for (var bucketInfoMapEntry : columnStreamInfo.bucketInfoMap.entrySet()) {
                final long bucketSecond = bucketInfoMapEntry.getKey();
                final Map<Long, IngestionBucketInfo> bucketNanoMap = bucketInfoMapEntry.getValue();
                for (IngestionBucketInfo columnBucketInfo : bucketNanoMap.values()) {

                    // skip buckets outside the query range
                    if ((columnBucketInfo.startSeconds > endSeconds)
                            || ((columnBucketInfo.startSeconds == endSeconds) && (columnBucketInfo.startNanos >= endNanos))) {
                        // bucket starts after query end time
                        continue;
                    }
                    if ((columnBucketInfo.endSeconds < startSeconds)
                            || ((columnBucketInfo.endSeconds == startSeconds) && (columnBucketInfo.endNanos < startNanos))) {
                        // bucket ends before query start time
                        continue;
                    }

                    // find the response bucket corresponding to the expected bucket
                    final QueryDataResponse.QueryData.DataBucket responseBucket =
                            responseBucketMap.get(columnName).get(columnBucketInfo.startSeconds, startNanos);

                    assertEquals(
                            columnBucketInfo.intervalNanos,
                            responseBucket.getDataTimestamps().getSamplingClock().getPeriodNanos());
                    assertEquals(
                            columnBucketInfo.numValues,
                            responseBucket.getDataTimestamps().getSamplingClock().getCount());

                    // validate bucket data values
                    int valueIndex = 0;
                    for (DataValue responseDataValue : responseBucket.getDataColumn().getDataValuesList()) {

                        final double actualDataValue = responseDataValue.getDoubleValue();

                        Object expectedValue = columnBucketInfo.dataValues.get(valueIndex);
                        assertTrue(expectedValue instanceof Double);
                        Double expectedDataValue = (Double) expectedValue;
                        assertEquals(expectedDataValue, actualDataValue, 0.0);

                        valueIndex = valueIndex + 1;
                    }

                    validatedBuckets = validatedBuckets + 1;
                }
            }
        }

        // check that we validated all buckets returned by the query, and that query returned expected number of buckets
        assertEquals(dataBucketList.size(), validatedBuckets);
        assertEquals(numBucketsExpected, dataBucketList.size());
    }

    protected List<QueryMetadataResponse.MetadataResult.PvInfo> sendQueryMetadata(
            QueryMetadataRequest request, boolean expectReject, String expectedRejectMessage
    ) {
        final DpQueryServiceGrpc.DpQueryServiceStub asyncStub = DpQueryServiceGrpc.newStub(queryChannel);

        final QueryTestBase.QueryMetadataResponseObserver responseObserver =
                new QueryTestBase.QueryMetadataResponseObserver();

        // send request in separate thread to better simulate out of process grpc,
        // otherwise service handles request in this thread
        new Thread(() -> {
            asyncStub.queryMetadata(request, responseObserver);
        }).start();

        responseObserver.await();

        if (expectReject) {
            assertTrue(responseObserver.isError());
            assertTrue(responseObserver.getErrorMessage().contains(expectedRejectMessage));
        } else {
            assertFalse(responseObserver.getErrorMessage(), responseObserver.isError());
        }

        return responseObserver.getPvInfoList();
    }

    private void sendAndVerifyQueryMetadata(
            QueryMetadataRequest request,
            List<String> columnNames,
            Map<String, IngestionStreamInfo> validationMap,
            boolean expectReject,
            String expectedRejectMessage
    ) {
        final List<QueryMetadataResponse.MetadataResult.PvInfo> pvInfoList =
                sendQueryMetadata(request, expectReject, expectedRejectMessage);

        if (expectReject) {
            assertEquals(0, pvInfoList.size());
            return;
        }

        // verify results, check that there is a ColumnInfo for each column in the query
        assertEquals(columnNames.size(), pvInfoList.size());

        // build map of column info list for convenience
        final Map<String, QueryMetadataResponse.MetadataResult.PvInfo> columnInfoMap = new HashMap<>();
        for (QueryMetadataResponse.MetadataResult.PvInfo columnInfo : pvInfoList) {
            columnInfoMap.put(columnInfo.getPvName(), columnInfo);
        }

        // check that a column info was received for each name and verify its contents
        for (String columnName : columnNames) {
            final QueryMetadataResponse.MetadataResult.PvInfo columnInfoForName =
                    columnInfoMap.get(columnName);
            assertNotNull(columnInfoForName);
            assertEquals(columnName, columnInfoForName.getPvName());
            assertEquals("DOUBLE", columnInfoForName.getLastBucketDataType());

            // iterate through validationMap to get info for first and last bucket for column
            IngestionBucketInfo firstBucketInfo = null;
            IngestionBucketInfo lastBucketInfo = null;
            boolean first = true;
            for (var bucketMapEntry : validationMap.get(columnName).bucketInfoMap.entrySet()) {
                final var nanoMap = bucketMapEntry.getValue();
                for (var nanoMapEntry : nanoMap.entrySet()) {
                    if (first) {
                        firstBucketInfo = nanoMapEntry.getValue();
                        first = false;
                    }
                    lastBucketInfo = nanoMapEntry.getValue();
                }
            }

            // verify ColumnInfo contents for column against last and first bucket details
            assertNotNull(lastBucketInfo);
            assertEquals(lastBucketInfo.intervalNanos, columnInfoForName.getLastSamplingClock().getPeriodNanos());
            assertEquals(lastBucketInfo.numValues, columnInfoForName.getLastSamplingClock().getCount());
            assertEquals(lastBucketInfo.endSeconds, columnInfoForName.getLastTimestamp().getEpochSeconds());
            assertEquals(lastBucketInfo.endNanos, columnInfoForName.getLastTimestamp().getNanoseconds());
            assertNotNull(firstBucketInfo);
            assertEquals(firstBucketInfo.startSeconds, columnInfoForName.getFirstTimestamp().getEpochSeconds());
            assertEquals(firstBucketInfo.startNanos, columnInfoForName.getFirstTimestamp().getNanoseconds());
        }
    }

    protected void sendAndVerifyQueryMetadata(
            List<String> columnNames,
            Map<String, IngestionStreamInfo> validationMap,
            boolean expectReject,
            String expectedRejectMessage
    ) {
        final QueryMetadataRequest request = QueryTestBase.buildQueryMetadataRequest(columnNames);
        sendAndVerifyQueryMetadata(request, columnNames, validationMap, expectReject, expectedRejectMessage);
    }

    protected void sendAndVerifyQueryMetadata(
            String columnNamePattern,
            Map<String, IngestionStreamInfo> validationMap,
            List<String> expectedColumnNames,
            boolean expectReject,
            String expectedRejectMessage
    ) {
        final QueryMetadataRequest request = QueryTestBase.buildQueryMetadataRequest(columnNamePattern);
        sendAndVerifyQueryMetadata(request, expectedColumnNames, validationMap, expectReject, expectedRejectMessage);
    }

}
