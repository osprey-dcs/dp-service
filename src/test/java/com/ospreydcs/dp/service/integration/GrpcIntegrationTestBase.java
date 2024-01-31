package com.ospreydcs.dp.service.integration;

import com.ospreydcs.dp.common.config.ConfigurationManager;
import com.ospreydcs.dp.grpc.v1.common.DataTable;
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

import java.util.*;

import static com.mongodb.client.model.Filters.and;
import static com.mongodb.client.model.Filters.eq;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
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

    protected Map<Long, Map<Long, Double>> streamingIngestion(
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
        final int numSeconds = numBuckets * numSecondsPerBucket;

        // create map to contain data values to use for later validation
        Map<Long, Map<Long, Double>> valueMap = new HashMap<>();

        // create requests
        final List<IngestionRequest> requestList = new ArrayList<>();
        long currentSeconds = startSeconds;
        int secondsCount = 0;
        for (int bucketIndex = 0 ; bucketIndex < numBuckets ; ++bucketIndex) {

            final String requestIdHundredths = requestIdBase + bucketIndex;

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
            final IngestionTestBase.IngestionRequestParams paramsHundredths =
                    new IngestionTestBase.IngestionRequestParams(
                    providerId,
                    requestIdHundredths,
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

            // build request
            final IngestionRequest request = IngestionTestBase.buildIngestionRequest(paramsHundredths);
            requestList.add(request);

            currentSeconds = currentSeconds + numSecondsPerBucket;
        }

        // send requests
        List<IngestionResponse> responseList = sendIngestionRequests(requestList);
        assertEquals(requestList.size(), responseList.size());

        return valueMap;
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

}
