package com.ospreydcs.dp.service.integration;

import com.ospreydcs.dp.grpc.v1.common.DataTable;
import com.ospreydcs.dp.grpc.v1.ingestion.IngestionRequest;
import com.ospreydcs.dp.grpc.v1.ingestion.IngestionResponse;
import com.ospreydcs.dp.service.ingest.IngestionTestBase;
import com.ospreydcs.dp.service.ingest.benchmark.BenchmarkStreamingIngestion;
import com.ospreydcs.dp.service.ingest.benchmark.IngestionBenchmarkBase;
import com.ospreydcs.dp.service.ingest.handler.IngestionHandlerInterface;
import com.ospreydcs.dp.service.ingest.handler.mongo.MongoIngestionHandler;
import com.ospreydcs.dp.service.ingest.service.IngestionServiceImpl;
import io.grpc.Channel;
import io.grpc.ManagedChannel;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import io.grpc.testing.GrpcCleanupRule;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import static org.junit.Assert.*;
import static org.mockito.AdditionalAnswers.delegatesTo;
import static org.mockito.Mockito.mock;

@RunWith(JUnit4.class)
public class IntegrationGrpcTest extends IngestionTestBase {

    private static final Logger LOGGER = LogManager.getLogger();

    private static class IntegrationTestStreamingIngestionApp extends BenchmarkStreamingIngestion {

        private static class IntegrationTestIngestionRequestInfo {
            public final int providerId;
            public boolean responseReceived = false;
            public IntegrationTestIngestionRequestInfo(int providerId) {
                this.providerId = providerId;
            }
        }

        private static class IntegrationTestIngestionTask
                extends BenchmarkStreamingIngestion.StreamingIngestionTask
        {
            // instance variables
            private Map<String, IntegrationTestIngestionRequestInfo> requestValidationMap = new HashMap<>();
            private final ReentrantReadWriteLock rwLock = new ReentrantReadWriteLock();
            private final Lock readLock = rwLock.readLock();
            private final Lock writeLock = rwLock.writeLock();

            public IntegrationTestIngestionTask(
                    IngestionBenchmarkBase.IngestionTaskParams params,
                    DataTable.Builder templateDataTable,
                    Channel channel) {

                super(params, templateDataTable, channel);
            }

            @Override
            protected void onRequest(IngestionRequest request) {

                LOGGER.debug("onRequest stream: " + this.params.streamNumber);

                // acquire writeLock for updating map
                writeLock.lock();
                try {
                    // add an entry for the request to the validation map
                    requestValidationMap.put(
                            request.getClientRequestId(),
                            new IntegrationTestIngestionRequestInfo(request.getProviderId()));

                } finally {
                    // using try...finally to make sure we unlock!
                    writeLock.unlock();
                }
            }

            @Override
            protected void onResponse(IngestionResponse response) {

                LOGGER.debug("onResponse stream: " + this.params.streamNumber);

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

            @Override
            protected void onCompleted() {

                LOGGER.debug("onCompleted stream: " + this.params.streamNumber);

                readLock.lock();
                try {
                    // iterate through requestMap and make sure all requests were acked/verified
                    for (var entry : requestValidationMap.entrySet()) {
                        IntegrationTestIngestionRequestInfo requestInfo = entry.getValue();
                        if (!requestInfo.responseReceived) {
                            fail("did not receive ack for request: " + entry.getKey());
                        }
                    }

                } finally {
                    readLock.unlock();
                }
            }

        }

        protected StreamingIngestionTask newIngestionTask(
                IngestionTaskParams params, DataTable.Builder templateDataTable, Channel channel
        ) {
            return new IntegrationTestIngestionTask(params, templateDataTable, channel);
        }
    }

    protected static class IntegrationTestGrpcClient {

        private static final int INGESTION_NUM_PVS = 4000;
        private static final int INGESTION_NUM_THREADS = 7;
        private static final int INGESTION_NUM_STREAMS = 20;
        private static final int INGESTION_NUM_ROWS = 1000;
        private static final int INGESTION_NUM_SECONDS = 60;
        final private Channel channel;

        public IntegrationTestGrpcClient(Channel channel) {
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
            ingestionApp.ingestionScenario(
                    channel,
                    INGESTION_NUM_THREADS,
                    INGESTION_NUM_STREAMS,
                    INGESTION_NUM_ROWS,
                    numColumnsPerStream,
                    INGESTION_NUM_SECONDS
            );

            System.out.println("========== ingestion scenario completed ==========");
            System.out.println();
        }
    }

    /**
     * This rule manages automatic graceful shutdown for the registered servers and channels at the
     * end of test.
     */
    @ClassRule
    public static final GrpcCleanupRule grpcCleanup = new GrpcCleanupRule();

    private static IngestionServiceImpl serviceImpl;

    private static IntegrationTestGrpcClient client;

    @BeforeClass
    public static void setUp() throws Exception {

        // uncomment line below to use a real handler, and write to database
//        IngestionHandlerInterface handler = new MongoSyncHandler();
        IngestionHandlerInterface handler = MongoIngestionHandler.newMongoSyncIngestionHandler();
        IngestionServiceImpl impl = new IngestionServiceImpl();
        if (!impl.init(handler)) {
            fail("IngestionServiceImpl.init failed");
        }
        serviceImpl = mock(IngestionServiceImpl.class, delegatesTo(impl));

        // Generate a unique in-process server name.
        String serverName = InProcessServerBuilder.generateName();

        // Create a server, add service, start, and register for automatic graceful shutdown.
        grpcCleanup.register(InProcessServerBuilder
                .forName(serverName).directExecutor().addService(serviceImpl).build().start());

        // Create a client channel and register for automatic graceful shutdown.
        ManagedChannel channel = grpcCleanup.register(
                InProcessChannelBuilder.forName(serverName).directExecutor().build());

        // Create a HelloWorldClient using the in-process channel;
        client = new IntegrationTestGrpcClient(channel);
    }

    @AfterClass
    public static void tearDown() {
        serviceImpl = null;
        client = null;
    }

    /**
     * Provides test coverage for a valid ingestion request stream.
     */
    @Test
    public void runIntegrationTestScenarios() {
        client.runStreamingIngestionScenario();
    }

//    /**
//     * To test the client, call from the client against the fake server, and verify behaviors or state
//     * changes from the server side.
//     * see https://github.com/grpc/grpc-java/blob/master/examples/src/test/java/io/grpc/examples/routeguide/RouteGuideClientTest.java
//     */
//    @Test
//    public void test03ArgumentCaptor() {
//
//
//        List<IngestionRequest> requests = new ArrayList<>();
//
//        // assemble request
//        int providerId = 1;
//        String requestId = "request-1";
//        List<String> columnNames = Arrays.asList("pv_01");
//        List<List<Object>> values = Arrays.asList(Arrays.asList(12.34, 42.00));
//        Instant instantNow = Instant.now();
//        Integer numSamples = 2;
//        IngestionRequestParams params =
//                new IngestionRequestParams(
//                        providerId,
//                        requestId,
//                        null,
//                        null,
//                        null,
//                        null,
//                        instantNow.getEpochSecond(),
//                        0L,
//                        1_000_000L,
//                        numSamples,
//                        columnNames,
//                        IngestionDataType.FLOAT,
//                        values);
//        IngestionRequest request = buildIngestionRequest(params);
//        requests.add(request);
//
//        ArgumentCaptor<IngestionRequest> requestCaptor = ArgumentCaptor.forClass(IngestionRequest.class);
//
////        client.greet("test name");
//        client.sendIngestionRequestStream(requests, 1);
//
//        var thingy = verify(serviceImpl)
//                .streamingIngestion(ArgumentMatchers.<StreamObserver<IngestionResponse>>any());
//        System.out.println("ArgumentCaptor test");
////        assertEquals("test name", requestCaptor.getValue().getName());
//    }

}
