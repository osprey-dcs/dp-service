package com.ospreydcs.dp.service.ingest.server;

import com.ospreydcs.dp.grpc.v1.common.ExceptionalResult;
import com.ospreydcs.dp.grpc.v1.ingestion.*;
import com.ospreydcs.dp.service.ingest.IngestionTestBase;
import com.ospreydcs.dp.service.ingest.handler.IngestionHandlerBase;
import com.ospreydcs.dp.service.ingest.handler.IngestionHandlerInterface;
import com.ospreydcs.dp.service.ingest.handler.model.HandlerIngestionRequest;
import com.ospreydcs.dp.service.ingest.service.IngestionServiceImpl;
import io.grpc.Channel;
import io.grpc.ManagedChannel;
import io.grpc.Status;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import io.grpc.stub.StreamObserver;
import io.grpc.testing.GrpcCleanupRule;
import org.junit.*;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.*;
import static org.mockito.AdditionalAnswers.delegatesTo;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

@RunWith(JUnit4.class)
public class IngestionGrpcTest extends IngestionTestBase {

    protected static class TestIngestionHandler extends IngestionHandlerBase implements IngestionHandlerInterface {

        @Override
        public boolean init() {
            System.out.println("handler.init");
            return true;
        }

        @Override
        public boolean fini() {
            System.out.println("handler.fini");
            return true;
        }

        @Override
        public boolean start() {
            System.out.println("handler.start");
            return true;
        }

        @Override
        public boolean stop() {
            System.out.println("handler.fini");
            return true;
        }

        @Override
        public void onNext(HandlerIngestionRequest request) {
            System.out.println("handler.onNext handling requestId: " + request.request.getClientRequestId());
        }
    }

    protected static class TestIngestionClient {

        protected static DpIngestionServiceGrpc.DpIngestionServiceStub asyncStub; // must use async stub for streaming api

        public TestIngestionClient(Channel channel) {
            // 'channel' here is a Channel, not a ManagedChannel, so it is not this code's responsibility to
            // shut it down.
            // Passing Channels to code makes code easier to test and makes it easier to reuse Channels.
            asyncStub = DpIngestionServiceGrpc.newStub(channel);
        }

        /**
         * Sends a list of IngestionRequest objects via the streamingIngestion() API method.  Returns
         * a list of IngestionResponse objects, one for each request.  Used by test methods for the
         * streamingIngestion() API.
         * @param requestList
         * @return
         */
        protected List<IngestDataResponse> sendIngestionRequestStream(
                List<IngestDataRequest> requestList,
                int numResponsesExpected) {

            System.out.println("sendIngestionRequestStream requestList size: "
                    + requestList.size() + " responses expected: " + numResponsesExpected);

            List<IngestDataResponse> responseList = new ArrayList<>();
            final CountDownLatch finishLatch = new CountDownLatch(1);

            /**
             * Implements StreamObserver interface for handling the API's response stream.
             */
            StreamObserver<IngestDataResponse> responseObserver = new StreamObserver<IngestDataResponse>() {

                /**
                 * Adds response to the list of responses for the API stream.
                 * @param response
                 */
                @Override
                public void onNext(IngestDataResponse response) {
                    System.out.println("sendIngestionRequestStream.responseObserver.onNext");
                    responseList.add(response);
                }

                /**
                 * Catches unexpected error in response stream.  Causes jUnit test exeuction to fail.
                 * @param t
                 */
                @Override
                public void onError(Throwable t) {
                    System.out.println("sendIngestionRequestStream.responseObserver.onError");
                    Status status = Status.fromThrowable(t);
                    finishLatch.countDown();
                }

                /**
                 * Closes API response stream.
                 */
                @Override
                public void onCompleted() {
                    System.out.println("sendIngestionRequestStream.responseObserver.onCompleted");
                    finishLatch.countDown();
                }
            };

            // send each request in a new stream
//        List<IngestionResponse> responseList = new ArrayList<>();
            for (IngestDataRequest request : requestList) {
//            IngestionResponseObserver responseObserver = new IngestionResponseObserver(numResponsesExpected);
                StreamObserver<IngestDataRequest> requestObserver = asyncStub.ingestDataStream(responseObserver);
                requestObserver.onNext(request);
                requestObserver.onCompleted();
                try {
                    finishLatch.await(1, TimeUnit.MINUTES);
                } catch (InterruptedException e) {
                    fail("InterruptedException waiting for finishLatch");
                }
//            responseObserver.await();
//            responseList.addAll(responseObserver.getResponseList());
            }

            return responseList;
        }

    }

    /**
     * This rule manages automatic graceful shutdown for the registered servers and channels at the
     * end of test.
     */
    @ClassRule
    public static final GrpcCleanupRule grpcCleanup = new GrpcCleanupRule();

    private static IngestionServiceImpl serviceImpl;

    private static TestIngestionClient client;

    @BeforeClass
    public static void setUp() throws Exception {

        // uncomment line below to use a real handler, and write to database
//        IngestionHandlerInterface handler = new MongoSyncHandler();
        IngestionHandlerInterface handler = new TestIngestionHandler();
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
        client = new TestIngestionClient(channel);
    }

    @AfterClass
    public static void tearDown() {
        serviceImpl = null;
        client = null;
    }

    /**
     * Test a validation failure, that column name is not specified.
     */
    @Test
    public void testValidateRequestEmptyTimestampsList() {

        System.out.println("test01ValidateRequestEmptyTimestampsList");

        // create request
        int providerId = 1;
        String requestId = "request-1";
        List<String> columnNames = Arrays.asList("");
        List<List<Object>> values = Arrays.asList(Arrays.asList(12.34));
        Instant instantNow = Instant.now();
        IngestionTestBase.IngestionRequestParams params =
                new IngestionTestBase.IngestionRequestParams(
                        providerId,
                        requestId,
                        null,
                        null,
                        null,
                        null,
                        instantNow.getEpochSecond(),
                        0L,
                        1_000_000L,
                        1,
                        columnNames,
                        IngestionTestBase.IngestionDataType.DOUBLE,
                        values);
        IngestDataRequest request = buildIngestionRequest(params);
        List<IngestDataRequest> requests = Arrays.asList(request);

        // send request and examine response
        List<IngestDataResponse> responses = client.sendIngestionRequestStream(requests, 1);
        assertTrue(responses.size() == requests.size());
        IngestDataResponse response = responses.get(0);
        assertTrue(response.getProviderId() == providerId);
        assertTrue(response.getClientRequestId().equals(requestId));
        assertTrue(response.hasExceptionalResult());
        assertEquals(
                ExceptionalResult.ExceptionalResultStatus.RESULT_STATUS_REJECT,
                response.getExceptionalResult().getExceptionalResultStatus());
        assertTrue(response.getResponseTime().getEpochSeconds() > 0);
        assertTrue(response.getExceptionalResult().getMessage().equals("name must be specified for all data columns"));
    }

    /**
     * Provides test coverage for a valid ingestion request stream.
     */
    @Test
    public void testSendValidIngestionRequestStream() {

        System.out.println("test02SendValidIngestionRequestStream");

        List<IngestDataRequest> requests = new ArrayList<>();

        // assemble request
        int providerId = 1;
        String requestId = "request-1";
        List<String> columnNames = Arrays.asList("pv_01");
        List<List<Object>> values = Arrays.asList(Arrays.asList(12.34, 42.00));
        Instant instantNow = Instant.now();
        Integer numSamples = 2;
        IngestionRequestParams params =
                new IngestionRequestParams(
                        providerId,
                        requestId,
                        null,
                        null,
                        null,
                        null,
                        instantNow.getEpochSecond(),
                        0L,
                        1_000_000L,
                        numSamples,
                        columnNames,
                        IngestionDataType.DOUBLE,
                        values);
        IngestDataRequest request = buildIngestionRequest(params);
        requests.add(request);

        // send request
        List<IngestDataResponse> responseList = client.sendIngestionRequestStream(requests, 1);

        // check response
        final int numResponses = 1;
        assertTrue(
                "responseList size not equal to " + numResponses + ": " + responseList.size(),
                responseList.size() == numResponses);

        // check ack
        IngestDataResponse ackResponse = responseList.get(0);
        assertTrue(ackResponse.getProviderId() == providerId);
        assertTrue(ackResponse.getClientRequestId().equals(requestId));
        assertTrue(ackResponse.hasAckResult());
        assertTrue(ackResponse.getResponseTime().getEpochSeconds() > 0);
        assertTrue(ackResponse.getAckResult().getNumRows() == numSamples);
        assertTrue(ackResponse.getAckResult().getNumColumns() == columnNames.size());
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
