package com.ospreydcs.dp.service.query.server;

import com.ospreydcs.dp.grpc.v1.common.RejectDetails;
import com.ospreydcs.dp.grpc.v1.common.ResponseType;
import com.ospreydcs.dp.grpc.v1.query.DpQueryServiceGrpc;
import com.ospreydcs.dp.grpc.v1.query.QueryRequest;
import com.ospreydcs.dp.grpc.v1.query.QueryResponse;
import com.ospreydcs.dp.service.query.QueryTestBase;
import com.ospreydcs.dp.service.query.handler.QueryHandlerBase;
import com.ospreydcs.dp.service.query.handler.interfaces.QueryHandlerInterface;
import com.ospreydcs.dp.service.query.handler.interfaces.ResultCursorInterface;
import com.ospreydcs.dp.service.query.service.QueryServiceImpl;
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
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.AdditionalAnswers.delegatesTo;
import static org.mockito.Mockito.mock;

@RunWith(JUnit4.class)
public class QueryGrpcTest extends QueryTestBase {

    protected static class TestQueryHandler extends QueryHandlerBase implements QueryHandlerInterface {

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
        public void handleQueryResponseStream(
                QueryRequest.QuerySpec querySpec, StreamObserver<QueryResponse> responseObserver
        ) {
            System.out.println("handleQueryRequest: " + querySpec.getColumnNamesList());
            responseObserver.onCompleted(); // close response stream so that client stream is closed
        }

        @Override
        public ResultCursorInterface handleQueryResponseCursor(
                QueryRequest.QuerySpec querySpec, StreamObserver<QueryResponse> responseObserver) {
            return null;
        }

        @Override
        public void handleQueryResponseSingle(
                QueryRequest.QuerySpec querySpec, StreamObserver<QueryResponse> responseObserver) {
        }
    }

    protected static class TestQueryClient {

        // must use async stub for streaming api
        protected static DpQueryServiceGrpc.DpQueryServiceStub asyncStub;

        public TestQueryClient(Channel channel) {
            asyncStub = DpQueryServiceGrpc.newStub(channel);
        }

        protected List<QueryResponse> sendQueryRequest(
                QueryRequest request, int numResponsesExpected) {

            System.out.println("sendQueryRequest responses expected: " + numResponsesExpected);

            List<QueryResponse> responseList = new ArrayList<>();
            final CountDownLatch finishLatch = new CountDownLatch(1);

            // create observer for api response stream
            StreamObserver<QueryResponse> responseObserver = new StreamObserver<QueryResponse>() {

                @Override
                public void onNext(QueryResponse queryDataResponse) {
                    System.out.println("sendQueryDataByTimeRequest.responseObserver.onNext");
                    responseList.add(queryDataResponse);
                }

                @Override
                public void onError(Throwable throwable) {
                    System.out.println("sendQueryDataByTimeRequest.responseObserver.onError");
                    Status status = Status.fromThrowable(throwable);
                    finishLatch.countDown();
                }

                @Override
                public void onCompleted() {
                    System.out.println("sendQueryDataByTimeRequest.responseObserver.Completed");
                    finishLatch.countDown();
                }
            };

            // send api request
            asyncStub.queryResponseStream(request, responseObserver);

            // wait for completion of api response stream via finishLatch notification
            try {
                finishLatch.await(1, TimeUnit.MINUTES);
            } catch (InterruptedException e) {
                fail("InterruptedException waiting for finishLatch");
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

    private static QueryServiceImpl serviceImpl;

    private static TestQueryClient client;

    @BeforeClass
    public static void setUp() throws Exception {
        QueryHandlerInterface handler = new TestQueryHandler();
        QueryServiceImpl impl = new QueryServiceImpl();
        if (!impl.init(handler)) {
            fail("impl.init failed");
        }
        serviceImpl = mock(QueryServiceImpl.class, delegatesTo(impl));

        // Generate a unique in-process server name.
        String serverName = InProcessServerBuilder.generateName();

        // Create a server, add service, start, and register for automatic graceful shutdown.
        grpcCleanup.register(InProcessServerBuilder
                .forName(serverName).directExecutor().addService(serviceImpl).build().start());

        // Create a client channel and register for automatic graceful shutdown.
        ManagedChannel channel = grpcCleanup.register(
                InProcessChannelBuilder.forName(serverName).directExecutor().build());

        client = new TestQueryClient(channel);
    }

    @AfterClass
    public static void tearDown() {
        serviceImpl = null;
        client = null;
    }

    /**
     * Test validation failure, that columnName not specified.
     */
    @Test
    public void testValidateRequestUnspecifiedColumnName() {

        // create request with unspecified column name list
        Long nowSeconds = Instant.now().getEpochSecond();
        QueryRequestParams params = new QueryRequestParams(
                null,
                nowSeconds,
                0L,
                nowSeconds + 1,
                0L);
        QueryRequest request = buildQueryRequest(params);

        // send request
        final int numResponesesExpected = 1;
        List<QueryResponse> responseList = client.sendQueryRequest(request, numResponesesExpected);

        // examine response
        assertTrue(responseList.size() == 1);
        QueryResponse response = responseList.get(0);
        assertTrue(response.getResponseType() == ResponseType.REJECT_RESPONSE);
        assertTrue(response.getResponseTime().getEpochSeconds() > 0);
        assertTrue(response.hasQueryReject());
        assertTrue(response.getQueryReject().getRejectReason() == RejectDetails.RejectReason.INVALID_REQUEST_REASON);
        assertTrue(response.getQueryReject().getMessage().equals("columnName must be specified"));
    }

    /**
     * Test validation failure, that columnName not specified.
     */
    @Test
    public void testSendValidQueryRequest() {

        // create request with unspecified column name
        List<String> columnNames = List.of("pv_1", "pv_2");
        Long nowSeconds = Instant.now().getEpochSecond();
        QueryRequestParams params = new QueryRequestParams(
                columnNames,
                nowSeconds,
                0L,
                nowSeconds + 1,
                0L);
        QueryRequest request = buildQueryRequest(params);

        // send request
        final int numResponesesExpected = 0;
        List<QueryResponse> responseList = client.sendQueryRequest(request, numResponesesExpected);

        // examine response
        assertTrue(responseList.size() == 0);
    }

}