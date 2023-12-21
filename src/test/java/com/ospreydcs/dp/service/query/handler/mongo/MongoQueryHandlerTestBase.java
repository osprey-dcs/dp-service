package com.ospreydcs.dp.service.query.handler.mongo;

import com.ospreydcs.dp.grpc.v1.common.ResponseType;
import com.ospreydcs.dp.grpc.v1.query.QueryRequest;
import com.ospreydcs.dp.grpc.v1.query.QueryResponse;
import com.ospreydcs.dp.service.query.QueryTestBase;
import com.ospreydcs.dp.service.query.handler.model.HandlerQueryRequest;
import io.grpc.stub.StreamObserver;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertTrue;

public class MongoQueryHandlerTestBase extends QueryTestBase {

    protected static MongoQueryHandler handler = null;
    protected static TestClientInterface clientTestInterface = null;

    protected interface TestClientInterface extends MongoQueryClientInterface {
    }

    public static void setUp(MongoQueryHandler handler, TestClientInterface clientInterface) throws Exception {
        System.out.println("setUp");
        MongoQueryHandlerTestBase.handler = handler;
        clientTestInterface = clientInterface;
        assertTrue("client init failed", clientTestInterface.init());
    }

    public static void tearDown() throws Exception {
        System.out.println("tearDown");
        assertTrue("client fini failed", clientTestInterface.fini());
        handler = null;
        clientTestInterface = null;
    }

    protected List<QueryResponse> processQueryRequest(
            QueryRequest request, int numResponsesExpected) {

        System.out.println("handleQueryRequest responses expected: " + numResponsesExpected);

        List<QueryResponse> responseList = new ArrayList<>();

        // create observer for api response stream
        StreamObserver<QueryResponse> responseObserver = new StreamObserver<QueryResponse>() {

            @Override
            public void onNext(QueryResponse queryDataResponse) {
                System.out.println("responseObserver.onNext");
                responseList.add(queryDataResponse);
            }

            @Override
            public void onError(Throwable throwable) {
                System.out.println("responseObserver.onError");
            }

            @Override
            public void onCompleted() {
                System.out.println("responseObserver.Completed");
            }
        };

        // send api request
        HandlerQueryRequest handlerQueryRequest = new HandlerQueryRequest(request, responseObserver);
        handler.processQueryRequest(handlerQueryRequest);

        return responseList;
    }

    public void testProcessQueryRequestNoData() {

        // assemble query request
        // create request with unspecified column name
        List<String> columnNames = List.of("pv_1", "pv_2");
        Long nowSeconds = Instant.now().getEpochSecond();
        QueryTestBase.QueryRequestParams params = new QueryTestBase.QueryRequestParams(
                columnNames,
                nowSeconds,
                0L,
                nowSeconds + 1,
                0L);
        QueryRequest request = buildQueryRequest(params);

        // send request
        final int numResponesesExpected = 1;
        List<QueryResponse> responseList = processQueryRequest(request, numResponesesExpected);

        // examine response
        assertTrue("unexpected responseList.size",
                responseList.size() == numResponesesExpected);
        QueryResponse summaryResponse = responseList.get(0);
        assertTrue("unexpected responseType",
                summaryResponse.getResponseType() == ResponseType.SUMMARY_RESPONSE);
        assertTrue("QueryReport not set",
                summaryResponse.hasQueryReport());
        assertTrue("QuerySummary not set",
                summaryResponse.getQueryReport().hasQuerySummary());
        assertTrue("numBuckets is nonzero",
                summaryResponse.getQueryReport().getQuerySummary().getNumBuckets() == 0);

    }

}
