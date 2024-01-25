package com.ospreydcs.dp.service.query.handler.mongo;

import com.ospreydcs.dp.grpc.v1.common.DataColumn;
import com.ospreydcs.dp.grpc.v1.common.DataValue;
import com.ospreydcs.dp.grpc.v1.common.ResponseType;
import com.ospreydcs.dp.grpc.v1.query.QueryRequest;
import com.ospreydcs.dp.grpc.v1.query.QueryResponse;
import com.ospreydcs.dp.service.common.bson.BucketDocument;
import com.ospreydcs.dp.service.common.bson.BucketUtility;
import com.ospreydcs.dp.service.common.mongo.MongoClientBase;
import com.ospreydcs.dp.service.query.QueryTestBase;
import com.ospreydcs.dp.service.query.handler.mongo.client.MongoQueryClientInterface;
import com.ospreydcs.dp.service.query.handler.mongo.dispatch.ResponseCursorDispatcher;
import com.ospreydcs.dp.service.query.handler.mongo.dispatch.ResponseStreamDispatcher;
import com.ospreydcs.dp.service.query.handler.mongo.job.QueryJob;
import io.grpc.stub.StreamObserver;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class MongoQueryHandlerTestBase extends QueryTestBase {

    protected static MongoQueryHandler handler = null;
    protected static TestClientInterface clientTestInterface = null;
    private static String collectionNamePrefix = null;
    protected static final long startSeconds = Instant.now().getEpochSecond();
    protected static final String columnNameBase = "testpv_";
    protected static final String COL_1_NAME = columnNameBase + "1";
    protected static final String COL_2_NAME = columnNameBase + "2";



    protected interface TestClientInterface extends MongoQueryClientInterface {
        public int insertBucketDocuments(List<BucketDocument> documentList);
    }

    private static String getTestCollectionNamePrefix() {
        if (collectionNamePrefix == null) {
            collectionNamePrefix = "test-" + System.currentTimeMillis() + "-";
        }
        return collectionNamePrefix;
    }

    protected static String getTestCollectionNameBuckets() {
        return getTestCollectionNamePrefix() + MongoClientBase.COLLECTION_NAME_BUCKETS;
    }

    protected static String getTestCollectionNameRequestStatus() {
        return getTestCollectionNamePrefix() + MongoClientBase.COLLECTION_NAME_REQUEST_STATUS;
    }

    public static void setUp(MongoQueryHandler handler, TestClientInterface clientInterface) throws Exception {
        System.out.println("setUp");
        MongoQueryHandlerTestBase.handler = handler;
        clientTestInterface = clientInterface;
        assertTrue(clientTestInterface.init());

        // create test data, 5 pvs sampled 10 times per second, 10 buckets per pv each with one second's data
        final int numSamplesPerSecond = 10;
        final int numSecondsPerBucket = 1;
        final int numColumns = 5;
        final int numBucketsPerColumn = 10;
        List<BucketDocument> bucketList = BucketUtility.createBucketDocuments(
                startSeconds, numSamplesPerSecond, numSecondsPerBucket, columnNameBase, numColumns, numBucketsPerColumn);
        assertEquals(clientTestInterface.insertBucketDocuments(bucketList), bucketList.size());
    }

    public static void tearDown() throws Exception {
        System.out.println("tearDown");
        assertTrue(clientTestInterface.fini());
        handler = null;
        clientTestInterface = null;
    }

    protected List<QueryResponse> executeAndDispatchResponseStream(QueryRequest request) {

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

        // create QueryJob and execute it
        final ResponseStreamDispatcher dispatcher = new ResponseStreamDispatcher(responseObserver);
        final QueryJob job = new QueryJob(request.getQuerySpec(), dispatcher, responseObserver, clientTestInterface);
        job.execute();

        return responseList;
    }

    private class ResponseCursorStreamObserver implements StreamObserver<QueryResponse> {

        private ResponseCursorDispatcher dispatcher = null;
        final CountDownLatch finishLatch;
        final List<QueryResponse> responseList;

        public ResponseCursorStreamObserver(CountDownLatch finishLatch, List<QueryResponse> responseList) {
            this.finishLatch = finishLatch;
            this.responseList = responseList;
        }

        public void setDispatcher(ResponseCursorDispatcher dispatcher) {
            this.dispatcher = dispatcher;
        }

        @Override
        public void onNext(QueryResponse queryDataResponse) {
            System.out.println("responseObserver.onNext");
            if (queryDataResponse.getResponseType() != ResponseType.DETAIL_RESPONSE) {
                System.out.println("unexpected response type: " + queryDataResponse.getResponseType().name());
                if (queryDataResponse.getResponseType() == ResponseType.ERROR_RESPONSE) {
                    System.out.println("response error msg: " + queryDataResponse.getQueryReport().getQueryStatus().getStatusMessage());
                }
                finishLatch.countDown();
            } else {
                System.out.println("adding detail response");
                responseList.add(queryDataResponse);
//                this.dispatcher.next();
            }
        }

        @Override
        public void onError(Throwable throwable) {
            System.out.println("responseObserver.onError");
            finishLatch.countDown();
        }

        @Override
        public void onCompleted() {
            System.out.println("responseObserver.onCompleted");
            finishLatch.countDown();
        }
    }

    protected List<QueryResponse> executeAndDispatchResponseCursor(QueryRequest request) {

        List<QueryResponse> responseList = new ArrayList<>();
        final CountDownLatch finishLatch = new CountDownLatch(1);

        // create observer for api response stream
        ResponseCursorStreamObserver responseObserver = new ResponseCursorStreamObserver(finishLatch, responseList);

        // create QueryJob and execute it
        final ResponseCursorDispatcher dispatcher = new ResponseCursorDispatcher(responseObserver);
        responseObserver.setDispatcher(dispatcher);
        final QueryJob job = new QueryJob(request.getQuerySpec(), dispatcher, responseObserver, clientTestInterface);
        job.execute();

        // check if RPC already completed
        if (finishLatch.getCount() == 0) {
            // RPC completed or errored already
            return responseList;
        }

        // otherwise wait for completion
        try {
            boolean awaitSuccess = finishLatch.await(1, TimeUnit.MINUTES);
            if (!awaitSuccess) {
                System.out.println("timeout waiting for finishLatch");
                return responseList;
            }
        } catch (InterruptedException e) {
            System.out.println("InterruptedException waiting for finishLatch");
            return responseList;
        }

        return responseObserver.responseList;
    }

    public void testResponseStreamDispatcherNoData() {

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
        List<QueryResponse> responseList = executeAndDispatchResponseStream(request);

        // examine response
        assertTrue(responseList.size() == numResponesesExpected);
        QueryResponse response = responseList.get(0);
        assertTrue(response.getResponseType() == ResponseType.STATUS_RESPONSE);
        assertTrue(response.hasQueryReport());
        assertTrue(response.getQueryReport().hasQueryStatus());
        QueryResponse.QueryReport.QueryStatus status = response.getQueryReport().getQueryStatus();
        assertEquals(QueryResponse.QueryReport.QueryStatus.QueryStatusType.QUERY_STATUS_EMPTY, status.getQueryStatusType());
    }

    private static void verifyDataBucket(
            QueryResponse.QueryReport.QueryData.DataBucket bucket,
            long bucketStartSeconds,
            long bucketStartNanos,
            long bucketSampleIntervalNanos,
            int bucketNumSamples,
            String bucketColumnName) {

        assertTrue(bucket.hasSamplingInterval());
        assertTrue(bucket.getSamplingInterval().getStartTime().getEpochSeconds() == bucketStartSeconds);
        assertTrue(bucket.getSamplingInterval().getStartTime().getNanoseconds() == bucketStartNanos);
        assertTrue(bucket.getSamplingInterval().getSampleIntervalNanos() == bucketSampleIntervalNanos);
        assertTrue(bucket.getSamplingInterval().getNumSamples() == bucketNumSamples);
        assertTrue(bucket.hasDataColumn());
        DataColumn bucketColumn = bucket.getDataColumn();
        assertTrue(bucketColumn.getName().equals(bucketColumnName));
        assertTrue(bucketColumn.getDataValuesCount() == bucketNumSamples);
        for (int valueIndex = 0 ; valueIndex < bucketColumn.getDataValuesCount() ; valueIndex++) {
            DataValue dataValue = bucketColumn.getDataValues(valueIndex);
            assertEquals((double) valueIndex, dataValue.getFloatValue(), 0);
        }
    }

    private static void verifyQueryResponse(List<QueryResponse> responseList) {

        // examine response
        final int numResponesesExpected = 1;
        assertTrue(responseList.size() == numResponesesExpected);

        // check data message
        final int numSamplesPerBucket = 10;
        final long bucketSampleIntervalNanos = 100_000_000L;
        QueryResponse dataResponse = responseList.get(0);
        assertTrue(dataResponse.getResponseType() == ResponseType.DETAIL_RESPONSE);
        assertTrue(dataResponse.hasQueryReport());
        assertTrue(dataResponse.getQueryReport().hasQueryData());
        QueryResponse.QueryReport.QueryData queryData = dataResponse.getQueryReport().getQueryData();
        assertEquals(numSamplesPerBucket, queryData.getDataBucketsCount());
        List<QueryResponse.QueryReport.QueryData.DataBucket> bucketList = queryData.getDataBucketsList();

        // check each bucket, 5 for each column
        int secondsIncrement = 0;
        String columnName = COL_1_NAME;
        for (int bucketIndex = 0 ; bucketIndex < 10 ; bucketIndex++) {
            final long bucketStartSeconds = startSeconds + secondsIncrement;
            verifyDataBucket(
                    bucketList.get(bucketIndex),
                    bucketStartSeconds,
                    0,
                    bucketSampleIntervalNanos,
                    numSamplesPerBucket,
                    columnName);
            secondsIncrement = secondsIncrement + 1;
            if (bucketIndex == 4) {
                secondsIncrement = 0;
                columnName = COL_2_NAME;
            }
        }
    }

    public void testResponseStreamDispatcher() {

        // assemble query request
        List<String> columnNames = List.of(COL_1_NAME, COL_2_NAME);
        QueryTestBase.QueryRequestParams params = new QueryTestBase.QueryRequestParams(
                columnNames,
                startSeconds,
                0L,
                startSeconds + 5,
                0L);
        QueryRequest request = buildQueryRequest(params);

        // execute query and dispatch result using ResponseStreamDispatcher
        List<QueryResponse> responseList = executeAndDispatchResponseStream(request);

        // examine response
        verifyQueryResponse(responseList);
    }

    public void testResponseCursorDispatcher() {

        // assemble query request
        List<String> columnNames = List.of(COL_1_NAME, COL_2_NAME);
        QueryTestBase.QueryRequestParams params = new QueryTestBase.QueryRequestParams(
                columnNames,
                startSeconds,
                0L,
                startSeconds + 5,
                0L);
        QueryRequest request = buildQueryRequest(params);

        // execute query and dispatch using ResponseCursorDispatcher
        List<QueryResponse> responseList = executeAndDispatchResponseCursor(request);

        // examine response
        verifyQueryResponse(responseList);
    }

}
