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
import com.ospreydcs.dp.service.query.handler.model.HandlerQueryRequest;
import io.grpc.stub.StreamObserver;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class MongoQueryHandlerTestBase extends QueryTestBase {

    protected static MongoQueryHandler handler = null;
    protected static TestClientInterface clientTestInterface = null;
    private static String collectionNamePrefix = null;
    protected static final long startSeconds = Instant.now().getEpochSecond();
    protected static final String columnNameBase = "testpv_";


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
        HandlerQueryRequest handlerQueryRequest = new HandlerQueryRequest(request.getQuerySpec(), responseObserver);
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
        assertTrue(responseList.size() == numResponesesExpected);
        QueryResponse summaryResponse = responseList.get(0);
        assertTrue(summaryResponse.getResponseType() == ResponseType.SUMMARY_RESPONSE);
        assertTrue(summaryResponse.hasQueryReport());
        assertTrue(summaryResponse.getQueryReport().hasQuerySummary());
        assertTrue(summaryResponse.getQueryReport().getQuerySummary().getNumBuckets() == 0);

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

    public void testProcessQueryRequestSuccess() {

        // assemble query request
        String col1Name = columnNameBase + "1";
        String col2Name = columnNameBase + "2";
        List<String> columnNames = List.of(col1Name, col2Name);
        QueryTestBase.QueryRequestParams params = new QueryTestBase.QueryRequestParams(
                columnNames,
                startSeconds,
                0L,
                startSeconds + 5,
                0L);
        QueryRequest request = buildQueryRequest(params);

        // send request
        final int numResponesesExpected = 2; // expect summary and data messages
        List<QueryResponse> responseList = processQueryRequest(request, numResponesesExpected);

        // examine response
        assertTrue(responseList.size() == numResponesesExpected);

        // check summary message
        final int numBuckets = 10;
        final int numSamplesPerBucket = 10;
        final long bucketSampleIntervalNanos = 100_000_000L;
        QueryResponse summaryResponse = responseList.get(0);
        assertTrue(summaryResponse.getResponseType() == ResponseType.SUMMARY_RESPONSE);
        assertTrue(summaryResponse.hasQueryReport());
        assertTrue(summaryResponse.getQueryReport().hasQuerySummary());
        assertTrue(summaryResponse.getQueryReport().getQuerySummary().getNumBuckets() == numBuckets); // should contain 10 buckets

        // check data message
        QueryResponse dataResponse = responseList.get(1);
        assertTrue(dataResponse.getResponseType() == ResponseType.DETAIL_RESPONSE);
        assertTrue(dataResponse.hasQueryReport());
        assertTrue(dataResponse.getQueryReport().hasQueryData());
        QueryResponse.QueryReport.QueryData queryData = dataResponse.getQueryReport().getQueryData();
        assertEquals(numSamplesPerBucket, queryData.getDataBucketsCount());
        List<QueryResponse.QueryReport.QueryData.DataBucket> bucketList = queryData.getDataBucketsList();

        // check each bucket, 5 for each column
        int secondsIncrement = 0;
        String columnName = col1Name;
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
                columnName = col2Name;
            }
        }
    }
    
}
