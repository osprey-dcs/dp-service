package com.ospreydcs.dp.service.query;

import com.ospreydcs.dp.grpc.v1.common.Timestamp;
import com.ospreydcs.dp.grpc.v1.query.*;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class QueryTestBase {

    public static class QueryDataRequestParams {

        public List<String> columnNames = null;
        public Long startTimeSeconds = null;
        public Long startTimeNanos = null;
        public Long endTimeSeconds = null;
        public Long endTimeNanos = null;

        public QueryDataRequestParams(
                List<String> columnNames,
                Long startTimeSeconds,
                Long startTimeNanos,
                Long endTimeSeconds,
                Long endTimeNanos) {

            this.columnNames = columnNames;
            this.startTimeSeconds = startTimeSeconds;
            this.startTimeNanos = startTimeNanos;
            this.endTimeSeconds = endTimeSeconds;
            this.endTimeNanos = endTimeNanos;
        }
    }
    
    public static QueryDataRequest buildQueryDataRequest(QueryDataRequestParams params) {
        
        // build API query request from params
        QueryDataRequest.Builder requestBuilder = QueryDataRequest.newBuilder();

        QueryDataRequest.QuerySpec.Builder querySpecBuilder = QueryDataRequest.QuerySpec.newBuilder();
        
        if (params.columnNames != null && !params.columnNames.isEmpty()) {
            querySpecBuilder.addAllPvNames(params.columnNames);
        }
        
        if (params.startTimeSeconds != null) {
            final Timestamp.Builder beginTimeBuilder = Timestamp.newBuilder();
            beginTimeBuilder.setEpochSeconds(params.startTimeSeconds);
            if (params.startTimeNanos != null) beginTimeBuilder.setNanoseconds(params.startTimeNanos);
            beginTimeBuilder.build();
            querySpecBuilder.setBeginTime(beginTimeBuilder);
        }
        
        if (params.endTimeSeconds != null) {
            final Timestamp.Builder endTimeBuilder = Timestamp.newBuilder();
            endTimeBuilder.setEpochSeconds(params.endTimeSeconds);
            if (params.endTimeNanos != null) endTimeBuilder.setNanoseconds(params.endTimeNanos);
            endTimeBuilder.build();
            querySpecBuilder.setEndTime(endTimeBuilder);
        }

        querySpecBuilder.build();
        requestBuilder.setQuerySpec(querySpecBuilder);

        return requestBuilder.build();
    }

    public static QueryMetadataRequest buildQueryMetadataRequest(String columnNamePattern) {

        QueryMetadataRequest.Builder requestBuilder = QueryMetadataRequest.newBuilder();

        QueryMetadataRequest.PvNamePattern.Builder pvNamePatternBuilder =
                QueryMetadataRequest.PvNamePattern.newBuilder();
        pvNamePatternBuilder.setPattern(columnNamePattern);
        pvNamePatternBuilder.build();

        requestBuilder.setPvNamePattern(pvNamePatternBuilder);
        return requestBuilder.build();
    }

    public static QueryMetadataRequest buildQueryMetadataRequest(List<String> pvNames) {

        QueryMetadataRequest.Builder requestBuilder = QueryMetadataRequest.newBuilder();

        QueryMetadataRequest.PvNameList.Builder pvNameListBuilder =
                QueryMetadataRequest.PvNameList.newBuilder();
        pvNameListBuilder.addAllPvNames(pvNames);
        pvNameListBuilder.build();

        requestBuilder.setPvNameList(pvNameListBuilder);
        return requestBuilder.build();
    }

    public static class QueryResponseTableObserver implements StreamObserver<QueryTableResponse> {

        private final CountDownLatch finishLatch = new CountDownLatch(1);
        private final AtomicBoolean isError = new AtomicBoolean(false);
        private final List<QueryTableResponse> responseList = Collections.synchronizedList(new ArrayList<>());

        public void await() {
            try {
                finishLatch.await(1, TimeUnit.MINUTES);
            } catch (InterruptedException e) {
                System.err.println("InterruptedException waiting for finishLatch");
                isError.set(true);
            }
        }

        public boolean isError() { return isError.get(); }

        public QueryTableResponse getQueryResponse() {
            return responseList.get(0);
        }

        @Override
        public void onNext(QueryTableResponse response) {
            // handle response in separate thread to better simulate out of process grpc,
            // otherwise response is handled in same thread as service handler that sent it
            new Thread(() -> {
                responseList.add(response);
                finishLatch.countDown();
                if (responseList.size() > 1) {
                    System.err.println("QueryResponseTableObserver onNext received more than one response");
                    isError.set(true);
                }
            }).start();

        }

        @Override
        public void onError(Throwable t) {
            // handle response in separate thread to better simulate out of process grpc,
            // otherwise response is handled in same thread as service handler that sent it
            new Thread(() -> {
                Status status = Status.fromThrowable(t);
                System.err.println("QueryResponseTableObserver error: " + status);
                isError.set(true);
            }).start();
        }

        @Override
        public void onCompleted() {
        }
    }

    public static class QueryResponseStreamObserver implements StreamObserver<QueryDataResponse> {

        private final CountDownLatch finishLatch = new CountDownLatch(1);
        private final AtomicBoolean isError = new AtomicBoolean(false);
        private final List<QueryDataResponse.QueryData.DataBucket> dataBucketList =
                Collections.synchronizedList(new ArrayList<>());

//        public QueryResponseStreamObserver(int numBucketsExpected) {
//            this.finishLatch = new CountDownLatch(numBucketsExpected);
//        }

        public void await() {
            try {
                finishLatch.await(1, TimeUnit.MINUTES);
            } catch (InterruptedException e) {
                System.err.println("InterruptedException waiting for finishLatch");
                isError.set(true);
            }
        }

        public boolean isError() { return isError.get(); }

        public List<QueryDataResponse.QueryData.DataBucket> getDataBucketList() {
            return dataBucketList;
        }

        @Override
        public void onNext(QueryDataResponse response) {
            
            // handle response in separate thread to better simulate out of process grpc,
            // otherwise response is handled in same thread as service handler that sent it
            new Thread(() -> {
                List<QueryDataResponse.QueryData.DataBucket> responseBucketList =
                        response.getQueryData().getDataBucketsList();
                for (QueryDataResponse.QueryData.DataBucket bucket : responseBucketList) {
                    dataBucketList.add(bucket);
                }
            }).start();
        }

        @Override
        public void onError(Throwable t) {
            // handle response in separate thread to better simulate out of process grpc,
            // otherwise response is handled in same thread as service handler that sent it
            new Thread(() -> {
                Status status = Status.fromThrowable(t);
                System.err.println("QueryResponseTableObserver error: " + status);
                isError.set(true);
                finishLatch.countDown();
            }).start();
        }

        @Override
        public void onCompleted() {
            // handle response in separate thread to better simulate out of process grpc,
            // otherwise response is handled in same thread as service handler that sent it
            new Thread(() -> {
                finishLatch.countDown();
            }).start();
        }
    }

    public static class QueryMetadataResponseObserver implements StreamObserver<QueryMetadataResponse> {

        // instance variables
        private final CountDownLatch finishLatch = new CountDownLatch(1);
        private final AtomicBoolean isError = new AtomicBoolean(false);
        private final List<String> errorMessageList = Collections.synchronizedList(new ArrayList<>());
        private final List<QueryMetadataResponse.MetadataResult.PvInfo> pvInfoList =
                Collections.synchronizedList(new ArrayList<>());

        public void await() {
            try {
                finishLatch.await(1, TimeUnit.MINUTES);
            } catch (InterruptedException e) {
                final String errorMsg = "InterruptedException waiting for finishLatch";
                System.err.println(errorMsg);
                isError.set(true);
                errorMessageList.add(errorMsg);
            }
        }

        public boolean isError() { return isError.get(); }
        public String getErrorMessage() {
            if (!errorMessageList.isEmpty()) {
                return errorMessageList.get(0);
            } else {
                return "";
            }
        }

        public List<QueryMetadataResponse.MetadataResult.PvInfo> getPvInfoList() {
            return pvInfoList;
        }

        @Override
        public void onNext(QueryMetadataResponse response) {

            // handle response in separate thread to better simulate out of process grpc,
            // otherwise response is handled in same thread as service handler that sent it
            new Thread(() -> {

                if (response.hasExceptionalResult()) {
                    final String errorMsg = "QueryResponseColumnInfoObserver onNext received exception response: "
                            + response.getExceptionalResult().getMessage();
                    System.err.println(errorMsg);
                    isError.set(true);
                    errorMessageList.add(errorMsg);
                    finishLatch.countDown();
                    return;
                }

                assertTrue(response.hasMetadataResult());
                final QueryMetadataResponse.MetadataResult metadataResult = response.getMetadataResult();
                assertNotNull(metadataResult);
                // assertTrue(metadataResult.getPvInfosCount() > 0); - MAYBE ALLOW AN EMPTY RESULT FOR NEGATIVE TEST CASE?

                // flag error if already received a response
                if (!pvInfoList.isEmpty()) {
                    final String errorMsg = "QueryResponseColumnInfoObserver onNext received more than one response";
                    System.err.println(errorMsg);
                    isError.set(true);
                    errorMessageList.add(errorMsg);

                } else {
                    for (QueryMetadataResponse.MetadataResult.PvInfo pvInfo :
                            response.getMetadataResult().getPvInfosList()) {
                        pvInfoList.add(pvInfo);
                    }
                    finishLatch.countDown();
                }
            }).start();

        }

        @Override
        public void onError(Throwable t) {
            // handle response in separate thread to better simulate out of process grpc,
            // otherwise response is handled in same thread as service handler that sent it
            new Thread(() -> {
                final Status status = Status.fromThrowable(t);
                final String errorMsg = "QueryResponseColumnInfoObserver error: " + status;
                System.err.println(errorMsg);
                isError.set(true);
                errorMessageList.add(errorMsg);
                finishLatch.countDown();
            }).start();
        }

        @Override
        public void onCompleted() {
        }
    }

}
