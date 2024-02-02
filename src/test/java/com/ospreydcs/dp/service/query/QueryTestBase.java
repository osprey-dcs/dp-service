package com.ospreydcs.dp.service.query;

import com.ospreydcs.dp.grpc.v1.common.Timestamp;
import com.ospreydcs.dp.grpc.v1.query.QueryRequest;
import com.ospreydcs.dp.grpc.v1.query.QueryResponse;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

public class QueryTestBase {

    public static class QueryRequestParams {

        public List<String> columnNames = null;
        public Long startTimeSeconds = null;
        public Long startTimeNanos = null;
        public Long endTimeSeconds = null;
        public Long endTimeNanos = null;

        public QueryRequestParams(
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
    
    public static QueryRequest buildQueryRequest(QueryRequestParams params) {
        
        // build API query request from params
        QueryRequest.Builder requestBuilder = QueryRequest.newBuilder();

        QueryRequest.QuerySpec.Builder querySpecBuilder = QueryRequest.QuerySpec.newBuilder();
        
        if (params.columnNames != null && !params.columnNames.isEmpty()) {
            querySpecBuilder.addAllColumnNames(params.columnNames);
        }
        
        if (params.startTimeSeconds != null) {
            final Timestamp.Builder startTimeBuilder = Timestamp.newBuilder();
            startTimeBuilder.setEpochSeconds(params.startTimeSeconds);
            if (params.startTimeNanos != null) startTimeBuilder.setNanoseconds(params.startTimeNanos);
            startTimeBuilder.build();
            querySpecBuilder.setStartTime(startTimeBuilder);
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

    public static class QueryResponseTableObserver implements StreamObserver<QueryResponse> {

        private final CountDownLatch finishLatch = new CountDownLatch(1);
        private final AtomicBoolean isError = new AtomicBoolean(false);
        private final List<QueryResponse> responseList = Collections.synchronizedList(new ArrayList<>());

        public void await() {
            try {
                finishLatch.await(1, TimeUnit.MINUTES);
            } catch (InterruptedException e) {
                System.err.println("InterruptedException waiting for finishLatch");
                isError.set(true);
            }
        }

        public boolean isError() { return isError.get(); }

        public QueryResponse getQueryResponse() {
            return responseList.get(0);
        }

        @Override
        public void onNext(QueryResponse response) {
            responseList.add(response);
            finishLatch.countDown();
            if (responseList.size() > 1) {
                System.err.println("QueryResponseTableObserver onNext received more than one response");
                isError.set(true);
            }
        }

        @Override
        public void onError(Throwable t) {
            Status status = Status.fromThrowable(t);
            System.err.println("QueryResponseTableObserver error: " + status);
            isError.set(true);
        }

        @Override
        public void onCompleted() {
        }
    }

    public static class QueryResponseStreamObserver implements StreamObserver<QueryResponse> {

        private final CountDownLatch finishLatch = new CountDownLatch(1);
        private final AtomicBoolean isError = new AtomicBoolean(false);
        private final List<QueryResponse.QueryReport.BucketData.DataBucket> dataBucketList =
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

        public List<QueryResponse.QueryReport.BucketData.DataBucket> getDataBucketList() {
            return dataBucketList;
        }

        @Override
        public void onNext(QueryResponse response) {

            List<QueryResponse.QueryReport.BucketData.DataBucket> responseBucketList =
                    response.getQueryReport().getBucketData().getDataBucketsList();

            for (QueryResponse.QueryReport.BucketData.DataBucket bucket : responseBucketList) {
                dataBucketList.add(bucket);
//                finishLatch.countDown();
            }
        }

        @Override
        public void onError(Throwable t) {
            Status status = Status.fromThrowable(t);
            System.err.println("QueryResponseTableObserver error: " + status);
            isError.set(true);
            finishLatch.countDown();
        }

        @Override
        public void onCompleted() {
            finishLatch.countDown();
        }
    }

}
