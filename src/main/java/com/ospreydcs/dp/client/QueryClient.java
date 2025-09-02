package com.ospreydcs.dp.client;

import com.ospreydcs.dp.client.result.QueryProvidersApiResult;
import com.ospreydcs.dp.client.result.QueryPvMetadataApiResult;
import com.ospreydcs.dp.client.result.QueryTableApiResult;
import com.ospreydcs.dp.grpc.v1.common.Timestamp;
import com.ospreydcs.dp.grpc.v1.query.*;
import io.grpc.ManagedChannel;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

public class QueryClient extends ServiceApiClientBase {

    public record QueryTableRequestParams(
            QueryTableRequest.TableResultFormat tableResultFormat,
            List<String> pvNameList,
            String pvNamePattern,
            Long beginTimeSeconds,
            Long beginTimeNanos,
            Long endTimeSeconds,
            Long endTimeNanos
    ) {
    }

    public static class QueryTableResponseObserver implements StreamObserver<QueryTableResponse> {

        private final CountDownLatch finishLatch = new CountDownLatch(1);
        private final AtomicBoolean isError = new AtomicBoolean(false);
        private final List<String> errorMessageList = Collections.synchronizedList(new ArrayList<>());
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

        public String getErrorMessage() {
            if (!errorMessageList.isEmpty()) {
                return errorMessageList.get(0);
            } else {
                return "";
            }
        }

        public QueryTableResponse getQueryResponse() {
            return responseList.get(0);
        }

        @Override
        public void onNext(QueryTableResponse response) {
            // handle response in separate thread to better simulate out of process grpc,
            // otherwise response is handled in same thread as service handler that sent it
            new Thread(() -> {

                if (response.hasExceptionalResult()) {
                    final String errorMsg = "onNext received exceptional response: "
                            + response.getExceptionalResult().getMessage();
                    System.err.println(errorMsg);
                    isError.set(true);
                    errorMessageList.add(errorMsg);
                }

                responseList.add(response);
                finishLatch.countDown();
                if (responseList.size() > 1) {
                    System.err.println("QueryTableResponseObserver onNext received more than one response");
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
                System.err.println("QueryTableResponseObserver error: " + status);
                isError.set(true);
            }).start();
        }

        @Override
        public void onCompleted() {
        }
    }

    public static class QueryPvMetadataResponseObserver implements StreamObserver<QueryPvMetadataResponse> {

        // instance variables
        private final CountDownLatch finishLatch = new CountDownLatch(1);
        private final AtomicBoolean isError = new AtomicBoolean(false);
        private final List<String> errorMessageList = Collections.synchronizedList(new ArrayList<>());
        private final List<QueryPvMetadataResponse> responseList =
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

        public QueryPvMetadataResponse getResponse() {
            if (responseList.isEmpty() || responseList.size() > 1) {
                return null;
            } else {
                return responseList.get(0);
            }
        }

        @Override
        public void onNext(QueryPvMetadataResponse response) {

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

                // flag error if already received a response
                if (!responseList.isEmpty()) {
                    final String errorMsg = "QueryResponseColumnInfoObserver onNext received more than one response";
                    System.err.println(errorMsg);
                    isError.set(true);
                    errorMessageList.add(errorMsg);

                } else {
                    responseList.add(response);
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

    public static class QueryProvidersRequestParams {

        public String idCriterion = null;
        public String textCriterion = null;
        public String tagsCriterion = null;
        public String attributesCriterionKey = null;
        public String attributesCriterionValue = null;

        public void setIdCriterion(String idCriterion) {
            this.idCriterion = idCriterion;
        }

        public void setTextCriterion(String textCriterion) {
            this.textCriterion = textCriterion;
        }

        public void setTagsCriterion(String tagsCriterion) {
            this.tagsCriterion = tagsCriterion;
        }

        public void setAttributesCriterion(String attributeCriterionKey, String attributeCriterionValue) {
            this.attributesCriterionKey = attributeCriterionKey;
            this.attributesCriterionValue = attributeCriterionValue;
        }
    }

    public static class QueryProvidersResponseObserver implements StreamObserver<QueryProvidersResponse> {

        // instance variables
        private final CountDownLatch finishLatch = new CountDownLatch(1);
        private final AtomicBoolean isError = new AtomicBoolean(false);
        private final List<String> errorMessageList = Collections.synchronizedList(new ArrayList<>());
        private final List<QueryProvidersResponse.ProvidersResult.ProviderInfo> providerInfoList =
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

        public List<QueryProvidersResponse.ProvidersResult.ProviderInfo> getProviderInfoList() {
            return providerInfoList;
        }

        @Override
        public void onNext(QueryProvidersResponse response) {

            // handle response in separate thread to better simulate out of process grpc,
            // otherwise response is handled in same thread as service handler that sent it
            new Thread(() -> {

                if (response.hasExceptionalResult()) {
                    final String errorMsg = "onNext received ExceptionalResult: "
                            + response.getExceptionalResult().getMessage();
                    System.err.println(errorMsg);
                    isError.set(true);
                    errorMessageList.add(errorMsg);
                    finishLatch.countDown();
                    return;
                }

                if (!response.hasProvidersResult()) {
                    final String errorMsg = "QueryProvidersResponse does not contain ProvidersResult";
                    System.err.println(errorMsg);
                    isError.set(true);
                    errorMessageList.add(errorMsg);
                    finishLatch.countDown();
                    return;
                }

                final QueryProvidersResponse.ProvidersResult providersResult = response.getProvidersResult();
                Objects.requireNonNull(providersResult);

                // flag error if already received a response
                if (!providerInfoList.isEmpty()) {
                    final String errorMsg = "onNext received more than one response";
                    System.err.println(errorMsg);
                    isError.set(true);
                    errorMessageList.add(errorMsg);

                } else {
                    providerInfoList.addAll(response.getProvidersResult().getProviderInfosList());
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
                final String errorMsg = "onError: " + status;
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

    // static variables
    private static final Logger logger = LogManager.getLogger();

    public QueryClient(ManagedChannel channel) {
        super(channel);
    }

    public static QueryTableRequest buildQueryTableRequest(QueryTableRequestParams params) {

        QueryTableRequest.Builder requestBuilder = QueryTableRequest.newBuilder();

        // set format
        if (params.tableResultFormat != null) {
            requestBuilder.setFormat(params.tableResultFormat);
        }

        // set pvNameList or PvNamePattern
        if (params.pvNameList != null && !params.pvNameList.isEmpty()) {
            PvNameList pvNameList = PvNameList.newBuilder()
                    .addAllPvNames(params.pvNameList)
                    .build();
            requestBuilder.setPvNameList(pvNameList);
        } else if (params.pvNamePattern != null && !params.pvNamePattern.isBlank()) {
            PvNamePattern pvNamePattern = PvNamePattern.newBuilder()
                    .setPattern(params.pvNamePattern)
                    .build();
            requestBuilder.setPvNamePattern(pvNamePattern);
        }

        // set begin time
        if (params.beginTimeSeconds != null) {
            final Timestamp.Builder beginTimeBuilder = Timestamp.newBuilder();
            beginTimeBuilder.setEpochSeconds(params.beginTimeSeconds);
            if (params.beginTimeNanos != null) beginTimeBuilder.setNanoseconds(params.beginTimeNanos);
            beginTimeBuilder.build();
            requestBuilder.setBeginTime(beginTimeBuilder);
        }

        // set end time
        if (params.endTimeSeconds != null) {
            final Timestamp.Builder endTimeBuilder = Timestamp.newBuilder();
            endTimeBuilder.setEpochSeconds(params.endTimeSeconds);
            if (params.endTimeNanos != null) endTimeBuilder.setNanoseconds(params.endTimeNanos);
            endTimeBuilder.build();
            requestBuilder.setEndTime(endTimeBuilder);
        }

        return requestBuilder.build();
    }

    public QueryTableApiResult sendQueryTable(QueryTableRequest request) {

        final DpQueryServiceGrpc.DpQueryServiceStub asyncStub = DpQueryServiceGrpc.newStub(channel);

        final QueryTableResponseObserver responseObserver = new QueryTableResponseObserver();

        // send request in separate thread to better simulate out of process grpc,
        // otherwise service handles request in this thread
        new Thread(() -> {
            asyncStub.queryTable(request, responseObserver);
        }).start();

        responseObserver.await();

        if (responseObserver.isError()) {
            return new QueryTableApiResult(true, responseObserver.getErrorMessage());
        } else {
            return new QueryTableApiResult(responseObserver.getQueryResponse());
        }
    }

    public QueryTableApiResult queryTable(
            QueryTableRequestParams params
    ) {
        final QueryTableRequest request = buildQueryTableRequest(params);
        return sendQueryTable(request);
    }

    public static QueryPvMetadataRequest buildQueryPvMetadataRequest(List<String> pvNames) {

        QueryPvMetadataRequest.Builder requestBuilder = QueryPvMetadataRequest.newBuilder();

        PvNameList.Builder pvNameListBuilder = PvNameList.newBuilder();
        pvNameListBuilder.addAllPvNames(pvNames);
        pvNameListBuilder.build();

        requestBuilder.setPvNameList(pvNameListBuilder);
        return requestBuilder.build();
    }
    
    public static QueryPvMetadataRequest buildQueryPvMetadataRequest(String columnNamePattern) {

        QueryPvMetadataRequest.Builder requestBuilder = QueryPvMetadataRequest.newBuilder();

        PvNamePattern.Builder pvNamePatternBuilder = PvNamePattern.newBuilder();
        pvNamePatternBuilder.setPattern(columnNamePattern);
        pvNamePatternBuilder.build();

        requestBuilder.setPvNamePattern(pvNamePatternBuilder);
        return requestBuilder.build();
    }

    public QueryPvMetadataApiResult sendQueryPvMetadata(
            QueryPvMetadataRequest request
    ) {
        final DpQueryServiceGrpc.DpQueryServiceStub asyncStub = DpQueryServiceGrpc.newStub(channel);

        final QueryPvMetadataResponseObserver responseObserver = new QueryPvMetadataResponseObserver();

        // send request in separate thread to better simulate out of process grpc,
        // otherwise service handles request in this thread
        new Thread(() -> {
            asyncStub.queryPvMetadata(request, responseObserver);
        }).start();

        responseObserver.await();

        if (responseObserver.isError()) {
            return new QueryPvMetadataApiResult(true, responseObserver.getErrorMessage());
        } else {
            return new QueryPvMetadataApiResult(responseObserver.getResponse());
        }
    }
    
    public QueryPvMetadataApiResult queryPvMetadata(
            List<String> columnNames
    ) {
        final QueryPvMetadataRequest request = buildQueryPvMetadataRequest(columnNames);
        return sendQueryPvMetadata(request);
    }

    public QueryPvMetadataApiResult queryPvMetadata(
            String columnNamePattern
    ) {
        final QueryPvMetadataRequest request = buildQueryPvMetadataRequest(columnNamePattern);
        return sendQueryPvMetadata(request);
    }
    
    public static QueryProvidersRequest buildQueryProvidersRequest(QueryProvidersRequestParams params) {

        QueryProvidersRequest.Builder requestBuilder = QueryProvidersRequest.newBuilder();

        if (params.idCriterion != null) {
            QueryProvidersRequest.Criterion.IdCriterion criterion =
                    QueryProvidersRequest.Criterion.IdCriterion.newBuilder()
                            .setId(params.idCriterion)
                            .build();
            QueryProvidersRequest.Criterion criteria = QueryProvidersRequest.Criterion.newBuilder()
                    .setIdCriterion(criterion)
                    .build();
            requestBuilder.addCriteria(criteria);
        }

        if (params.textCriterion != null) {
            QueryProvidersRequest.Criterion.TextCriterion criterion =
                    QueryProvidersRequest.Criterion.TextCriterion.newBuilder()
                            .setText(params.textCriterion)
                            .build();
            QueryProvidersRequest.Criterion criteria = QueryProvidersRequest.Criterion.newBuilder()
                    .setTextCriterion(criterion)
                    .build();
            requestBuilder.addCriteria(criteria);
        }

        if (params.tagsCriterion != null) {
            QueryProvidersRequest.Criterion.TagsCriterion criterion =
                    QueryProvidersRequest.Criterion.TagsCriterion.newBuilder()
                            .setTagValue(params.tagsCriterion)
                            .build();
            QueryProvidersRequest.Criterion criteria = QueryProvidersRequest.Criterion.newBuilder()
                    .setTagsCriterion(criterion)
                    .build();
            requestBuilder.addCriteria(criteria);
        }

        if (params.attributesCriterionKey != null && params.attributesCriterionValue != null) {
            QueryProvidersRequest.Criterion.AttributesCriterion criterion =
                    QueryProvidersRequest.Criterion.AttributesCriterion.newBuilder()
                            .setKey(params.attributesCriterionKey)
                            .setValue(params.attributesCriterionValue)
                            .build();
            QueryProvidersRequest.Criterion criteria = QueryProvidersRequest.Criterion.newBuilder()
                    .setAttributesCriterion(criterion)
                    .build();
            requestBuilder.addCriteria(criteria);
        }

        return requestBuilder.build();
    }

    private QueryProvidersApiResult sendQueryProviders(
            QueryProvidersRequest request
    ) {
        final DpQueryServiceGrpc.DpQueryServiceStub asyncStub = DpQueryServiceGrpc.newStub(channel);

        final QueryProvidersResponseObserver responseObserver = new QueryProvidersResponseObserver();

        // send request in separate thread to better simulate out of process grpc,
        // otherwise service handles request in this thread
        new Thread(() -> {
            asyncStub.queryProviders(request, responseObserver);
        }).start();

        responseObserver.await();

        if (responseObserver.isError()) {
            return new QueryProvidersApiResult(true, responseObserver.getErrorMessage());
        } else {
            return new QueryProvidersApiResult(responseObserver.getProviderInfoList());
        }
    }

    protected QueryProvidersApiResult queryProviders(
            QueryProvidersRequestParams queryParams
    ) {
        final QueryProvidersRequest request = buildQueryProvidersRequest(queryParams);
        return sendQueryProviders(request);
    }

}
