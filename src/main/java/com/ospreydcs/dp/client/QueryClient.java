package com.ospreydcs.dp.client;

import com.ospreydcs.dp.client.result.QueryProvidersApiResult;
import com.ospreydcs.dp.client.result.QueryPvStatsApiResult;
import com.ospreydcs.dp.client.result.QueryTableApiResult;
import com.ospreydcs.dp.grpc.v1.common.ExceptionalResult;
import com.ospreydcs.dp.grpc.v1.common.Timestamp;
import com.ospreydcs.dp.grpc.v1.query.*;
import io.grpc.ManagedChannel;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

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

    public static class QueryTableResponseObserver
            extends ApiResponseObserverBase<QueryTableResponse> {

        private final List<QueryTableResponse> responseList = Collections.synchronizedList(new ArrayList<>());

        @Override
        protected boolean hasExceptionalResult(QueryTableResponse response) {
            return response.hasExceptionalResult();
        }

        @Override
        protected ExceptionalResult getExceptionalResult(QueryTableResponse response) {
            return response.getExceptionalResult();
        }

        @Override
        protected boolean handleResult(QueryTableResponse response) {
            responseList.add(response);
            return true;
        }

        public QueryTableResponse getQueryResponse() {
            if (responseList.isEmpty()) {
                return null;
            } else {
                return responseList.get(0);
            }
        }
    }

    public static class QueryPvStatsResponseObserver
            extends ApiResponseObserverBase<QueryPvStatsResponse> {

        private final List<QueryPvStatsResponse> responseList =
                Collections.synchronizedList(new ArrayList<>());

        @Override
        protected boolean hasExceptionalResult(QueryPvStatsResponse response) {
            return response.hasExceptionalResult();
        }

        @Override
        protected ExceptionalResult getExceptionalResult(QueryPvStatsResponse response) {
            return response.getExceptionalResult();
        }

        @Override
        protected boolean handleResult(QueryPvStatsResponse response) {
            responseList.add(response);
            return true;
        }

        public QueryPvStatsResponse getResponse() {
            if (responseList.isEmpty()) {
                return null;
            } else {
                return responseList.get(0);
            }
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

    public static class QueryProvidersResponseObserver
            extends ApiResponseObserverBase<QueryProvidersResponse> {

        private final List<QueryProvidersResponse.ProvidersResult.ProviderInfo> providerInfoList =
                Collections.synchronizedList(new ArrayList<>());

        @Override
        protected boolean hasExceptionalResult(QueryProvidersResponse response) {
            return response.hasExceptionalResult();
        }

        @Override
        protected ExceptionalResult getExceptionalResult(QueryProvidersResponse response) {
            return response.getExceptionalResult();
        }

        @Override
        protected boolean handleResult(QueryProvidersResponse response) {

            if (!response.hasProvidersResult()) {
                recordFailure(observerName() + " response does not contain ProvidersResult");
                return false;
            }

            providerInfoList.addAll(response.getProvidersResult().getProviderInfosList());
            return true;
        }

        public List<QueryProvidersResponse.ProvidersResult.ProviderInfo> getProviderInfoList() {
            return providerInfoList;
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
            return new QueryTableApiResult(
                    true, responseObserver.getErrorMessage(), responseObserver.getApiResultStatus());
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

    public static QueryPvStatsRequest buildQueryPvStatsRequest(List<String> pvNames) {

        QueryPvStatsRequest.Builder requestBuilder = QueryPvStatsRequest.newBuilder();

        PvNameList.Builder pvNameListBuilder = PvNameList.newBuilder();
        pvNameListBuilder.addAllPvNames(pvNames);
        pvNameListBuilder.build();

        requestBuilder.setPvNameList(pvNameListBuilder);
        return requestBuilder.build();
    }

    public static QueryPvStatsRequest buildQueryPvStatsRequest(String columnNamePattern) {

        QueryPvStatsRequest.Builder requestBuilder = QueryPvStatsRequest.newBuilder();

        PvNamePattern.Builder pvNamePatternBuilder = PvNamePattern.newBuilder();
        pvNamePatternBuilder.setPattern(columnNamePattern);
        pvNamePatternBuilder.build();

        requestBuilder.setPvNamePattern(pvNamePatternBuilder);
        return requestBuilder.build();
    }

    public QueryPvStatsApiResult sendQueryPvStats(
            QueryPvStatsRequest request
    ) {
        final DpQueryServiceGrpc.DpQueryServiceStub asyncStub = DpQueryServiceGrpc.newStub(channel);

        final QueryPvStatsResponseObserver responseObserver = new QueryPvStatsResponseObserver();

        // send request in separate thread to better simulate out of process grpc,
        // otherwise service handles request in this thread
        new Thread(() -> {
            asyncStub.queryPvStats(request, responseObserver);
        }).start();

        responseObserver.await();

        if (responseObserver.isError()) {
            return new QueryPvStatsApiResult(
                    true, responseObserver.getErrorMessage(), responseObserver.getApiResultStatus());
        } else {
            return new QueryPvStatsApiResult(responseObserver.getResponse());
        }
    }

    public QueryPvStatsApiResult queryPvStats(
            List<String> columnNames
    ) {
        final QueryPvStatsRequest request = buildQueryPvStatsRequest(columnNames);
        return sendQueryPvStats(request);
    }

    public QueryPvStatsApiResult queryPvStats(
            String columnNamePattern
    ) {
        final QueryPvStatsRequest request = buildQueryPvStatsRequest(columnNamePattern);
        return sendQueryPvStats(request);
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

    public QueryProvidersApiResult sendQueryProviders(
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
            return new QueryProvidersApiResult(
                    true, responseObserver.getErrorMessage(), responseObserver.getApiResultStatus());
        } else {
            return new QueryProvidersApiResult(responseObserver.getProviderInfoList());
        }
    }

    public QueryProvidersApiResult queryProviders(
            QueryProvidersRequestParams queryParams
    ) {
        final QueryProvidersRequest request = buildQueryProvidersRequest(queryParams);
        return sendQueryProviders(request);
    }

}
