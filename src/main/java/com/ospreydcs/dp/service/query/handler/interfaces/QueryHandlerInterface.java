package com.ospreydcs.dp.service.query.handler.interfaces;

import com.ospreydcs.dp.grpc.v1.query.*;
import com.ospreydcs.dp.service.common.model.ResultStatus;
import io.grpc.stub.StreamObserver;

public interface QueryHandlerInterface {

    boolean init();
    boolean fini();
    boolean start();
    boolean stop();

    ResultStatus validateQuerySpecData(QueryDataRequest.QuerySpec querySpec);

    ResultStatus validateQueryTableRequest(QueryTableRequest request);

    void handleQueryDataStream(
            QueryDataRequest.QuerySpec querySpec, StreamObserver<QueryDataResponse> responseObserver);

    ResultCursorInterface handleQueryDataBidiStream(
            QueryDataRequest.QuerySpec querySpec, StreamObserver<QueryDataResponse> responseObserver);

    void handleQueryData(
            QueryDataRequest.QuerySpec querySpec, StreamObserver<QueryDataResponse> responseObserver);

    void handleQueryTable(
            QueryTableRequest request, StreamObserver<QueryTableResponse> responseObserver);

    void handleQueryPvStats(
            QueryPvStatsRequest request, StreamObserver<QueryPvStatsResponse> responseObserver);

    void handleQueryProviders(
            QueryProvidersRequest request, StreamObserver<QueryProvidersResponse> responseObserver);

    void handleQueryProviderStats(
            QueryProviderStatsRequest request, StreamObserver<QueryProviderStatsResponse> responseObserver);

}
