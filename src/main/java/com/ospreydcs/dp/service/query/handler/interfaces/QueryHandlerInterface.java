package com.ospreydcs.dp.service.query.handler.interfaces;

import com.ospreydcs.dp.grpc.v1.query.*;
import com.ospreydcs.dp.service.common.model.ValidationResult;
import io.grpc.stub.StreamObserver;

public interface QueryHandlerInterface {

    boolean init();
    boolean fini();
    boolean start();
    boolean stop();

    ValidationResult validateQuerySpecData(QueryDataRequest.QuerySpec querySpec);

    void handleQueryDataStream(
            QueryDataRequest.QuerySpec querySpec, StreamObserver<QueryDataResponse> responseObserver);

    ResultCursorInterface handleQueryDataBidiStream(
            QueryDataRequest.QuerySpec querySpec, StreamObserver<QueryDataResponse> responseObserver);

    void handleQueryData(
            QueryDataRequest.QuerySpec querySpec, StreamObserver<QueryDataResponse> responseObserver);

    void handleQueryDataTable(
            QueryDataRequest.QuerySpec querySpec, StreamObserver<QueryTableResponse> responseObserver);

    void handleQueryMetadata(
            QueryMetadataRequest request, StreamObserver<QueryMetadataResponse> responseObserver);

}
