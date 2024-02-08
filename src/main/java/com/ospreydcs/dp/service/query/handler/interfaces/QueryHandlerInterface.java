package com.ospreydcs.dp.service.query.handler.interfaces;

import com.ospreydcs.dp.grpc.v1.query.QueryRequest;
import com.ospreydcs.dp.grpc.v1.query.QueryResponse;
import com.ospreydcs.dp.service.common.model.ValidationResult;
import io.grpc.stub.StreamObserver;

public interface QueryHandlerInterface {
    boolean init();
    boolean fini();
    boolean start();
    boolean stop();
    ValidationResult validateQuerySpec(
            QueryRequest.QuerySpec querySpec);
    void handleQueryResponseStream(
            QueryRequest.QuerySpec querySpec, StreamObserver<QueryResponse> responseObserver);
    ResultCursorInterface handleQueryResponseCursor(
            QueryRequest.QuerySpec querySpec, StreamObserver<QueryResponse> responseObserver);
    void handleQueryResponseSingle(
            QueryRequest.QuerySpec querySpec, StreamObserver<QueryResponse> responseObserver);
    void handleQueryResponseTable(
            QueryRequest.QuerySpec querySpec, StreamObserver<QueryResponse> responseObserver);
    void handleGetColumnInfo(
            QueryRequest.ColumnInfoQuerySpec spec, StreamObserver<QueryResponse> responseObserver);
}
