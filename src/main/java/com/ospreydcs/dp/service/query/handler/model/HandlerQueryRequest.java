package com.ospreydcs.dp.service.query.handler.model;

import com.ospreydcs.dp.grpc.v1.query.QueryRequest;
import com.ospreydcs.dp.grpc.v1.query.QueryResponse;
import io.grpc.stub.StreamObserver;

public class HandlerQueryRequest {

    public QueryRequest.QuerySpec querySpec = null;
    public StreamObserver<QueryResponse> responseObserver = null;

    public HandlerQueryRequest(QueryRequest.QuerySpec querySpec, StreamObserver<QueryResponse> responseObserver) {
        this.querySpec = querySpec;
        this.responseObserver = responseObserver;
    }

}
