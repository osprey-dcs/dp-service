package com.ospreydcs.dp.service.query.handler.model;

import com.ospreydcs.dp.grpc.v1.query.QueryRequest;
import com.ospreydcs.dp.grpc.v1.query.QueryResponse;
import io.grpc.stub.StreamObserver;

public class HandlerQueryRequest {

    public QueryRequest request = null;
    public StreamObserver<QueryResponse> responseObserver = null;

    public HandlerQueryRequest(QueryRequest request, StreamObserver<QueryResponse> responseObserver) {
        this.request = request;
        this.responseObserver = responseObserver;
    }

}
