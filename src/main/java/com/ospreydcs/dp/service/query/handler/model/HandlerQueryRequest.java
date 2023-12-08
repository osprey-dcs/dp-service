package com.ospreydcs.dp.service.query.handler.model;

import com.ospreydcs.dp.grpc.v1.query.QueryDataByTimeRequest;
import com.ospreydcs.dp.grpc.v1.query.QueryDataResponse;
import io.grpc.stub.StreamObserver;

public class HandlerQueryRequest {

    public QueryDataByTimeRequest request = null;
    public StreamObserver<QueryDataResponse> responseObserver = null;

    public HandlerQueryRequest(QueryDataByTimeRequest request, StreamObserver<QueryDataResponse> responseObserver) {
        this.request = request;
        this.responseObserver = responseObserver;
    }

}
