package com.ospreydcs.dp.service.ingest.handler.mongo.dispatch;

import com.ospreydcs.dp.grpc.v1.ingestion.SubscribeDataRequest;
import com.ospreydcs.dp.grpc.v1.ingestion.SubscribeDataResponse;
import com.ospreydcs.dp.service.ingest.service.IngestionServiceImpl;
import io.grpc.stub.StreamObserver;

public class SubscribeDataDispatcher {
    
    // instance variables
    private final SubscribeDataRequest request;
    private final StreamObserver<SubscribeDataResponse> responseObserver;

    public SubscribeDataDispatcher(
            StreamObserver<SubscribeDataResponse> responseObserver,
            SubscribeDataRequest request
    ) {
        this.responseObserver = responseObserver;
        this.request = request;
    }

    public void sendReject(String errorMsg) {
        IngestionServiceImpl.sendSubscribeDataResponseReject(errorMsg, responseObserver);
    }

    public void sendError(String errorMsg) {
        IngestionServiceImpl.sendSubscribeDataResponseError(errorMsg, responseObserver);
    }

    public void sendAck() {
        IngestionServiceImpl.sendSubscribeDataResponseAck(responseObserver);
    }

}
