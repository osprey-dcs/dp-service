package com.ospreydcs.dp.service.ingest.handler.interfaces;

import com.ospreydcs.dp.grpc.v1.ingestion.*;
import com.ospreydcs.dp.service.ingest.handler.model.HandlerIngestionRequest;
import io.grpc.stub.StreamObserver;

public interface IngestionHandlerInterface {

    boolean init();
    boolean fini();
    boolean start();
    boolean stop();

    void handleRegisterProvider(
            RegisterProviderRequest request, StreamObserver<RegisterProviderResponse> responseObserver);

    void handleIngestionRequest(HandlerIngestionRequest request);

    void handleQueryRequestStatus(
            QueryRequestStatusRequest request, StreamObserver<QueryRequestStatusResponse> responseObserver);

    void handleSubscribeData(SubscribeDataRequest request, StreamObserver<SubscribeDataResponse> responseObserver);

    void cancelDataSubscriptions(StreamObserver<SubscribeDataResponse> responseObserver);
}
