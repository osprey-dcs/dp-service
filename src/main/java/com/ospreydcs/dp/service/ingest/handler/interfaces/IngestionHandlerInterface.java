package com.ospreydcs.dp.service.ingest.handler.interfaces;

import com.ospreydcs.dp.grpc.v1.ingestion.*;
import com.ospreydcs.dp.service.ingest.handler.model.HandlerIngestionRequest;
import com.ospreydcs.dp.service.ingest.model.SourceMonitor;
import io.grpc.stub.StreamObserver;

public interface IngestionHandlerInterface {

    boolean init();
    boolean fini();
    boolean start();
    boolean stop();
    boolean getShutdownRequested();

    void handleRegisterProvider(
            RegisterProviderRequest request, StreamObserver<RegisterProviderResponse> responseObserver);

    void handleIngestionRequest(HandlerIngestionRequest request);

    void handleQueryRequestStatus(
            QueryRequestStatusRequest request, StreamObserver<QueryRequestStatusResponse> responseObserver);

    SourceMonitor handleSubscribeData(
            SubscribeDataRequest request,
            StreamObserver<SubscribeDataResponse> responseStreamObserver);

    void terminateSourceMonitor(SourceMonitor monitor);

}
