package com.ospreydcs.dp.service.ingest.handler.interfaces;

import com.ospreydcs.dp.grpc.v1.ingestion.QueryRequestStatusRequest;
import com.ospreydcs.dp.grpc.v1.ingestion.QueryRequestStatusResponse;
import com.ospreydcs.dp.service.ingest.handler.model.HandlerIngestionRequest;
import io.grpc.stub.StreamObserver;

public interface IngestionHandlerInterface {

    boolean init();
    boolean fini();
    boolean start();
    boolean stop();

    void handleIngestionRequest(HandlerIngestionRequest request);

    void handleQueryRequestStatus(
            QueryRequestStatusRequest request, StreamObserver<QueryRequestStatusResponse> responseObserver);
}
