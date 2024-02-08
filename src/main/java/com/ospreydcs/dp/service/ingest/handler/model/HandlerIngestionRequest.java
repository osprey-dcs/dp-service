package com.ospreydcs.dp.service.ingest.handler.model;

import com.ospreydcs.dp.grpc.v1.ingestion.IngestionRequest;
import com.ospreydcs.dp.grpc.v1.ingestion.IngestionResponse;
import io.grpc.stub.StreamObserver;

public class HandlerIngestionRequest {

    public IngestionRequest request = null;
    public StreamObserver<IngestionResponse> responseObserver = null;
    public Boolean rejected = null;
    public String rejectMsg = null;

    public HandlerIngestionRequest(
            IngestionRequest request,
            StreamObserver<IngestionResponse> responseObserver,
            boolean rejected,
            String rejectMsg) {

        this.request = request;
        this.responseObserver = responseObserver;
        this.rejected = rejected;
        this.rejectMsg = rejectMsg;
    }
}
