package com.ospreydcs.dp.service.ingest.handler.model;

import com.ospreydcs.dp.grpc.v1.ingestion.IngestDataRequest;
import com.ospreydcs.dp.grpc.v1.ingestion.IngestDataResponse;
import io.grpc.stub.StreamObserver;

public class HandlerIngestionRequest {

    public IngestDataRequest request = null;
    public StreamObserver<IngestDataResponse> responseObserver = null;
    public Boolean rejected = null;
    public String rejectMsg = null;

    public HandlerIngestionRequest(
            IngestDataRequest request,
            StreamObserver<IngestDataResponse> responseObserver,
            boolean rejected,
            String rejectMsg) {

        this.request = request;
        this.responseObserver = responseObserver;
        this.rejected = rejected;
        this.rejectMsg = rejectMsg;
    }
}
