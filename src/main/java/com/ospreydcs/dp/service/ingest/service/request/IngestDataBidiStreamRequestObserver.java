package com.ospreydcs.dp.service.ingest.service.request;

import com.ospreydcs.dp.grpc.v1.ingestion.IngestDataRequest;
import com.ospreydcs.dp.grpc.v1.ingestion.IngestDataResponse;
import com.ospreydcs.dp.service.ingest.handler.interfaces.IngestionHandlerInterface;
import com.ospreydcs.dp.service.ingest.service.IngestionServiceImpl;
import io.grpc.stub.StreamObserver;

public class IngestDataBidiStreamRequestObserver extends IngestionStreamRequestObserverBase {

    private final StreamObserver<IngestDataResponse> responseObserver;

    public IngestDataBidiStreamRequestObserver(
            StreamObserver<IngestDataResponse> responseObserver,
            IngestionHandlerInterface handler,
            IngestionServiceImpl serviceImpl
    ) {
        super(handler, serviceImpl);
        this.responseObserver = responseObserver;
    }

    @Override
    protected void handleIngestionRequest_(IngestDataRequest request) {
        serviceImpl.handleIngestionRequest(request, responseObserver);
    }

    @Override
    protected void handleClose_() {
        responseObserver.onCompleted();
    }
}
