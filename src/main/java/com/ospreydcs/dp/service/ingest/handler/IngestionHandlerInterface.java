package com.ospreydcs.dp.service.ingest.handler;

import com.ospreydcs.dp.grpc.v1.ingestion.IngestionRequest;
import com.ospreydcs.dp.service.ingest.handler.model.HandlerIngestionRequest;
import com.ospreydcs.dp.service.ingest.handler.model.ValidateIngestionRequestResult;

public interface IngestionHandlerInterface {
    boolean init();
    boolean fini();
    boolean start();
    boolean stop();
    ValidateIngestionRequestResult validateIngestionRequest(IngestionRequest request);
    void onNext(HandlerIngestionRequest request);
}
