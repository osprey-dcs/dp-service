package com.ospreydcs.dp.service.ingest.handler;

import com.ospreydcs.dp.grpc.v1.ingestion.IngestionRequest;
import com.ospreydcs.dp.service.common.model.ValidationResult;
import com.ospreydcs.dp.service.ingest.handler.model.HandlerIngestionRequest;

public interface IngestionHandlerInterface {
    boolean init();
    boolean fini();
    boolean start();
    boolean stop();
    ValidationResult validateIngestionRequest(IngestionRequest request);
    void onNext(HandlerIngestionRequest request);
}
