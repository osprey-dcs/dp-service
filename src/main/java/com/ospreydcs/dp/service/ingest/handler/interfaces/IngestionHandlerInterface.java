package com.ospreydcs.dp.service.ingest.handler.interfaces;

import com.ospreydcs.dp.grpc.v1.ingestion.IngestDataRequest;
import com.ospreydcs.dp.service.common.model.ValidationResult;
import com.ospreydcs.dp.service.ingest.handler.model.HandlerIngestionRequest;

public interface IngestionHandlerInterface {
    boolean init();
    boolean fini();
    boolean start();
    boolean stop();
    void handleIngestDataStream(HandlerIngestionRequest request);
}
