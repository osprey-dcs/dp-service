package com.ospreydcs.dp.service.ingest.handler.interfaces;

import com.ospreydcs.dp.service.ingest.handler.model.HandlerIngestionRequest;

public interface IngestionHandlerInterface {
    boolean init();
    boolean fini();
    boolean start();
    boolean stop();
    void handleIngestionRequest(HandlerIngestionRequest request);
}
