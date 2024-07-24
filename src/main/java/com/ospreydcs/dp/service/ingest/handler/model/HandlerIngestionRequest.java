package com.ospreydcs.dp.service.ingest.handler.model;

import com.ospreydcs.dp.grpc.v1.ingestion.IngestDataRequest;

public class HandlerIngestionRequest {

    public IngestDataRequest request = null;
    public Boolean rejected = null;
    public String rejectMsg = null;

    public HandlerIngestionRequest(
            IngestDataRequest request,
            boolean rejected,
            String rejectMsg) {

        this.request = request;
        this.rejected = rejected;
        this.rejectMsg = rejectMsg;
    }
}
