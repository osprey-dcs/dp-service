package com.ospreydcs.dp.service.ingest.handler.model;

import com.ospreydcs.dp.grpc.v1.ingestion.IngestDataRequest;

public class HandlerIngestionRequest {

    public IngestDataRequest request = null;
    public Boolean rejected = null;
    public String rejectMsg = null;

    /**
     * When the service received this request, stamped by the constructor (issue #212, §2).
     *
     * <p>This is the start of the only latency that describes ingestion. The gRPC call duration
     * for {@code ingestData} cannot serve: the service acks as soon as the request is validated
     * and enqueued, so the RPC ends before the provider lookup, the bucket generation, the
     * {@code insertMany}, and the {@code requestStatus} insert have run. An operator watching the
     * RPC duration alone would read ingestion as healthy while the queue behind it fell
     * arbitrarily far behind.
     *
     * <p>Stamped in the constructor rather than passed in because both construction sites
     * ({@code IngestionServiceImpl.handleIngestionRequest()} and
     * {@code IngestDataStreamRequestObserver.handleIngestionRequest_()}) build the request
     * immediately after validating and immediately before enqueueing it, so the constructor is
     * the arrival point, and a third site added later is instrumented without having to know
     * that it should be.
     *
     * <p>Measured on {@link System#nanoTime()}, so it is comparable only against other readings
     * from the same source, never against wall-clock time.
     */
    public final long arrivalNanos;

    public HandlerIngestionRequest(
            IngestDataRequest request,
            boolean rejected,
            String rejectMsg) {

        this.request = request;
        this.rejected = rejected;
        this.rejectMsg = rejectMsg;
        this.arrivalNanos = System.nanoTime();
    }
}
