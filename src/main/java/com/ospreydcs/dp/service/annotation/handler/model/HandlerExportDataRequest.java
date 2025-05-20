package com.ospreydcs.dp.service.annotation.handler.model;

import com.ospreydcs.dp.grpc.v1.annotation.ExportDataRequest;
import com.ospreydcs.dp.grpc.v1.annotation.ExportDataResponse;
import io.grpc.stub.StreamObserver;

public class HandlerExportDataRequest {

    public final ExportDataRequest exportDataRequest;
    public final StreamObserver<ExportDataResponse> responseObserver;

    public HandlerExportDataRequest(
            ExportDataRequest exportDataRequest,
            StreamObserver<ExportDataResponse> responseObserver
    ) {
        this.exportDataRequest = exportDataRequest;
        this.responseObserver = responseObserver;
    }
}
