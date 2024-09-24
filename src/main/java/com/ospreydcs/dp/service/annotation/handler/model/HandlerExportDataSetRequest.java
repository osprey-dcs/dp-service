package com.ospreydcs.dp.service.annotation.handler.model;

import com.ospreydcs.dp.grpc.v1.annotation.ExportDataSetRequest;
import com.ospreydcs.dp.grpc.v1.annotation.ExportDataSetResponse;
import io.grpc.stub.StreamObserver;

public class HandlerExportDataSetRequest {

    public final ExportDataSetRequest exportDataSetRequest;
    public final StreamObserver<ExportDataSetResponse> responseObserver;

    public HandlerExportDataSetRequest(
            ExportDataSetRequest exportDataSetRequest,
            StreamObserver<ExportDataSetResponse> responseObserver
    ) {
        this.exportDataSetRequest = exportDataSetRequest;
        this.responseObserver = responseObserver;
    }
}
