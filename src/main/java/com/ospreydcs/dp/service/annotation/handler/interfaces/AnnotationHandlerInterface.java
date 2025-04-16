package com.ospreydcs.dp.service.annotation.handler.interfaces;

import com.ospreydcs.dp.grpc.v1.annotation.*;
import com.ospreydcs.dp.service.annotation.handler.model.HandlerExportDataRequest;
import io.grpc.stub.StreamObserver;

public interface AnnotationHandlerInterface {

    boolean init();
    boolean fini();
    boolean start();
    boolean stop();

    void handleCreateDataSet(
            CreateDataSetRequest request,
            StreamObserver<CreateDataSetResponse> responseObserver);

    void handleQueryDataSets(QueryDataSetsRequest request, StreamObserver<QueryDataSetsResponse> responseObserver);

    void handleCreateAnnotation(
            CreateAnnotationRequest request,
            StreamObserver<CreateAnnotationResponse> responseObserver);

    void handleQueryAnnotations(
            QueryAnnotationsRequest request, StreamObserver<QueryAnnotationsResponse> responseObserver);

    void handleExportData(HandlerExportDataRequest handlerRequest);
}
