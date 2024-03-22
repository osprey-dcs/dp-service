package com.ospreydcs.dp.service.annotation.handler.interfaces;

import com.ospreydcs.dp.grpc.v1.annotation.CreateAnnotationRequest;
import com.ospreydcs.dp.grpc.v1.annotation.CreateAnnotationResponse;
import com.ospreydcs.dp.grpc.v1.annotation.CreateDataSetRequest;
import com.ospreydcs.dp.grpc.v1.annotation.CreateDataSetResponse;
import io.grpc.stub.StreamObserver;

public interface AnnotationHandlerInterface {

    boolean init();
    boolean fini();
    boolean start();
    boolean stop();

    void handleCreateDataSetRequest(
            CreateDataSetRequest request,
            StreamObserver<CreateDataSetResponse> responseObserver);

    void handleCreateCommentAnnotationRequest(
            CreateAnnotationRequest request,
            StreamObserver<CreateAnnotationResponse> responseObserver);

}
