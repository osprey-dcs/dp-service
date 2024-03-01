package com.ospreydcs.dp.service.annotation.handler.interfaces;

import com.ospreydcs.dp.grpc.v1.annotation.CreateAnnotationRequest;
import com.ospreydcs.dp.grpc.v1.annotation.CreateAnnotationResponse;
import io.grpc.stub.StreamObserver;

public interface AnnotationHandlerInterface {
    boolean init();
    boolean fini();
    boolean start();
    boolean stop();
    void handleCreateCommentAnnotationRequest(
            CreateAnnotationRequest request,
            StreamObserver<CreateAnnotationResponse> responseObserver);
}
