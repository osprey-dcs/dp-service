package com.ospreydcs.dp.service.annotation.handler.mongo.dispatch;

import com.ospreydcs.dp.grpc.v1.annotation.DeleteAnnotationRequest;
import com.ospreydcs.dp.grpc.v1.annotation.DeleteAnnotationResponse;
import com.ospreydcs.dp.service.annotation.service.AnnotationServiceImpl;
import com.ospreydcs.dp.service.common.handler.Dispatcher;
import com.ospreydcs.dp.service.common.model.MongoDeleteResult;
import com.ospreydcs.dp.service.common.model.ResultStatus;
import io.grpc.stub.StreamObserver;

public class DeleteAnnotationDispatcher extends Dispatcher {

    private final StreamObserver<DeleteAnnotationResponse> responseObserver;
    private final DeleteAnnotationRequest request;

    public DeleteAnnotationDispatcher(
            StreamObserver<DeleteAnnotationResponse> responseObserver,
            DeleteAnnotationRequest request
    ) {
        this.responseObserver = responseObserver;
        this.request = request;
    }

    public void handleValidationError(ResultStatus resultStatus) {
        AnnotationServiceImpl.sendDeleteAnnotationResponseReject(resultStatus.msg, responseObserver);
    }

    public void handleResult(MongoDeleteResult result) {
        if (result.isReject) {
            // business-rule rejection detected in the mongo client, not an infrastructure failure
            AnnotationServiceImpl.sendDeleteAnnotationResponseReject(result.message, responseObserver);
        } else if (result.isError) {
            AnnotationServiceImpl.sendDeleteAnnotationResponseError(result.message, responseObserver);
        } else if (result.deletedIdentifier == null) {
            final String msg = "no Annotation record found for id: " + request.getAnnotationId();
            AnnotationServiceImpl.sendDeleteAnnotationResponseReject(msg, responseObserver);
        } else {
            AnnotationServiceImpl.sendDeleteAnnotationResponseSuccess(result.deletedIdentifier, responseObserver);
        }
    }
}
