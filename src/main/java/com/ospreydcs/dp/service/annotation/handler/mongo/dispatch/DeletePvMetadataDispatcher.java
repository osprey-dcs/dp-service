package com.ospreydcs.dp.service.annotation.handler.mongo.dispatch;

import com.ospreydcs.dp.grpc.v1.annotation.DeletePvMetadataRequest;
import com.ospreydcs.dp.grpc.v1.annotation.DeletePvMetadataResponse;
import com.ospreydcs.dp.service.annotation.service.AnnotationServiceImpl;
import com.ospreydcs.dp.service.common.handler.Dispatcher;
import com.ospreydcs.dp.service.common.model.MongoDeleteResult;
import com.ospreydcs.dp.service.common.model.ResultStatus;
import io.grpc.stub.StreamObserver;

public class DeletePvMetadataDispatcher extends Dispatcher {

    private final StreamObserver<DeletePvMetadataResponse> responseObserver;
    private final DeletePvMetadataRequest request;

    public DeletePvMetadataDispatcher(
            StreamObserver<DeletePvMetadataResponse> responseObserver,
            DeletePvMetadataRequest request
    ) {
        this.responseObserver = responseObserver;
        this.request = request;
    }

    public void handleValidationError(ResultStatus resultStatus) {
        AnnotationServiceImpl.sendDeletePvMetadataResponseReject(resultStatus.msg, responseObserver);
    }

    public void handleResult(MongoDeleteResult result) {
        if (result.isError) {
            AnnotationServiceImpl.sendDeletePvMetadataResponseError(result.message, responseObserver);
        } else if (result.deletedPvName == null) {
            final String msg = "no PvMetadata record found for: " + request.getPvNameOrAlias();
            AnnotationServiceImpl.sendDeletePvMetadataResponseReject(msg, responseObserver);
        } else {
            AnnotationServiceImpl.sendDeletePvMetadataResponseSuccess(result.deletedPvName, responseObserver);
        }
    }
}
