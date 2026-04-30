package com.ospreydcs.dp.service.annotation.handler.mongo.dispatch;

import com.ospreydcs.dp.grpc.v1.annotation.SavePvMetadataRequest;
import com.ospreydcs.dp.grpc.v1.annotation.SavePvMetadataResponse;
import com.ospreydcs.dp.service.annotation.service.AnnotationServiceImpl;
import com.ospreydcs.dp.service.common.handler.Dispatcher;
import com.ospreydcs.dp.service.common.model.MongoSaveResult;
import com.ospreydcs.dp.service.common.model.ResultStatus;
import io.grpc.stub.StreamObserver;

public class SavePvMetadataDispatcher extends Dispatcher {

    private final StreamObserver<SavePvMetadataResponse> responseObserver;
    private final SavePvMetadataRequest request;

    public SavePvMetadataDispatcher(
            StreamObserver<SavePvMetadataResponse> responseObserver,
            SavePvMetadataRequest request
    ) {
        this.responseObserver = responseObserver;
        this.request = request;
    }

    public void handleValidationError(ResultStatus resultStatus) {
        AnnotationServiceImpl.sendSavePvMetadataResponseReject(resultStatus.msg, responseObserver);
    }

    public void handleError(String errorMsg) {
        AnnotationServiceImpl.sendSavePvMetadataResponseError(errorMsg, responseObserver);
    }

    public void handleResult(MongoSaveResult result) {
        if (result.isError) {
            AnnotationServiceImpl.sendSavePvMetadataResponseError(result.message, responseObserver);
        } else {
            AnnotationServiceImpl.sendSavePvMetadataResponseSuccess(result.documentId, responseObserver);
        }
    }
}
