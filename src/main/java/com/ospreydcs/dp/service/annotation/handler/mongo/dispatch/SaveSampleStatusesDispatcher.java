package com.ospreydcs.dp.service.annotation.handler.mongo.dispatch;

import com.ospreydcs.dp.grpc.v1.annotation.SaveSampleStatusesRequest;
import com.ospreydcs.dp.grpc.v1.annotation.SaveSampleStatusesResponse;
import com.ospreydcs.dp.service.annotation.service.AnnotationServiceImpl;
import com.ospreydcs.dp.service.common.handler.Dispatcher;
import com.ospreydcs.dp.service.common.model.MongoCountResult;
import com.ospreydcs.dp.service.common.model.ResultStatus;
import io.grpc.stub.StreamObserver;

public class SaveSampleStatusesDispatcher extends Dispatcher {

    private final StreamObserver<SaveSampleStatusesResponse> responseObserver;
    private final SaveSampleStatusesRequest request;

    public SaveSampleStatusesDispatcher(
            StreamObserver<SaveSampleStatusesResponse> responseObserver,
            SaveSampleStatusesRequest request
    ) {
        this.responseObserver = responseObserver;
        this.request = request;
    }

    public void handleValidationError(ResultStatus resultStatus) {
        AnnotationServiceImpl.sendSaveSampleStatusesResponseReject(resultStatus.msg, responseObserver);
    }

    public void handleError(String errorMsg) {
        AnnotationServiceImpl.sendSaveSampleStatusesResponseError(errorMsg, responseObserver);
    }

    public void handleResult(MongoCountResult result) {
        if (result.isError) {
            AnnotationServiceImpl.sendSaveSampleStatusesResponseError(result.message, responseObserver);
        } else {
            AnnotationServiceImpl.sendSaveSampleStatusesResponseSuccess(result.count, responseObserver);
        }
    }
}
