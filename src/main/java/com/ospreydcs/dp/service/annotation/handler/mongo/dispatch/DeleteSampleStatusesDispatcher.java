package com.ospreydcs.dp.service.annotation.handler.mongo.dispatch;

import com.ospreydcs.dp.grpc.v1.annotation.DeleteSampleStatusesRequest;
import com.ospreydcs.dp.grpc.v1.annotation.DeleteSampleStatusesResponse;
import com.ospreydcs.dp.service.annotation.service.AnnotationServiceImpl;
import com.ospreydcs.dp.service.common.handler.Dispatcher;
import com.ospreydcs.dp.service.common.model.MongoCountResult;
import com.ospreydcs.dp.service.common.model.ResultStatus;
import io.grpc.stub.StreamObserver;

public class DeleteSampleStatusesDispatcher extends Dispatcher {

    private final StreamObserver<DeleteSampleStatusesResponse> responseObserver;
    private final DeleteSampleStatusesRequest request;

    public DeleteSampleStatusesDispatcher(
            StreamObserver<DeleteSampleStatusesResponse> responseObserver,
            DeleteSampleStatusesRequest request
    ) {
        this.responseObserver = responseObserver;
        this.request = request;
    }

    public void handleValidationError(ResultStatus resultStatus) {
        AnnotationServiceImpl.sendDeleteSampleStatusesResponseReject(resultStatus.msg, responseObserver);
    }

    public void handleError(String errorMsg) {
        AnnotationServiceImpl.sendDeleteSampleStatusesResponseError(errorMsg, responseObserver);
    }

    public void handleResult(MongoCountResult result) {
        if (result.isReject) {
            // business-rule rejection detected in the mongo client, not an infrastructure failure
            AnnotationServiceImpl.sendDeleteSampleStatusesResponseReject(result.message, responseObserver);
        } else if (result.isError) {
            AnnotationServiceImpl.sendDeleteSampleStatusesResponseError(result.message, responseObserver);
        } else {
            // a delete matching nothing is a success with deletedCount = 0, not an ExceptionalResult
            AnnotationServiceImpl.sendDeleteSampleStatusesResponseSuccess(result.count, responseObserver);
        }
    }
}
