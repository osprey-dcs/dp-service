package com.ospreydcs.dp.service.annotation.handler.mongo.dispatch;

import com.ospreydcs.dp.grpc.v1.annotation.DeleteDataSetRequest;
import com.ospreydcs.dp.grpc.v1.annotation.DeleteDataSetResponse;
import com.ospreydcs.dp.service.annotation.service.AnnotationServiceImpl;
import com.ospreydcs.dp.service.common.handler.Dispatcher;
import com.ospreydcs.dp.service.common.model.MongoDeleteResult;
import com.ospreydcs.dp.service.common.model.ResultStatus;
import io.grpc.stub.StreamObserver;

public class DeleteDataSetDispatcher extends Dispatcher {

    private final StreamObserver<DeleteDataSetResponse> responseObserver;
    private final DeleteDataSetRequest request;

    public DeleteDataSetDispatcher(
            StreamObserver<DeleteDataSetResponse> responseObserver,
            DeleteDataSetRequest request
    ) {
        this.responseObserver = responseObserver;
        this.request = request;
    }

    public void handleValidationError(ResultStatus resultStatus) {
        AnnotationServiceImpl.sendDeleteDataSetResponseReject(resultStatus.msg, responseObserver);
    }

    public void handleResult(MongoDeleteResult result) {
        if (result.isReject) {
            // business-rule rejection detected in the mongo client (referencing annotations exist),
            // not an infrastructure failure
            AnnotationServiceImpl.sendDeleteDataSetResponseReject(result.message, responseObserver);
        } else if (result.isError) {
            AnnotationServiceImpl.sendDeleteDataSetResponseError(result.message, responseObserver);
        } else if (result.deletedIdentifier == null) {
            final String msg = "no DataSet record found for id: " + request.getDataSetId();
            AnnotationServiceImpl.sendDeleteDataSetResponseReject(msg, responseObserver);
        } else {
            AnnotationServiceImpl.sendDeleteDataSetResponseSuccess(result.deletedIdentifier, responseObserver);
        }
    }
}
