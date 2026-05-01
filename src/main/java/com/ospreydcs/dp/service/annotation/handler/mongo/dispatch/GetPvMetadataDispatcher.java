package com.ospreydcs.dp.service.annotation.handler.mongo.dispatch;

import com.ospreydcs.dp.grpc.v1.annotation.GetPvMetadataRequest;
import com.ospreydcs.dp.grpc.v1.annotation.GetPvMetadataResponse;
import com.ospreydcs.dp.service.annotation.service.AnnotationServiceImpl;
import com.ospreydcs.dp.service.common.bson.pvmetadata.PvMetadataDocument;
import com.ospreydcs.dp.service.common.handler.Dispatcher;
import com.ospreydcs.dp.service.common.model.ResultStatus;
import io.grpc.stub.StreamObserver;

public class GetPvMetadataDispatcher extends Dispatcher {

    private final StreamObserver<GetPvMetadataResponse> responseObserver;
    private final GetPvMetadataRequest request;

    public GetPvMetadataDispatcher(
            StreamObserver<GetPvMetadataResponse> responseObserver,
            GetPvMetadataRequest request
    ) {
        this.responseObserver = responseObserver;
        this.request = request;
    }

    public void handleValidationError(ResultStatus resultStatus) {
        AnnotationServiceImpl.sendGetPvMetadataResponseReject(resultStatus.msg, responseObserver);
    }

    public void handleError(String errorMsg) {
        AnnotationServiceImpl.sendGetPvMetadataResponseError(errorMsg, responseObserver);
    }

    public void handleResult(PvMetadataDocument document) {
        if (document == null) {
            final String msg = "no PvMetadata record found for: " + request.getPvNameOrAlias();
            AnnotationServiceImpl.sendGetPvMetadataResponseReject(msg, responseObserver);
        } else {
            AnnotationServiceImpl.sendGetPvMetadataResponseSuccess(document.toPvMetadata(), responseObserver);
        }
    }
}
