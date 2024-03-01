package com.ospreydcs.dp.service.annotation.service;

import com.ospreydcs.dp.grpc.v1.annotation.CreateAnnotationRequest;
import com.ospreydcs.dp.grpc.v1.annotation.CreateAnnotationResponse;
import com.ospreydcs.dp.grpc.v1.annotation.DpAnnotationServiceGrpc;
import com.ospreydcs.dp.grpc.v1.common.RejectionDetails;
import com.ospreydcs.dp.grpc.v1.common.ResponseType;
import com.ospreydcs.dp.service.annotation.handler.AnnotationValidationUtility;
import com.ospreydcs.dp.service.annotation.handler.interfaces.AnnotationHandlerInterface;
import com.ospreydcs.dp.service.common.grpc.GrpcUtility;
import com.ospreydcs.dp.service.common.model.ValidationResult;
import io.grpc.stub.StreamObserver;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class AnnotationServiceImpl extends DpAnnotationServiceGrpc.DpAnnotationServiceImplBase {

    // static variables
    private static final Logger logger = LogManager.getLogger();

    // instance variables
    private AnnotationHandlerInterface handler;

    public boolean init(AnnotationHandlerInterface handler) {
        this.handler = handler;
        if (!handler.init()) {
            logger.error("handler.init failed");
            return false;
        }
        if (!handler.start()) {
            logger.error("handler.start failed");
        }
        return true;
    }

    public void fini() {
        if (handler != null) {
            handler.stop();
            handler.fini();
            handler = null;
        }
    }

    private static CreateAnnotationResponse createAnnotationResponseReject(
            String msg,
            RejectionDetails.Reason reason
    ) {
        final RejectionDetails rejectionDetails = RejectionDetails.newBuilder()
                .setReason(reason)
                .setMessage(msg)
                .build();

        final CreateAnnotationResponse response = CreateAnnotationResponse.newBuilder()
                .setResponseType(ResponseType.REJECT_RESPONSE)
                .setResponseTime(GrpcUtility.getTimestampNow())
                .setRejectionDetails(rejectionDetails)
                .build();

        return response;
    }

    private static void sendCreateAnnotationResponseReject(
            String errorMsg,
            RejectionDetails.Reason rejectionReason,
            StreamObserver<CreateAnnotationResponse> responseObserver
    ) {
        final CreateAnnotationResponse response = createAnnotationResponseReject(errorMsg, rejectionReason);
        responseObserver.onNext(response);
        responseObserver.onCompleted();
    }

    @Override
    public void createCommentAnnotation(
            CreateAnnotationRequest request,
            StreamObserver<CreateAnnotationResponse> responseObserver
    ) {
        logger.debug("id: {} createCommentAnnotation request received", responseObserver.hashCode());

        // validate request
        ValidationResult validationResult = AnnotationValidationUtility.validateCreateCommentRequest(request);
        if (validationResult.isError) {
            logger.debug("id: {} createCommentAnnotation validation failed: {}",
                    responseObserver.hashCode(),
                    validationResult.msg);
            sendCreateAnnotationResponseReject(
                    validationResult.msg, RejectionDetails.Reason.INVALID_REQUEST_REASON, responseObserver);
            return;
        }

        // handle request
        handler.handleCreateCommentAnnotationRequest(request, responseObserver);
    }
}
