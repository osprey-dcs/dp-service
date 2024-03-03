package com.ospreydcs.dp.service.annotation.service;

import com.google.protobuf.Message;
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
import org.bson.BsonValue;

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
            CreateAnnotationResponse.ExceptionalResult.StatusType statusType
    ) {
        final CreateAnnotationResponse.ExceptionalResult exceptionalResult =
                CreateAnnotationResponse.ExceptionalResult.newBuilder()
                        .setStatusType(statusType)
                        .setStatusMessage(msg)
                        .build();

        final CreateAnnotationResponse response = CreateAnnotationResponse.newBuilder()
                .setResponseTime(GrpcUtility.getTimestampNow())
                .setExceptionalResult(exceptionalResult)
                .build();

        return response;
    }

    private static void sendCreateAnnotationResponseReject(
            String errorMsg,
            CreateAnnotationResponse.ExceptionalResult.StatusType statusType,
            StreamObserver<CreateAnnotationResponse> responseObserver
    ) {
        final CreateAnnotationResponse response = createAnnotationResponseReject(errorMsg, statusType);
        responseObserver.onNext(response);
        responseObserver.onCompleted();
    }

    private static CreateAnnotationResponse createAnnotationResponseError(String msg) {

        final CreateAnnotationResponse.ExceptionalResult exceptionalResult =
                CreateAnnotationResponse.ExceptionalResult.newBuilder()
                        .setStatusType(CreateAnnotationResponse.ExceptionalResult.StatusType.STATUS_ERROR)
                        .setStatusMessage(msg)
                        .build();

        final CreateAnnotationResponse response = CreateAnnotationResponse.newBuilder()
                .setResponseTime(GrpcUtility.getTimestampNow())
                .setExceptionalResult(exceptionalResult)
                .build();

        return response;
    }

    public static void sendCreateAnnotationResponseError(
            String errorMsg, StreamObserver<CreateAnnotationResponse> responseObserver
    ) {
        final CreateAnnotationResponse response = createAnnotationResponseError(errorMsg);
        responseObserver.onNext(response);
        responseObserver.onCompleted();
    }

    private static CreateAnnotationResponse createAnnotationResponseSuccess(String annotationId) {

        final CreateAnnotationResponse.SuccessfulResult successfulResult =
                CreateAnnotationResponse.SuccessfulResult.newBuilder()
                        .setAnnotationId(annotationId)
                        .build();

        final CreateAnnotationResponse response = CreateAnnotationResponse.newBuilder()
                .setResponseTime(GrpcUtility.getTimestampNow())
                .setSuccessfulResult(successfulResult)
                .build();

        return response;
    }

    public static void sendCreateAnnotationResponseSuccess(
            String annotationId, StreamObserver<CreateAnnotationResponse> responseObserver) {
        final CreateAnnotationResponse response = createAnnotationResponseSuccess(annotationId);
        responseObserver.onNext(response);
        responseObserver.onCompleted();
    }

    private void createCommentAnnotation(
            CreateAnnotationRequest request,
            StreamObserver<CreateAnnotationResponse> responseObserver
    ) {
        // validate request
        ValidationResult validationResult = AnnotationValidationUtility.validateCreateCommentRequest(request);
        if (validationResult.isError) {
            logger.debug("id: {} createCommentAnnotation validation failed: {}",
                    responseObserver.hashCode(),
                    validationResult.msg);
            sendCreateAnnotationResponseReject(
                    validationResult.msg,
                    CreateAnnotationResponse.ExceptionalResult.StatusType.STATUS_REJECT,
                    responseObserver);
            return;
        }

        // handle request
        handler.handleCreateCommentAnnotationRequest(request, responseObserver);
    }

    @Override
    public void createAnnotation(
            CreateAnnotationRequest request,
            StreamObserver<CreateAnnotationResponse> responseObserver
    ) {
        logger.info("id: {} createAnnotation request received with type: {}",
                responseObserver.hashCode(), request.getAnnotationTypeDetailsCase().toString());

        // dispatch request based on annotation type
        switch(request.getAnnotationTypeDetailsCase()) {

            case COMMENTDETAILS -> {
                createCommentAnnotation(request, responseObserver);
            }
            case ANNOTATIONTYPEDETAILS_NOT_SET -> {
                final String errorMsg = "id: " + responseObserver.hashCode()
                        + " createAnnotation annotationTypeDetails not specified";
                logger.debug(errorMsg);
                sendCreateAnnotationResponseReject(
                        errorMsg,
                        CreateAnnotationResponse.ExceptionalResult.StatusType.STATUS_REJECT,
                        responseObserver);
                return;
            }
        }
    }
}
