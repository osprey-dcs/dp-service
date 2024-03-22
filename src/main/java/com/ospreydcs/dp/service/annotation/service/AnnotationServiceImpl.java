package com.ospreydcs.dp.service.annotation.service;

import com.ospreydcs.dp.grpc.v1.annotation.*;
import com.ospreydcs.dp.grpc.v1.common.ExceptionalResult;
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

    private static CreateAnnotationResponse createAnnotationResponseReject(String msg) {
        final ExceptionalResult exceptionalResult =
                ExceptionalResult.newBuilder()
                        .setExceptionalResultStatus(
                                ExceptionalResult.ExceptionalResultStatus.RESULT_STATUS_REJECT)
                        .setMessage(msg)
                        .build();

        final CreateAnnotationResponse response = CreateAnnotationResponse.newBuilder()
                .setResponseTime(GrpcUtility.getTimestampNow())
                .setExceptionalResult(exceptionalResult)
                .build();

        return response;
    }

    public static void sendCreateAnnotationResponseReject(
            String errorMsg,
            StreamObserver<CreateAnnotationResponse> responseObserver
    ) {
        final CreateAnnotationResponse response = createAnnotationResponseReject(errorMsg);
        responseObserver.onNext(response);
        responseObserver.onCompleted();
    }

    private static CreateAnnotationResponse createAnnotationResponseError(String msg) {

        final ExceptionalResult exceptionalResult =
                ExceptionalResult.newBuilder()
                        .setExceptionalResultStatus(ExceptionalResult.ExceptionalResultStatus.RESULT_STATUS_ERROR)
                        .setMessage(msg)
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

        final CreateAnnotationResponse.CreateAnnotationResult result =
                CreateAnnotationResponse.CreateAnnotationResult.newBuilder()
                        .setAnnotationId(annotationId)
                        .build();

        final CreateAnnotationResponse response = CreateAnnotationResponse.newBuilder()
                .setResponseTime(GrpcUtility.getTimestampNow())
                .setCreateAnnotationResult(result)
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
                responseObserver.hashCode(), request.getAnnotationCase().toString());

        // dispatch request based on annotation type
        switch(request.getAnnotationCase()) {

            case COMMENTANNOTATION -> {
                createCommentAnnotation(request, responseObserver);
            }
            case ANNOTATION_NOT_SET -> {
                final String errorMsg = "id: " + responseObserver.hashCode()
                        + " createAnnotation annotationTypeDetails not specified";
                logger.debug(errorMsg);
                sendCreateAnnotationResponseReject(
                        errorMsg,
                        responseObserver);
                return;
            }
        }
    }

    private static CreateDataSetResponse createDataSetResponseReject(String msg) {

        final ExceptionalResult exceptionalResult =
                ExceptionalResult.newBuilder()
                        .setExceptionalResultStatus(
                                ExceptionalResult.ExceptionalResultStatus.RESULT_STATUS_REJECT)
                        .setMessage(msg)
                        .build();

        final CreateDataSetResponse response = CreateDataSetResponse.newBuilder()
                .setResponseTime(GrpcUtility.getTimestampNow())
                .setExceptionalResult(exceptionalResult)
                .build();

        return response;
    }

    public static void sendCreateDataSetResponseReject(
            String errorMsg,
            StreamObserver<CreateDataSetResponse> responseObserver
    ) {
        final CreateDataSetResponse response = createDataSetResponseReject(errorMsg);
        responseObserver.onNext(response);
        responseObserver.onCompleted();
    }

    private static CreateDataSetResponse createDataSetResponseError(String msg) {

        final ExceptionalResult exceptionalResult =
                ExceptionalResult.newBuilder()
                        .setExceptionalResultStatus(ExceptionalResult.ExceptionalResultStatus.RESULT_STATUS_ERROR)
                        .setMessage(msg)
                        .build();

        final CreateDataSetResponse response = CreateDataSetResponse.newBuilder()
                .setResponseTime(GrpcUtility.getTimestampNow())
                .setExceptionalResult(exceptionalResult)
                .build();

        return response;
    }

    public static void sendCreateDataSetResponseError(
            String errorMsg, StreamObserver<CreateDataSetResponse> responseObserver
    ) {
        final CreateDataSetResponse response = createDataSetResponseError(errorMsg);
        responseObserver.onNext(response);
        responseObserver.onCompleted();
    }

    private static CreateDataSetResponse createDataSetResponseSuccess(String dataSetId) {

        final CreateDataSetResponse.CreateDataSetResult result =
                CreateDataSetResponse.CreateDataSetResult.newBuilder()
                        .setDataSetId(dataSetId)
                        .build();

        final CreateDataSetResponse response = CreateDataSetResponse.newBuilder()
                .setResponseTime(GrpcUtility.getTimestampNow())
                .setCreateDataSetResult(result)
                .build();

        return response;
    }

    public static void sendCreateDataSetResponseSuccess(
            String dataSetId, StreamObserver<CreateDataSetResponse> responseObserver
    ) {
        final CreateDataSetResponse response = createDataSetResponseSuccess(dataSetId);
        responseObserver.onNext(response);
        responseObserver.onCompleted();
    }

    @Override
    public void createDataSet(
            CreateDataSetRequest request,
            StreamObserver<CreateDataSetResponse> responseObserver
    ) {
        logger.info("id: {} createDataSet request received", responseObserver.hashCode());
        
        final DataSet dataSet = request.getDataSet();
        if (dataSet == null) {
            final String errorMsg = "CreateDataSetRequest.dataSet must be specified";
            sendCreateDataSetResponseReject(errorMsg, responseObserver);
        }

        // validate DataSet
        ValidationResult validationResult = AnnotationValidationUtility.validateDataSet(dataSet);
        if (validationResult.isError) {
            logger.debug("id: {} CreateDataSetRequest.dataSet validation failed: {}",
                    responseObserver.hashCode(),
                    validationResult.msg);
            sendCreateDataSetResponseReject(
                    validationResult.msg,
                    responseObserver);
            return;
        }

        // handle request
        handler.handleCreateDataSetRequest(request, responseObserver);
    }

}
