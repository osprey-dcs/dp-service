package com.ospreydcs.dp.service.annotation.handler.mongo.dispatch;

import com.ospreydcs.dp.grpc.v1.annotation.Annotation;
import com.ospreydcs.dp.grpc.v1.annotation.GetAnnotationRequest;
import com.ospreydcs.dp.grpc.v1.annotation.GetAnnotationResponse;
import com.ospreydcs.dp.service.annotation.service.AnnotationServiceImpl;
import com.ospreydcs.dp.service.common.bson.annotation.AnnotationDocument;
import com.ospreydcs.dp.service.common.bson.calculations.CalculationsDocument;
import com.ospreydcs.dp.service.common.exception.DpException;
import com.ospreydcs.dp.service.common.handler.Dispatcher;
import com.ospreydcs.dp.service.common.model.ResultStatus;
import io.grpc.stub.StreamObserver;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class GetAnnotationDispatcher extends Dispatcher {

    private static final Logger logger = LogManager.getLogger();

    private final StreamObserver<GetAnnotationResponse> responseObserver;
    private final GetAnnotationRequest request;

    public GetAnnotationDispatcher(
            StreamObserver<GetAnnotationResponse> responseObserver,
            GetAnnotationRequest request
    ) {
        this.responseObserver = responseObserver;
        this.request = request;
    }

    public void handleValidationError(ResultStatus resultStatus) {
        AnnotationServiceImpl.sendGetAnnotationResponseReject(resultStatus.msg, responseObserver);
    }

    public void handleError(String errorMsg) {
        AnnotationServiceImpl.sendGetAnnotationResponseError(errorMsg, responseObserver);
    }

    public void handleResult(AnnotationDocument annotationDocument, CalculationsDocument calculationsDocument) {
        if (annotationDocument == null) {
            final String msg = "no Annotation record found for id: " + request.getAnnotationId();
            AnnotationServiceImpl.sendGetAnnotationResponseReject(msg, responseObserver);
            return;
        }

        // Contain conversion failures: a malformed stored document must produce a reportable error,
        // never an unchecked throw that terminates the response stream with no payload.
        final Annotation annotation;
        try {
            Annotation converted = annotationDocument.toAnnotation();
            if (calculationsDocument != null) {
                converted = converted.toBuilder()
                        .setCalculations(calculationsDocument.toCalculations())
                        .build();
            }
            annotation = converted;
        } catch (DpException | RuntimeException ex) {
            final String errorMsg = "error building Annotation from document id "
                    + request.getAnnotationId() + ": " + ex.getMessage();
            logger.error("handleResult conversion error: {}", ex.getMessage(), ex);
            AnnotationServiceImpl.sendGetAnnotationResponseError(errorMsg, responseObserver);
            return;
        }
        AnnotationServiceImpl.sendGetAnnotationResponseSuccess(annotation, responseObserver);
    }
}
