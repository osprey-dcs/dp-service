package com.ospreydcs.dp.service.annotation.handler.mongo.dispatch;

import com.ospreydcs.dp.grpc.v1.annotation.Calculations;
import com.ospreydcs.dp.grpc.v1.annotation.GetCalculationsRequest;
import com.ospreydcs.dp.grpc.v1.annotation.GetCalculationsResponse;
import com.ospreydcs.dp.service.annotation.service.AnnotationServiceImpl;
import com.ospreydcs.dp.service.common.bson.calculations.CalculationsDocument;
import com.ospreydcs.dp.service.common.exception.DpException;
import com.ospreydcs.dp.service.common.handler.Dispatcher;
import com.ospreydcs.dp.service.common.model.ResultStatus;
import io.grpc.stub.StreamObserver;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class GetCalculationsDispatcher extends Dispatcher {

    private static final Logger logger = LogManager.getLogger();

    private final StreamObserver<GetCalculationsResponse> responseObserver;
    private final GetCalculationsRequest request;

    public GetCalculationsDispatcher(
            StreamObserver<GetCalculationsResponse> responseObserver,
            GetCalculationsRequest request
    ) {
        this.responseObserver = responseObserver;
        this.request = request;
    }

    public void handleValidationError(ResultStatus resultStatus) {
        AnnotationServiceImpl.sendGetCalculationsResponseReject(resultStatus.msg, responseObserver);
    }

    public void handleError(String errorMsg) {
        AnnotationServiceImpl.sendGetCalculationsResponseError(errorMsg, responseObserver);
    }

    public void handleResult(CalculationsDocument document) {
        if (document == null) {
            final String msg = "no Calculations record found for id: " + request.getCalculationsId();
            AnnotationServiceImpl.sendGetCalculationsResponseReject(msg, responseObserver);
            return;
        }

        // Contain conversion failures: a malformed stored document must produce a reportable error,
        // never an unchecked throw that terminates the response stream with no payload.
        // toCalculations() deserializes the embedded frames and throws DpException on corruption.
        final Calculations calculations;
        try {
            calculations = document.toCalculations();
        } catch (DpException | RuntimeException ex) {
            final String errorMsg = "error building Calculations from document id "
                    + request.getCalculationsId() + ": " + ex.getMessage();
            logger.error("handleResult conversion error: {}", ex.getMessage(), ex);
            AnnotationServiceImpl.sendGetCalculationsResponseError(errorMsg, responseObserver);
            return;
        }
        AnnotationServiceImpl.sendGetCalculationsResponseSuccess(calculations, responseObserver);
    }
}
