package com.ospreydcs.dp.service.annotation.handler.mongo.dispatch;

import com.mongodb.client.result.InsertOneResult;
import com.ospreydcs.dp.grpc.v1.annotation.SaveAnnotationRequest;
import com.ospreydcs.dp.grpc.v1.annotation.SaveAnnotationResponse;
import com.ospreydcs.dp.service.annotation.service.AnnotationServiceImpl;
import com.ospreydcs.dp.service.common.handler.Dispatcher;
import com.ospreydcs.dp.service.common.model.MongoSaveResult;
import com.ospreydcs.dp.service.common.model.ResultStatus;
import io.grpc.stub.StreamObserver;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class SaveAnnotationDispatcher extends Dispatcher {

    // static variables
    private static final Logger logger = LogManager.getLogger();

    // instance variables
    private final SaveAnnotationRequest request;
    private final StreamObserver<SaveAnnotationResponse> responseObserver;

    public SaveAnnotationDispatcher(
            StreamObserver<SaveAnnotationResponse> responseObserver,
            SaveAnnotationRequest request
    ) {
        this.responseObserver = responseObserver;
        this.request = request;
    }

    public void handleValidationError(ResultStatus resultStatus) {
        AnnotationServiceImpl.sendSaveAnnotationResponseReject(
                resultStatus.msg,
                this.responseObserver);
    }

    public void handleError(String errorMsg) {
        AnnotationServiceImpl.sendSaveAnnotationResponseError(
                errorMsg,
                this.responseObserver);
    }

    public void handleResult(MongoSaveResult result, String calculationsId) {

        // A rejection means the request violated a business rule (e.g. it referenced a document
        // that does not exist), as opposed to the service failing to handle it.
        if (result.isReject) {
            AnnotationServiceImpl.sendSaveAnnotationResponseReject(result.message, responseObserver);
            return;
        }

        // Check to see if error flag is set in our result wrapper, this indicates that insertOne failed.
        if (result.isError) {
            // send error response and close response stream
            final String errorMsg = result.message;
            AnnotationServiceImpl.sendSaveAnnotationResponseError(errorMsg, responseObserver);
            return;
        }

        // insert was successful, return a response with the annotation id, plus the id of the saved
        // calculations document when the request carried calculations
        AnnotationServiceImpl.sendSaveAnnotationResponseSuccess(result.documentId, calculationsId, responseObserver);
    }

}