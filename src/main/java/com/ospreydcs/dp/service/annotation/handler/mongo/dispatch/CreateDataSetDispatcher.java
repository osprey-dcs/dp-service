package com.ospreydcs.dp.service.annotation.handler.mongo.dispatch;

import com.mongodb.client.result.InsertOneResult;
import com.ospreydcs.dp.grpc.v1.annotation.CreateAnnotationRequest;
import com.ospreydcs.dp.grpc.v1.annotation.CreateAnnotationResponse;
import com.ospreydcs.dp.grpc.v1.annotation.CreateDataSetRequest;
import com.ospreydcs.dp.grpc.v1.annotation.CreateDataSetResponse;
import com.ospreydcs.dp.service.annotation.service.AnnotationServiceImpl;
import com.ospreydcs.dp.service.common.handler.Dispatcher;
import com.ospreydcs.dp.service.common.model.MongoInsertOneResult;
import com.ospreydcs.dp.service.common.model.ValidationResult;
import io.grpc.stub.StreamObserver;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class CreateDataSetDispatcher extends Dispatcher {

    // static variables
    private static final Logger logger = LogManager.getLogger();

    // instance variables
    private final CreateDataSetRequest request;
    private final StreamObserver<CreateDataSetResponse> responseObserver;

    public CreateDataSetDispatcher(
            StreamObserver<CreateDataSetResponse> responseObserver,
            CreateDataSetRequest request
    ) {
        this.responseObserver = responseObserver;
        this.request = request;
    }

    public void handleValidationError(ValidationResult validationResult) {
        AnnotationServiceImpl.sendCreateDataSetResponseReject(
                validationResult.msg,
                this.responseObserver);
    }

    public void handleResult(MongoInsertOneResult result) {

        // Check to see if error flag is set in our result wrapper, this indicates that insertOne threw an exception.
        if (result.isError) {
            // send error response and close response stream
            final String errorMsg = "exception inserting DataSetDocument: " + result.message;
            AnnotationServiceImpl.sendCreateDataSetResponseError(errorMsg, responseObserver);
            return;
        }

        // Otherwise check to see if the wrapped InsertOneResult indicates an error
        final InsertOneResult insertOneResult = result.insertOneResult;
        if (!insertOneResult.wasAcknowledged()) {
            final String errorMsg = "DataSetDocument insert failed (insertOne() not acknowledged";
            AnnotationServiceImpl.sendCreateDataSetResponseError(errorMsg, responseObserver);
            return;
        }

        // check if result contains id inserted
        if (insertOneResult.getInsertedId() == null) {
            final String errorMsg = "DataSetDocument insert failed to return document id";
            AnnotationServiceImpl.sendCreateDataSetResponseError(errorMsg, responseObserver);
            return;
        }

        // insert was successful, return a response with the id
        AnnotationServiceImpl.sendCreateDataSetResponseSuccess(
                insertOneResult.getInsertedId().asObjectId().getValue().toString(), responseObserver);
    }

}
