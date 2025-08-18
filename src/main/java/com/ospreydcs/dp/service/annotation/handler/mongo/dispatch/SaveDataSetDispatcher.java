package com.ospreydcs.dp.service.annotation.handler.mongo.dispatch;

import com.mongodb.client.result.InsertOneResult;
import com.ospreydcs.dp.grpc.v1.annotation.SaveDataSetRequest;
import com.ospreydcs.dp.grpc.v1.annotation.SaveDataSetResponse;
import com.ospreydcs.dp.service.annotation.service.AnnotationServiceImpl;
import com.ospreydcs.dp.service.common.handler.Dispatcher;
import com.ospreydcs.dp.service.common.model.MongoInsertOneResult;
import com.ospreydcs.dp.service.common.model.ResultStatus;
import io.grpc.stub.StreamObserver;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class SaveDataSetDispatcher extends Dispatcher {

    // static variables
    private static final Logger logger = LogManager.getLogger();

    // instance variables
    private final SaveDataSetRequest request;
    private final StreamObserver<SaveDataSetResponse> responseObserver;

    public SaveDataSetDispatcher(
            StreamObserver<SaveDataSetResponse> responseObserver,
            SaveDataSetRequest request
    ) {
        this.responseObserver = responseObserver;
        this.request = request;
    }

    public void handleValidationError(ResultStatus resultStatus) {
        AnnotationServiceImpl.sendSaveDataSetResponseReject(
                resultStatus.msg,
                this.responseObserver);
    }

    public void handleResult(MongoInsertOneResult result) {

        // Check to see if error flag is set in our result wrapper, this indicates that insertOne threw an exception.
        if (result.isError) {
            // send error response and close response stream
            final String errorMsg = "exception inserting DataSetDocument: " + result.message;
            AnnotationServiceImpl.sendSaveDataSetResponseError(errorMsg, responseObserver);
            return;
        }

        // Otherwise check to see if the wrapped InsertOneResult indicates an error
        final InsertOneResult insertOneResult = result.insertOneResult;
        if (!insertOneResult.wasAcknowledged()) {
            final String errorMsg = "DataSetDocument insert failed (insertOne() not acknowledged";
            AnnotationServiceImpl.sendSaveDataSetResponseError(errorMsg, responseObserver);
            return;
        }

        // check if result contains id inserted
        if (insertOneResult.getInsertedId() == null) {
            final String errorMsg = "DataSetDocument insert failed to return document id";
            AnnotationServiceImpl.sendSaveDataSetResponseError(errorMsg, responseObserver);
            return;
        }

        // insert was successful, return a response with the id
        AnnotationServiceImpl.sendSaveDataSetResponseSuccess(
                insertOneResult.getInsertedId().asObjectId().getValue().toString(), responseObserver);
    }

}