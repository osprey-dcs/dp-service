package com.ospreydcs.dp.service.annotation.handler.mongo.dispatch;

import com.mongodb.client.result.InsertOneResult;
import com.ospreydcs.dp.grpc.v1.annotation.SaveDataSetRequest;
import com.ospreydcs.dp.grpc.v1.annotation.SaveDataSetResponse;
import com.ospreydcs.dp.service.annotation.service.AnnotationServiceImpl;
import com.ospreydcs.dp.service.common.handler.Dispatcher;
import com.ospreydcs.dp.service.common.model.MongoSaveResult;
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

    public void handleResult(MongoSaveResult result) {

        // Check to see if error flag is set in our result wrapper, this indicates that insertOne failed.
        if (result.isError) {
            // send error response and close response stream
            final String errorMsg = result.message;
            AnnotationServiceImpl.sendSaveDataSetResponseError(errorMsg, responseObserver);
            return;
        }

        // insert was successful, return a response with the id
        AnnotationServiceImpl.sendSaveDataSetResponseSuccess(result.documentId, responseObserver);
    }

}