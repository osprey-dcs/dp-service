package com.ospreydcs.dp.service.annotation.handler.mongo.job;

import com.ospreydcs.dp.grpc.v1.annotation.SaveDataSetRequest;
import com.ospreydcs.dp.grpc.v1.annotation.SaveDataSetResponse;
import com.ospreydcs.dp.service.annotation.handler.mongo.MongoAnnotationHandler;
import com.ospreydcs.dp.service.annotation.handler.mongo.client.MongoAnnotationClientInterface;
import com.ospreydcs.dp.service.annotation.handler.mongo.dispatch.SaveDataSetDispatcher;
import com.ospreydcs.dp.service.common.bson.dataset.DataSetDocument;
import com.ospreydcs.dp.service.common.handler.HandlerJob;
import com.ospreydcs.dp.service.common.model.MongoSaveResult;
import com.ospreydcs.dp.service.common.model.ResultStatus;
import io.grpc.stub.StreamObserver;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class SaveDataSetJob extends HandlerJob {
    
    // static variables
    private static final Logger logger = LogManager.getLogger();

    // instance variables
    protected final SaveDataSetRequest request;
    protected final StreamObserver<SaveDataSetResponse> responseObserver;
    protected final MongoAnnotationClientInterface mongoClient;
    protected final MongoAnnotationHandler handler;
    protected SaveDataSetDispatcher dispatcher;

    public SaveDataSetJob(
            SaveDataSetRequest request,
            StreamObserver<SaveDataSetResponse> responseObserver,
            MongoAnnotationClientInterface mongoClient,
            MongoAnnotationHandler handler
    ) {
        this.request = request;
        this.responseObserver = responseObserver;
        this.mongoClient = mongoClient;
        this.handler = handler;
        this.dispatcher = new SaveDataSetDispatcher(responseObserver, request);
    }

    protected DataSetDocument generateDataSetDocument(SaveDataSetRequest request) {
        DataSetDocument document = DataSetDocument.fromSaveRequest(request);
        return document;
    }

    @Override
    public void execute() {
        logger.debug("executing SaveDataSetJob id: {}", this.responseObserver.hashCode());
        final ResultStatus resultStatus = this.handler.validateSaveDataSetRequest(request);
        if (resultStatus.isError) {
            dispatcher.handleValidationError(resultStatus);
            return;
        }
        final DataSetDocument dataSetDocument = generateDataSetDocument(request);
        final String existingDocumentId = request.getDataSet().getId();
        final MongoSaveResult result = this.mongoClient.saveDataSet(dataSetDocument, existingDocumentId);
        logger.debug("dispatching SaveDataSetJob id: {}", this.responseObserver.hashCode());
        dispatcher.handleResult(result);
    }
}