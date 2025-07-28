package com.ospreydcs.dp.service.annotation.handler.mongo.job;

import com.ospreydcs.dp.grpc.v1.annotation.CreateDataSetRequest;
import com.ospreydcs.dp.grpc.v1.annotation.CreateDataSetResponse;
import com.ospreydcs.dp.service.annotation.handler.mongo.MongoAnnotationHandler;
import com.ospreydcs.dp.service.annotation.handler.mongo.client.MongoAnnotationClientInterface;
import com.ospreydcs.dp.service.annotation.handler.mongo.dispatch.CreateDataSetDispatcher;
import com.ospreydcs.dp.service.common.bson.dataset.DataSetDocument;
import com.ospreydcs.dp.service.common.handler.HandlerJob;
import com.ospreydcs.dp.service.common.model.MongoInsertOneResult;
import com.ospreydcs.dp.service.common.model.ResultStatus;
import io.grpc.stub.StreamObserver;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class CreateDataSetJob extends HandlerJob {
    
    // static variables
    private static final Logger logger = LogManager.getLogger();

    // instance variables
    protected final CreateDataSetRequest request;
    protected final StreamObserver<CreateDataSetResponse> responseObserver;
    protected final MongoAnnotationClientInterface mongoClient;
    protected final MongoAnnotationHandler handler;
    protected CreateDataSetDispatcher dispatcher;

    public CreateDataSetJob(
            CreateDataSetRequest request,
            StreamObserver<CreateDataSetResponse> responseObserver,
            MongoAnnotationClientInterface mongoClient,
            MongoAnnotationHandler handler
    ) {
        this.request = request;
        this.responseObserver = responseObserver;
        this.mongoClient = mongoClient;
        this.handler = handler;
        this.dispatcher = new CreateDataSetDispatcher(responseObserver, request);
    }

    protected DataSetDocument generateDataSetDocument(CreateDataSetRequest request) {
        DataSetDocument document = DataSetDocument.fromCreateRequest(request);
        return document;
    }

    @Override
    public void execute() {
        logger.debug("executing CreateDataSetJob id: {}", this.responseObserver.hashCode());
        final ResultStatus resultStatus = this.handler.validateCreateDataSetRequest(request);
        if (resultStatus.isError) {
            dispatcher.handleValidationError(resultStatus);
            return;
        }
        final DataSetDocument dataSetDocument = generateDataSetDocument(request);
        final MongoInsertOneResult result = this.mongoClient.insertDataSet(dataSetDocument);
        logger.debug("dispatching CreateDataSetJob id: {}", this.responseObserver.hashCode());
        dispatcher.handleResult(result);
    }
}
