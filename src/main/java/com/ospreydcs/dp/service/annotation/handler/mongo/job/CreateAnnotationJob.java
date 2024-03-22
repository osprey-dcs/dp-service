package com.ospreydcs.dp.service.annotation.handler.mongo.job;

import com.ospreydcs.dp.grpc.v1.annotation.CreateAnnotationRequest;
import com.ospreydcs.dp.grpc.v1.annotation.CreateAnnotationResponse;
import com.ospreydcs.dp.service.annotation.handler.mongo.MongoAnnotationHandler;
import com.ospreydcs.dp.service.annotation.handler.mongo.client.MongoAnnotationClientInterface;
import com.ospreydcs.dp.service.annotation.handler.mongo.dispatch.CreateAnnotationDispatcher;
import com.ospreydcs.dp.service.common.bson.annotation.AnnotationDocument;
import com.ospreydcs.dp.service.common.handler.HandlerJob;
import com.ospreydcs.dp.service.common.model.MongoInsertOneResult;
import com.ospreydcs.dp.service.common.model.ValidationResult;
import io.grpc.stub.StreamObserver;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public abstract class CreateAnnotationJob extends HandlerJob {

    // static variables
    private static final Logger logger = LogManager.getLogger();

    // instance variables
    protected final CreateAnnotationRequest request;
    protected final StreamObserver<CreateAnnotationResponse> responseObserver;
    protected final MongoAnnotationClientInterface mongoClient;
    protected final MongoAnnotationHandler handler;
    protected CreateAnnotationDispatcher dispatcher;

    public CreateAnnotationJob(
            CreateAnnotationRequest request,
            StreamObserver<CreateAnnotationResponse> responseObserver,
            MongoAnnotationClientInterface mongoClient,
            MongoAnnotationHandler handler
    ) {
        this.request = request;
        this.responseObserver = responseObserver;
        this.mongoClient = mongoClient;
        this.handler = handler;
        this.dispatcher = new CreateAnnotationDispatcher(responseObserver, request);
    }

    protected abstract AnnotationDocument generateAnnotationDocument_(CreateAnnotationRequest request);

    @Override
    public void execute() {
        logger.debug("executing CreateAnnotationJob id: {}", this.responseObserver.hashCode());
        final ValidationResult validationResult = this.handler.validateAnnotationRequest(request);
        if (validationResult.isError) {
            dispatcher.handleValidationError(validationResult);
            return;
        }
        final AnnotationDocument annotationDocument = generateAnnotationDocument_(request);
        final MongoInsertOneResult result = this.mongoClient.insertAnnotation(annotationDocument);
        logger.debug("dispatching CreateAnnotationJob id: {}", this.responseObserver.hashCode());
        dispatcher.handleResult(result);
    }
}
