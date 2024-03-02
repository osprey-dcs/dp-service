package com.ospreydcs.dp.service.annotation.handler.mongo.job;

import com.mongodb.client.result.InsertOneResult;
import com.ospreydcs.dp.grpc.v1.annotation.Annotation;
import com.ospreydcs.dp.grpc.v1.annotation.CreateAnnotationRequest;
import com.ospreydcs.dp.grpc.v1.annotation.CreateAnnotationResponse;
import com.ospreydcs.dp.service.annotation.handler.mongo.client.MongoAnnotationClientInterface;
import com.ospreydcs.dp.service.annotation.handler.mongo.dispatch.CreateAnnotationDispatcher;
import com.ospreydcs.dp.service.common.bson.annotation.AnnotationDocument;
import com.ospreydcs.dp.service.common.handler.HandlerJob;
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
    protected CreateAnnotationDispatcher dispatcher;

    public CreateAnnotationJob(
            CreateAnnotationRequest request,
            StreamObserver<CreateAnnotationResponse> responseObserver,
            MongoAnnotationClientInterface mongoClient
    ) {
        this.request = request;
        this.responseObserver = responseObserver;
        this.mongoClient = mongoClient;
    }

    protected abstract AnnotationDocument generateAnnotationDocument_(CreateAnnotationRequest request);

    @Override
    public void execute() {
        logger.debug("executing CreateAnnotationJob id: {}", this.responseObserver.hashCode());
        AnnotationDocument annotationDocument = generateAnnotationDocument_(request);
        InsertOneResult result = this.mongoClient.insertAnnotation(annotationDocument);
//        logger.debug("dispatching CreateAnnotationJob id: {}", this.responseObserver.hashCode());
//        dispatcher.handleResult();
    }
}
