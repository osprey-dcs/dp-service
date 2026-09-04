package com.ospreydcs.dp.service.annotation.handler.mongo.job;

import com.ospreydcs.dp.grpc.v1.annotation.DeleteAnnotationRequest;
import com.ospreydcs.dp.grpc.v1.annotation.DeleteAnnotationResponse;
import com.ospreydcs.dp.service.annotation.handler.mongo.client.MongoAnnotationClientInterface;
import com.ospreydcs.dp.service.annotation.handler.mongo.dispatch.DeleteAnnotationDispatcher;
import com.ospreydcs.dp.service.common.handler.HandlerJob;
import com.ospreydcs.dp.service.common.model.MongoDeleteResult;
import com.ospreydcs.dp.service.common.model.ResultStatus;
import io.grpc.stub.StreamObserver;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.bson.types.ObjectId;

public class DeleteAnnotationJob extends HandlerJob {

    private static final Logger logger = LogManager.getLogger();

    private final DeleteAnnotationRequest request;
    private final StreamObserver<DeleteAnnotationResponse> responseObserver;
    private final MongoAnnotationClientInterface mongoClient;
    private final DeleteAnnotationDispatcher dispatcher;

    public DeleteAnnotationJob(
            DeleteAnnotationRequest request,
            StreamObserver<DeleteAnnotationResponse> responseObserver,
            MongoAnnotationClientInterface mongoClient
    ) {
        this.request = request;
        this.responseObserver = responseObserver;
        this.mongoClient = mongoClient;
        this.dispatcher = new DeleteAnnotationDispatcher(responseObserver, request);
    }

    @Override
    public void execute() {
        logger.debug("executing DeleteAnnotationJob id: {}", responseObserver.hashCode());

        if (request.getAnnotationId().isBlank()) {
            dispatcher.handleValidationError(
                    new ResultStatus(true, "DeleteAnnotationRequest.annotationId must be specified"));
            return;
        }

        // A malformed id is a client mistake, rejected here (#248 plan D11). Unvalidated, it would
        // throw IllegalArgumentException from the ObjectId constructor inside the worker thread,
        // where QueueHandlerBase swallows it and the caller's stream hangs with no response.
        if (!ObjectId.isValid(request.getAnnotationId())) {
            dispatcher.handleValidationError(
                    new ResultStatus(true,
                            "DeleteAnnotationRequest.annotationId is not a valid id: " + request.getAnnotationId()));
            return;
        }

        final MongoDeleteResult result = mongoClient.deleteAnnotation(request.getAnnotationId());
        dispatcher.handleResult(result);
    }
}
