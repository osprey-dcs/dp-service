package com.ospreydcs.dp.service.annotation.handler.mongo.job;

import com.ospreydcs.dp.grpc.v1.annotation.DeletePvMetadataRequest;
import com.ospreydcs.dp.grpc.v1.annotation.DeletePvMetadataResponse;
import com.ospreydcs.dp.service.annotation.handler.mongo.client.MongoAnnotationClientInterface;
import com.ospreydcs.dp.service.annotation.handler.mongo.dispatch.DeletePvMetadataDispatcher;
import com.ospreydcs.dp.service.common.handler.HandlerJob;
import com.ospreydcs.dp.service.common.model.MongoDeleteResult;
import com.ospreydcs.dp.service.common.model.ResultStatus;
import io.grpc.stub.StreamObserver;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class DeletePvMetadataJob extends HandlerJob {

    private static final Logger logger = LogManager.getLogger();

    private final DeletePvMetadataRequest request;
    private final StreamObserver<DeletePvMetadataResponse> responseObserver;
    private final MongoAnnotationClientInterface mongoClient;
    private final DeletePvMetadataDispatcher dispatcher;

    public DeletePvMetadataJob(
            DeletePvMetadataRequest request,
            StreamObserver<DeletePvMetadataResponse> responseObserver,
            MongoAnnotationClientInterface mongoClient
    ) {
        this.request = request;
        this.responseObserver = responseObserver;
        this.mongoClient = mongoClient;
        this.dispatcher = new DeletePvMetadataDispatcher(responseObserver, request);
    }

    @Override
    public void execute() {
        logger.debug("executing DeletePvMetadataJob id: {}", responseObserver.hashCode());

        if (request.getPvNameOrAlias().isBlank()) {
            dispatcher.handleValidationError(
                    new ResultStatus(true, "DeletePvMetadataRequest.pvNameOrAlias must be specified"));
            return;
        }

        final MongoDeleteResult result = mongoClient.deletePvMetadata(request.getPvNameOrAlias());
        dispatcher.handleResult(result);
    }
}
