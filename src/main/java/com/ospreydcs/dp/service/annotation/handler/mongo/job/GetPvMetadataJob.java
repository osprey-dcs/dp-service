package com.ospreydcs.dp.service.annotation.handler.mongo.job;

import com.ospreydcs.dp.grpc.v1.annotation.GetPvMetadataRequest;
import com.ospreydcs.dp.grpc.v1.annotation.GetPvMetadataResponse;
import com.ospreydcs.dp.service.annotation.handler.mongo.client.MongoAnnotationClientInterface;
import com.ospreydcs.dp.service.annotation.handler.mongo.dispatch.GetPvMetadataDispatcher;
import com.ospreydcs.dp.service.common.bson.pvmetadata.PvMetadataDocument;
import com.ospreydcs.dp.service.common.handler.HandlerJob;
import com.ospreydcs.dp.service.common.model.ResultStatus;
import io.grpc.stub.StreamObserver;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class GetPvMetadataJob extends HandlerJob {

    private static final Logger logger = LogManager.getLogger();

    private final GetPvMetadataRequest request;
    private final StreamObserver<GetPvMetadataResponse> responseObserver;
    private final MongoAnnotationClientInterface mongoClient;
    private final GetPvMetadataDispatcher dispatcher;

    public GetPvMetadataJob(
            GetPvMetadataRequest request,
            StreamObserver<GetPvMetadataResponse> responseObserver,
            MongoAnnotationClientInterface mongoClient
    ) {
        this.request = request;
        this.responseObserver = responseObserver;
        this.mongoClient = mongoClient;
        this.dispatcher = new GetPvMetadataDispatcher(responseObserver, request);
    }

    @Override
    public void execute() {
        logger.debug("executing GetPvMetadataJob id: {}", responseObserver.hashCode());

        if (request.getPvNameOrAlias().isBlank()) {
            dispatcher.handleValidationError(
                    new ResultStatus(true, "GetPvMetadataRequest.pvNameOrAlias must be specified"));
            return;
        }

        final PvMetadataDocument document;
        try {
            document = mongoClient.findPvMetadataByNameOrAlias(request.getPvNameOrAlias());
        } catch (Exception ex) {
            dispatcher.handleError("error looking up pvMetadata: " + ex.getMessage());
            return;
        }
        dispatcher.handleResult(document);
    }
}
