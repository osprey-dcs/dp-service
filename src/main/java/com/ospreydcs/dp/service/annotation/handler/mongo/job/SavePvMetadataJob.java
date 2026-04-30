package com.ospreydcs.dp.service.annotation.handler.mongo.job;

import com.ospreydcs.dp.grpc.v1.annotation.SavePvMetadataRequest;
import com.ospreydcs.dp.grpc.v1.annotation.SavePvMetadataResponse;
import com.ospreydcs.dp.service.annotation.handler.mongo.client.MongoAnnotationClientInterface;
import com.ospreydcs.dp.service.annotation.handler.mongo.dispatch.SavePvMetadataDispatcher;
import com.ospreydcs.dp.service.common.bson.pvmetadata.PvMetadataDocument;
import com.ospreydcs.dp.service.common.handler.HandlerJob;
import com.ospreydcs.dp.service.common.model.MongoSaveResult;
import com.ospreydcs.dp.service.common.model.ResultStatus;
import io.grpc.stub.StreamObserver;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class SavePvMetadataJob extends HandlerJob {

    private static final Logger logger = LogManager.getLogger();

    private final SavePvMetadataRequest request;
    private final StreamObserver<SavePvMetadataResponse> responseObserver;
    private final MongoAnnotationClientInterface mongoClient;
    private final SavePvMetadataDispatcher dispatcher;

    public SavePvMetadataJob(
            SavePvMetadataRequest request,
            StreamObserver<SavePvMetadataResponse> responseObserver,
            MongoAnnotationClientInterface mongoClient
    ) {
        this.request = request;
        this.responseObserver = responseObserver;
        this.mongoClient = mongoClient;
        this.dispatcher = new SavePvMetadataDispatcher(responseObserver, request);
    }

    @Override
    public void execute() {
        logger.debug("executing SavePvMetadataJob id: {}", responseObserver.hashCode());

        // validate pvName not blank
        if (request.getPvName().isBlank()) {
            dispatcher.handleValidationError(
                    new ResultStatus(true, "SavePvMetadataRequest.pvName must be specified"));
            return;
        }

        // check for duplicate attribute keys
        final java.util.Set<String> attributeKeys = new java.util.HashSet<>();
        for (var attr : request.getAttributesList()) {
            if (!attributeKeys.add(attr.getName())) {
                dispatcher.handleValidationError(new ResultStatus(
                        true, "SavePvMetadataRequest.attributes contains duplicate key: " + attr.getName()));
                return;
            }
        }

        // check for alias conflicts with other pvName records
        for (String alias : request.getAliasesList()) {
            final PvMetadataDocument conflictDoc = mongoClient.findPvMetadataByNameOrAlias(alias);
            if (conflictDoc != null && !conflictDoc.getPvName().equals(request.getPvName())) {
                dispatcher.handleValidationError(new ResultStatus(
                        true, "alias '" + alias + "' is already used by pvName: " + conflictDoc.getPvName()));
                return;
            }
        }

        // build document and upsert
        final PvMetadataDocument document = PvMetadataDocument.fromSavePvMetadataRequest(request);
        final MongoSaveResult result = mongoClient.savePvMetadata(document);
        dispatcher.handleResult(result);
    }
}
