package com.ospreydcs.dp.service.annotation.handler.mongo.job;

import com.mongodb.client.MongoCursor;
import com.ospreydcs.dp.grpc.v1.annotation.QueryPvMetadataRequest;
import com.ospreydcs.dp.grpc.v1.annotation.QueryPvMetadataResponse;
import com.ospreydcs.dp.service.annotation.handler.mongo.client.MongoAnnotationClientInterface;
import com.ospreydcs.dp.service.annotation.handler.mongo.dispatch.QueryPvMetadataDispatcher;
import com.ospreydcs.dp.service.common.bson.pvmetadata.PvMetadataDocument;
import com.ospreydcs.dp.service.common.handler.HandlerJob;
import com.ospreydcs.dp.service.common.model.ResultStatus;
import io.grpc.stub.StreamObserver;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class QueryPvMetadataJob extends HandlerJob {

    private static final Logger logger = LogManager.getLogger();

    private final QueryPvMetadataRequest request;
    private final StreamObserver<QueryPvMetadataResponse> responseObserver;
    private final MongoAnnotationClientInterface mongoClient;
    private final QueryPvMetadataDispatcher dispatcher;

    public QueryPvMetadataJob(
            QueryPvMetadataRequest request,
            StreamObserver<QueryPvMetadataResponse> responseObserver,
            MongoAnnotationClientInterface mongoClient
    ) {
        this.request = request;
        this.responseObserver = responseObserver;
        this.mongoClient = mongoClient;
        this.dispatcher = new QueryPvMetadataDispatcher(responseObserver, request);
    }

    @Override
    public void execute() {
        logger.debug("executing QueryPvMetadataJob id: {}", responseObserver.hashCode());

        // validate: criteria list must not be empty
        if (request.getCriteriaList().isEmpty()) {
            dispatcher.handleValidationError(
                    new ResultStatus(true, "QueryPvMetadataRequest.criteria list must not be empty"));
            return;
        }

        // validate each criterion
        for (QueryPvMetadataRequest.QueryPvMetadataCriterion criterion : request.getCriteriaList()) {
            switch (criterion.getCriterionCase()) {
                case ATTRIBUTESCRITERION -> {
                    if (criterion.getAttributesCriterion().getKey().isBlank()) {
                        dispatcher.handleValidationError(new ResultStatus(
                                true, "QueryPvMetadataRequest.criteria.AttributesCriterion key must be specified"));
                        return;
                    }
                }
                case CRITERION_NOT_SET -> {
                    dispatcher.handleValidationError(new ResultStatus(
                            true, "QueryPvMetadataRequest.criteria criterion case not set"));
                    return;
                }
                default -> { /* pvNameCriterion, aliasesCriterion, tagsCriterion are valid as-is */ }
            }
        }

        final MongoCursor<PvMetadataDocument> cursor = mongoClient.executeQueryPvMetadata(request);
        if (cursor == null) {
            dispatcher.handleError("error executing pvMetadata query");
            return;
        }
        final String nextPageToken = mongoClient.getQueryPvMetadataNextPageToken(request);
        dispatcher.handleResult(cursor, nextPageToken);
    }
}
