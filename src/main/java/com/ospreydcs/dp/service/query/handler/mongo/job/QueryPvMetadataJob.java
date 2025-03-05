package com.ospreydcs.dp.service.query.handler.mongo.job;

import com.mongodb.client.MongoCursor;
import com.ospreydcs.dp.grpc.v1.query.QueryPvMetadataRequest;
import com.ospreydcs.dp.grpc.v1.query.QueryPvMetadataResponse;
import com.ospreydcs.dp.service.common.bson.PvMetadataQueryResultDocument;
import com.ospreydcs.dp.service.common.handler.HandlerJob;
import com.ospreydcs.dp.service.query.handler.mongo.client.MongoQueryClientInterface;
import com.ospreydcs.dp.service.query.handler.mongo.dispatch.QueryPvMetadataDispatcher;
import io.grpc.stub.StreamObserver;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class QueryPvMetadataJob extends HandlerJob {

    // static variables
    private static final Logger logger = LogManager.getLogger();

    // instance variables
    private final QueryPvMetadataRequest request;
    private final StreamObserver<QueryPvMetadataResponse> responseObserver;
    private final QueryPvMetadataDispatcher dispatcher;
    private final MongoQueryClientInterface mongoClient;

    public QueryPvMetadataJob(
            QueryPvMetadataRequest request,
            StreamObserver<QueryPvMetadataResponse> responseObserver,
            MongoQueryClientInterface mongoClient
    ) {
        this.request = request;
        this.responseObserver = responseObserver;
        this.mongoClient = mongoClient;
        dispatcher = new QueryPvMetadataDispatcher(responseObserver, request);
    }

    @Override
    public void execute() {
        logger.debug("executing QueryMetadataJob id: {}", this.responseObserver.hashCode());
        final MongoCursor<PvMetadataQueryResultDocument> cursor = this.mongoClient.executeQueryPvMetadata(this.request);
        logger.debug("dispatching QueryMetadataJob id: {}", this.responseObserver.hashCode());
        dispatcher.handleResult(cursor);
    }
}
