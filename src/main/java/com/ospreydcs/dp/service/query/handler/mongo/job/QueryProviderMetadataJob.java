package com.ospreydcs.dp.service.query.handler.mongo.job;

import com.mongodb.client.MongoCursor;
import com.ospreydcs.dp.grpc.v1.query.QueryProviderMetadataRequest;
import com.ospreydcs.dp.grpc.v1.query.QueryProviderMetadataResponse;
import com.ospreydcs.dp.service.common.bson.ProviderMetadataQueryResultDocument;
import com.ospreydcs.dp.service.common.handler.HandlerJob;
import com.ospreydcs.dp.service.query.handler.mongo.client.MongoQueryClientInterface;
import com.ospreydcs.dp.service.query.handler.mongo.dispatch.QueryProviderMetadataResponseDispatcher;
import io.grpc.stub.StreamObserver;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class QueryProviderMetadataJob extends HandlerJob {

    // static variables
    private static final Logger logger = LogManager.getLogger();

    // instance variables
    private final QueryProviderMetadataRequest request;
    private final StreamObserver<QueryProviderMetadataResponse> responseObserver;
    private final QueryProviderMetadataResponseDispatcher dispatcher;
    private final MongoQueryClientInterface mongoClient;

    public QueryProviderMetadataJob(
            QueryProviderMetadataRequest request,
            StreamObserver<QueryProviderMetadataResponse> responseObserver,
            MongoQueryClientInterface mongoClient
    ) {
        this.request = request;
        this.responseObserver = responseObserver;
        this.mongoClient = mongoClient;
        dispatcher = new QueryProviderMetadataResponseDispatcher(responseObserver, request);
    }

    @Override
    public void execute() {
        logger.debug("executing QueryProviderMetadataJob id: {}", this.responseObserver.hashCode());
        final MongoCursor<ProviderMetadataQueryResultDocument> cursor =
                this.mongoClient.executeQueryProviderMetadata(this.request);
        logger.debug("dispatching QueryProviderMetadataJob id: {}", this.responseObserver.hashCode());
        dispatcher.handleResult(cursor);
    }
}
