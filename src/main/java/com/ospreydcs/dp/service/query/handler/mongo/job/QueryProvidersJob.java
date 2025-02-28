package com.ospreydcs.dp.service.query.handler.mongo.job;

import com.mongodb.client.MongoCursor;
import com.ospreydcs.dp.grpc.v1.query.QueryProvidersRequest;
import com.ospreydcs.dp.grpc.v1.query.QueryProvidersResponse;
import com.ospreydcs.dp.service.common.bson.ProviderDocument;
import com.ospreydcs.dp.service.common.handler.HandlerJob;
import com.ospreydcs.dp.service.query.handler.mongo.client.MongoQueryClientInterface;
import com.ospreydcs.dp.service.query.handler.mongo.dispatch.QueryProvidersResponseDispatcher;
import io.grpc.stub.StreamObserver;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class QueryProvidersJob extends HandlerJob {

    // static variables
    private static final Logger logger = LogManager.getLogger();

    // instance variables
    private final QueryProvidersRequest request;
    private final StreamObserver<QueryProvidersResponse> responseObserver;
    private final QueryProvidersResponseDispatcher dispatcher;
    private final MongoQueryClientInterface mongoClient;

    public QueryProvidersJob(
            QueryProvidersRequest request,
            StreamObserver<QueryProvidersResponse> responseObserver,
            MongoQueryClientInterface mongoClient
    ) {
        this.request = request;
        this.responseObserver = responseObserver;
        this.mongoClient = mongoClient;
        dispatcher = new QueryProvidersResponseDispatcher(responseObserver, request);
    }

    @Override
    public void execute() {
        logger.debug("executing QueryProvidersJob id: {}", this.responseObserver.hashCode());
        final MongoCursor<ProviderDocument> cursor =
                this.mongoClient.executeQueryProviders(this.request);
        logger.debug("dispatching QueryProvidersJob id: {}", this.responseObserver.hashCode());
        dispatcher.handleResult(cursor);
    }
}
