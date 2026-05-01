package com.ospreydcs.dp.service.query.handler.mongo.job;

import com.mongodb.client.MongoCursor;
import com.ospreydcs.dp.grpc.v1.query.QueryProviderStatsRequest;
import com.ospreydcs.dp.grpc.v1.query.QueryProviderStatsResponse;
import com.ospreydcs.dp.service.common.bson.ProviderMetadataQueryResultDocument;
import com.ospreydcs.dp.service.common.handler.HandlerJob;
import com.ospreydcs.dp.service.query.handler.mongo.client.MongoQueryClientInterface;
import com.ospreydcs.dp.service.query.handler.mongo.dispatch.QueryProviderStatsDispatcher;
import io.grpc.stub.StreamObserver;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class QueryProviderStatsJob extends HandlerJob {

    // static variables
    private static final Logger logger = LogManager.getLogger();

    // instance variables
    private final QueryProviderStatsRequest request;
    private final StreamObserver<QueryProviderStatsResponse> responseObserver;
    private final QueryProviderStatsDispatcher dispatcher;
    private final MongoQueryClientInterface mongoClient;

    public QueryProviderStatsJob(
            QueryProviderStatsRequest request,
            StreamObserver<QueryProviderStatsResponse> responseObserver,
            MongoQueryClientInterface mongoClient
    ) {
        this.request = request;
        this.responseObserver = responseObserver;
        this.mongoClient = mongoClient;
        dispatcher = new QueryProviderStatsDispatcher(responseObserver, request);
    }

    @Override
    public void execute() {
        logger.debug("executing QueryProviderStatsJob id: {}", this.responseObserver.hashCode());
        final MongoCursor<ProviderMetadataQueryResultDocument> cursor =
                this.mongoClient.executeQueryProviderStats(this.request);
        logger.debug("dispatching QueryProviderStatsJob id: {}", this.responseObserver.hashCode());
        dispatcher.handleResult(cursor);
    }
}
