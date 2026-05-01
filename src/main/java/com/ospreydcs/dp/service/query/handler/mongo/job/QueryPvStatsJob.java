package com.ospreydcs.dp.service.query.handler.mongo.job;

import com.mongodb.client.MongoCursor;
import com.ospreydcs.dp.grpc.v1.query.QueryPvStatsRequest;
import com.ospreydcs.dp.grpc.v1.query.QueryPvStatsResponse;
import com.ospreydcs.dp.service.common.bson.PvMetadataQueryResultDocument;
import com.ospreydcs.dp.service.common.handler.HandlerJob;
import com.ospreydcs.dp.service.query.handler.mongo.client.MongoQueryClientInterface;
import com.ospreydcs.dp.service.query.handler.mongo.dispatch.QueryPvStatsDispatcher;
import io.grpc.stub.StreamObserver;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class QueryPvStatsJob extends HandlerJob {

    // static variables
    private static final Logger logger = LogManager.getLogger();

    // instance variables
    private final QueryPvStatsRequest request;
    private final StreamObserver<QueryPvStatsResponse> responseObserver;
    private final QueryPvStatsDispatcher dispatcher;
    private final MongoQueryClientInterface mongoClient;

    public QueryPvStatsJob(
            QueryPvStatsRequest request,
            StreamObserver<QueryPvStatsResponse> responseObserver,
            MongoQueryClientInterface mongoClient
    ) {
        this.request = request;
        this.responseObserver = responseObserver;
        this.mongoClient = mongoClient;
        dispatcher = new QueryPvStatsDispatcher(responseObserver, request);
    }

    @Override
    public void execute() {
        logger.debug("executing QueryPvStatsJob id: {}", this.responseObserver.hashCode());
        final MongoCursor<PvMetadataQueryResultDocument> cursor = this.mongoClient.executeQueryPvStats(this.request);
        logger.debug("dispatching QueryPvStatsJob id: {}", this.responseObserver.hashCode());
        dispatcher.handleResult(cursor);
    }
}
