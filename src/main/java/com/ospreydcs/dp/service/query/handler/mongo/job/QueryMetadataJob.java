package com.ospreydcs.dp.service.query.handler.mongo.job;

import com.mongodb.client.MongoCursor;
import com.ospreydcs.dp.grpc.v1.query.QueryMetadataRequest;
import com.ospreydcs.dp.grpc.v1.query.QueryMetadataResponse;
import com.ospreydcs.dp.service.common.handler.HandlerJob;
import com.ospreydcs.dp.service.query.handler.mongo.client.MongoQueryClientInterface;
import com.ospreydcs.dp.service.query.handler.mongo.dispatch.MetadataResponseDispatcher;
import io.grpc.stub.StreamObserver;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.bson.Document;

public class QueryMetadataJob extends HandlerJob {

    // static variables
    private static final Logger logger = LogManager.getLogger();

    // instance variables
    private final QueryMetadataRequest.QuerySpec querySpec;
    private final StreamObserver<QueryMetadataResponse> responseObserver;
    private final MetadataResponseDispatcher dispatcher;
    private final MongoQueryClientInterface mongoClient;

    public QueryMetadataJob(
            QueryMetadataRequest.QuerySpec querySpec,
            StreamObserver<QueryMetadataResponse> responseObserver,
            MongoQueryClientInterface mongoClient
    ) {
        this.querySpec = querySpec;
        this.responseObserver = responseObserver;
        this.mongoClient = mongoClient;
        dispatcher = new MetadataResponseDispatcher(responseObserver, querySpec);
    }

    @Override
    public void execute() {
        logger.debug("executing ColumnInfoQueryJob id: {}", this.responseObserver.hashCode());
        final MongoCursor<Document> cursor = this.mongoClient.executeQueryMetadata(this.querySpec);
        logger.debug("dispatching ColumnInfoQueryJob id: {}", this.responseObserver.hashCode());
        dispatcher.handleResult(cursor);
    }
}
