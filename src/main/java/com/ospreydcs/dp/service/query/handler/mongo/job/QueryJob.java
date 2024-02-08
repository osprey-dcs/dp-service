package com.ospreydcs.dp.service.query.handler.mongo.job;

import com.ospreydcs.dp.grpc.v1.query.QueryRequest;
import com.ospreydcs.dp.grpc.v1.query.QueryResponse;
import com.ospreydcs.dp.service.query.handler.mongo.client.MongoQueryClientInterface;
import com.ospreydcs.dp.service.query.handler.mongo.dispatch.BucketCursorResponseDispatcher;
import io.grpc.stub.StreamObserver;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class QueryJob extends HandlerJob {

    // static variables
    private static final Logger logger = LogManager.getLogger();

    // instance variables
    private final QueryRequest.QuerySpec querySpec;
    private final BucketCursorResponseDispatcher dispatcher;
    private final StreamObserver<QueryResponse> responseObserver;
    private final MongoQueryClientInterface mongoClient;

    public QueryJob(QueryRequest.QuerySpec spec,
                    BucketCursorResponseDispatcher dispatcher,
                    StreamObserver<QueryResponse> responseObserver,
                    MongoQueryClientInterface mongoClient
    ) {
        this.querySpec = spec;
        this.dispatcher = dispatcher;
        this.responseObserver = responseObserver;
        this.mongoClient = mongoClient;
    }

    public void execute() {
        logger.debug("executing QueryJob id: {}", this.responseObserver.hashCode());
        final var cursor = this.mongoClient.executeQuery(this.querySpec);
        logger.debug("dispatching QueryJob id: {}", this.responseObserver.hashCode());
        dispatcher.handleResult(cursor);
    }

}
