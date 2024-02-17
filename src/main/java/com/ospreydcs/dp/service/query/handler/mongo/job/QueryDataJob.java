package com.ospreydcs.dp.service.query.handler.mongo.job;

import com.ospreydcs.dp.grpc.v1.query.QueryDataRequest;
import com.ospreydcs.dp.service.query.handler.mongo.client.MongoQueryClientInterface;
import com.ospreydcs.dp.service.query.handler.mongo.dispatch.BucketDocumentResponseDispatcher;
import io.grpc.stub.StreamObserver;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class QueryDataJob extends HandlerJob {

    // static variables
    private static final Logger logger = LogManager.getLogger();

    // instance variables
    private final QueryDataRequest.QuerySpec querySpec;
    private final BucketDocumentResponseDispatcher dispatcher;
    private final StreamObserver responseObserver;
    private final MongoQueryClientInterface mongoClient;

    public QueryDataJob(QueryDataRequest.QuerySpec spec,
                        BucketDocumentResponseDispatcher dispatcher,
                        StreamObserver responseObserver,
                        MongoQueryClientInterface mongoClient
    ) {
        this.querySpec = spec;
        this.dispatcher = dispatcher;
        this.responseObserver = responseObserver;
        this.mongoClient = mongoClient;
    }

    public void execute() {
        logger.debug("executing QueryJob id: {}", this.responseObserver.hashCode());
        final var cursor = this.mongoClient.executeQueryData(this.querySpec);
        logger.debug("dispatching QueryJob id: {}", this.responseObserver.hashCode());
        dispatcher.handleResult(cursor);
    }

}
