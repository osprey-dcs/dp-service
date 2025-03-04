package com.ospreydcs.dp.service.query.handler.mongo.job;

import com.ospreydcs.dp.grpc.v1.query.QueryTableRequest;
import com.ospreydcs.dp.grpc.v1.query.QueryTableResponse;
import com.ospreydcs.dp.service.common.handler.HandlerJob;
import com.ospreydcs.dp.service.query.handler.mongo.client.MongoQueryClientInterface;
import com.ospreydcs.dp.service.query.handler.mongo.dispatch.QueryTableDispatcher;
import io.grpc.stub.StreamObserver;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class QueryTableJob extends HandlerJob {

    // static variables
    private static final Logger logger = LogManager.getLogger();

    // instance variables
    private final QueryTableRequest request;
    private final QueryTableDispatcher dispatcher;
    private final StreamObserver<QueryTableResponse> responseObserver;
    private final MongoQueryClientInterface mongoClient;

    public QueryTableJob(QueryTableRequest request,
                         StreamObserver<QueryTableResponse> responseObserver,
                         MongoQueryClientInterface mongoClient
    ) {
        this.request = request;
        this.responseObserver = responseObserver;
        this.mongoClient = mongoClient;
        this.dispatcher = new QueryTableDispatcher(responseObserver, this.request);
    }

    public void execute() {
        logger.debug("executing QueryTableJob id: {}", this.responseObserver.hashCode());
        final var cursor = this.mongoClient.executeQueryTable(this.request);
        logger.debug("dispatching QueryTableJob id: {}", this.responseObserver.hashCode());
        dispatcher.handleResult(cursor);
    }


}
