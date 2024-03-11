package com.ospreydcs.dp.service.query.handler.mongo.job;

import com.ospreydcs.dp.grpc.v1.query.QueryDataRequest;
import com.ospreydcs.dp.grpc.v1.query.QueryTableResponse;
import com.ospreydcs.dp.service.common.handler.HandlerJob;
import com.ospreydcs.dp.service.query.handler.mongo.client.MongoQueryClientInterface;
import com.ospreydcs.dp.service.query.handler.mongo.dispatch.BucketDocumentResponseDispatcher;
import com.ospreydcs.dp.service.query.handler.mongo.dispatch.TableResponseDispatcher;
import io.grpc.stub.StreamObserver;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class QueryTableJob extends HandlerJob {

    // static variables
    private static final Logger logger = LogManager.getLogger();

    // instance variables
    private final QueryDataRequest.QuerySpec querySpec;
    private final TableResponseDispatcher dispatcher;
    private final StreamObserver<QueryTableResponse> responseObserver;
    private final MongoQueryClientInterface mongoClient;

    public QueryTableJob(QueryDataRequest.QuerySpec spec,
                         StreamObserver<QueryTableResponse> responseObserver,
                         MongoQueryClientInterface mongoClient
    ) {
        this.querySpec = spec;
        this.responseObserver = responseObserver;
        this.mongoClient = mongoClient;
        this.dispatcher = new TableResponseDispatcher(responseObserver, querySpec);
    }

    public void execute() {
        logger.debug("executing QueryTableJob id: {}", this.responseObserver.hashCode());
        final var cursor = this.mongoClient.executeQueryData(this.querySpec);
        logger.debug("dispatching QueryTableJob id: {}", this.responseObserver.hashCode());
        dispatcher.handleResult(cursor);
    }


}
