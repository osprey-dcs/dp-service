package com.ospreydcs.dp.service.query.handler.mongo.job;

import com.ospreydcs.dp.grpc.v1.query.QueryDataRequest;
import com.ospreydcs.dp.grpc.v1.query.QueryDataResponse;
import com.ospreydcs.dp.service.common.handler.HandlerJob;
import com.ospreydcs.dp.service.query.handler.mongo.client.MongoQueryClientInterface;
import com.ospreydcs.dp.service.query.handler.mongo.dispatch.QueryDataAbstractDispatcher;
import io.grpc.stub.StreamObserver;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class QueryDataJob extends HandlerJob {

    // static variables
    private static final Logger logger = LogManager.getLogger();

    // instance variables
    private final QueryDataRequest.QuerySpec querySpec;
    private final QueryDataAbstractDispatcher dispatcher;
    private final StreamObserver<QueryDataResponse> responseObserver;
    private final MongoQueryClientInterface mongoClient;

    public QueryDataJob(QueryDataRequest.QuerySpec spec,
                        QueryDataAbstractDispatcher dispatcher,
                        StreamObserver<QueryDataResponse> responseObserver,
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
