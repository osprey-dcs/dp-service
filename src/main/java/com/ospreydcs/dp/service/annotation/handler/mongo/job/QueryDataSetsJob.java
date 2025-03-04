package com.ospreydcs.dp.service.annotation.handler.mongo.job;

import com.mongodb.client.MongoCursor;
import com.ospreydcs.dp.grpc.v1.annotation.QueryDataSetsRequest;
import com.ospreydcs.dp.grpc.v1.annotation.QueryDataSetsResponse;
import com.ospreydcs.dp.service.annotation.handler.mongo.client.MongoAnnotationClientInterface;
import com.ospreydcs.dp.service.annotation.handler.mongo.dispatch.QueryDataSetsDispatcher;
import com.ospreydcs.dp.service.common.bson.dataset.DataSetDocument;
import com.ospreydcs.dp.service.common.handler.HandlerJob;
import io.grpc.stub.StreamObserver;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class QueryDataSetsJob extends HandlerJob {

    // static variables
    private static final Logger logger = LogManager.getLogger();

    // instance variables
    private final QueryDataSetsRequest request;
    private final StreamObserver<QueryDataSetsResponse> responseObserver;
    private final QueryDataSetsDispatcher dispatcher;
    private final MongoAnnotationClientInterface mongoClient;

    public QueryDataSetsJob(
            QueryDataSetsRequest request,
            StreamObserver<QueryDataSetsResponse> responseObserver,
            MongoAnnotationClientInterface mongoClient
    ) {
        this.request = request;
        this.responseObserver = responseObserver;
        this.mongoClient = mongoClient;
        dispatcher = new QueryDataSetsDispatcher(responseObserver, request, mongoClient);
    }

    @Override
    public void execute() {

        logger.debug("executing QueryDataSetsJob id: {}", this.responseObserver.hashCode());
        final MongoCursor<DataSetDocument> cursor = this.mongoClient.executeQueryDataSets(this.request);

        logger.debug("dispatching QueryDataSetsJob id: {}", this.responseObserver.hashCode());
        dispatcher.handleResult(cursor);
    }
}
