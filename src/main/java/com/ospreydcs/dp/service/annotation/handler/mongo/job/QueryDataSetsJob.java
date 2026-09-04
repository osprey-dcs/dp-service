package com.ospreydcs.dp.service.annotation.handler.mongo.job;

import com.ospreydcs.dp.grpc.v1.annotation.QueryDataSetsRequest;
import com.ospreydcs.dp.grpc.v1.annotation.QueryDataSetsResponse;
import com.ospreydcs.dp.service.annotation.handler.mongo.client.MongoAnnotationClientInterface;
import com.ospreydcs.dp.service.annotation.handler.mongo.dispatch.QueryDataSetsDispatcher;
import com.ospreydcs.dp.service.common.handler.HandlerJob;
import com.ospreydcs.dp.service.common.model.DataSetQueryResult;
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
        dispatcher = new QueryDataSetsDispatcher(responseObserver);
    }

    @Override
    public void execute() {

        logger.debug("executing QueryDataSetsJob id: {}", this.responseObserver.hashCode());
        final DataSetQueryResult queryResult = this.mongoClient.executeQueryDataSets(this.request);
        if (queryResult == null) {
            dispatcher.handleError("error executing dataSets query");
            return;
        }

        logger.debug("dispatching QueryDataSetsJob id: {}", this.responseObserver.hashCode());
        dispatcher.handleResult(queryResult);
    }
}
