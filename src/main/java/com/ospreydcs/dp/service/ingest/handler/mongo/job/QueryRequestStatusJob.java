package com.ospreydcs.dp.service.ingest.handler.mongo.job;

import com.mongodb.client.MongoCursor;
import com.ospreydcs.dp.grpc.v1.ingestion.QueryRequestStatusRequest;
import com.ospreydcs.dp.grpc.v1.ingestion.QueryRequestStatusResponse;
import com.ospreydcs.dp.service.common.bson.RequestStatusDocument;
import com.ospreydcs.dp.service.common.handler.HandlerJob;
import com.ospreydcs.dp.service.ingest.handler.mongo.client.MongoIngestionClientInterface;
import com.ospreydcs.dp.service.ingest.handler.mongo.dispatch.QueryRequestStatusResponseDispatcher;
import io.grpc.stub.StreamObserver;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class QueryRequestStatusJob extends HandlerJob {

    // static variables
    private static final Logger logger = LogManager.getLogger();

    // instance variables
    private final QueryRequestStatusRequest request;
    private final StreamObserver<QueryRequestStatusResponse> responseObserver;
    private final QueryRequestStatusResponseDispatcher dispatcher;
    private final MongoIngestionClientInterface mongoClient;

    public QueryRequestStatusJob(
            QueryRequestStatusRequest request,
            StreamObserver<QueryRequestStatusResponse> responseObserver,
            MongoIngestionClientInterface mongoClient
    ) {
        this.request = request;
        this.responseObserver = responseObserver;
        this.mongoClient = mongoClient;
        dispatcher = new QueryRequestStatusResponseDispatcher(responseObserver, request, mongoClient);
    }

    @Override
    public void execute() {

        logger.debug("executing QueryRequestStatusJob id: {}", this.responseObserver.hashCode());
        final MongoCursor<RequestStatusDocument> cursor = this.mongoClient.executeQueryRequestStatus(this.request);

        logger.debug("dispatching QueryRequestStatusJob id: {}", this.responseObserver.hashCode());
        dispatcher.handleResult(cursor);
    }

}
