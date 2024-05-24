package com.ospreydcs.dp.service.annotation.handler.mongo.job;

import com.mongodb.client.MongoCursor;
import com.ospreydcs.dp.grpc.v1.annotation.QueryAnnotationsRequest;
import com.ospreydcs.dp.grpc.v1.annotation.QueryAnnotationsResponse;
import com.ospreydcs.dp.service.annotation.handler.mongo.client.MongoAnnotationClientInterface;
import com.ospreydcs.dp.service.common.bson.annotation.AnnotationDocument;
import com.ospreydcs.dp.service.common.handler.HandlerJob;
import com.ospreydcs.dp.service.annotation.handler.mongo.dispatch.QueryAnnotationsResponseDispatcher;
import io.grpc.stub.StreamObserver;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class QueryAnnotationsJob extends HandlerJob {
    
    // static variables
    private static final Logger logger = LogManager.getLogger();

    // instance variables
    private final QueryAnnotationsRequest request;
    private final StreamObserver<QueryAnnotationsResponse> responseObserver;
    private final QueryAnnotationsResponseDispatcher dispatcher;
    private final MongoAnnotationClientInterface mongoClient;

    public QueryAnnotationsJob(
            QueryAnnotationsRequest request,
            StreamObserver<QueryAnnotationsResponse> responseObserver,
            MongoAnnotationClientInterface mongoClient
    ) {
        this.request = request;
        this.responseObserver = responseObserver;
        this.mongoClient = mongoClient;
        dispatcher = new QueryAnnotationsResponseDispatcher(responseObserver, request, mongoClient);
    }

    @Override
    public void execute() {

        logger.debug("executing QueryAnnotationsJob id: {}", this.responseObserver.hashCode());
        final MongoCursor<AnnotationDocument> cursor = this.mongoClient.executeQueryAnnotations(this.request);

        logger.debug("dispatching QueryAnnotationsJob id: {}", this.responseObserver.hashCode());
        dispatcher.handleResult(cursor);
    }
}
