package com.ospreydcs.dp.service.query.handler.mongo.job;

import com.mongodb.client.MongoCursor;
import com.ospreydcs.dp.grpc.v1.query.QueryAnnotationsRequest;
import com.ospreydcs.dp.grpc.v1.query.QueryAnnotationsResponse;
import com.ospreydcs.dp.service.common.bson.annotation.AnnotationDocument;
import com.ospreydcs.dp.service.common.handler.HandlerJob;
import com.ospreydcs.dp.service.query.handler.mongo.client.MongoQueryClientInterface;
import com.ospreydcs.dp.service.query.handler.mongo.dispatch.AnnotationsResponseDispatcher;
import io.grpc.stub.StreamObserver;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.bson.Document;

public class QueryAnnotationsJob extends HandlerJob {
    
    // static variables
    private static final Logger logger = LogManager.getLogger();

    // instance variables
    private final QueryAnnotationsRequest request;
    private final StreamObserver<QueryAnnotationsResponse> responseObserver;
    private final AnnotationsResponseDispatcher dispatcher;
    private final MongoQueryClientInterface mongoClient;

    public QueryAnnotationsJob(
            QueryAnnotationsRequest request,
            StreamObserver<QueryAnnotationsResponse> responseObserver,
            MongoQueryClientInterface mongoClient
    ) {
        this.request = request;
        this.responseObserver = responseObserver;
        this.mongoClient = mongoClient;
        dispatcher = new AnnotationsResponseDispatcher(responseObserver, request);
    }

    @Override
    public void execute() {
        logger.debug("executing QueryAnnotationsJob id: {}", this.responseObserver.hashCode());
        final MongoCursor<AnnotationDocument> cursor = this.mongoClient.executeQueryAnnotations(this.request);
        logger.debug("dispatching QueryAnnotationsJob id: {}", this.responseObserver.hashCode());
        dispatcher.handleResult(cursor);
    }
}
