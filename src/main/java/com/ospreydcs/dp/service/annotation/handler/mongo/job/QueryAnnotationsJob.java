package com.ospreydcs.dp.service.annotation.handler.mongo.job;

import com.ospreydcs.dp.grpc.v1.annotation.QueryAnnotationsRequest;
import com.ospreydcs.dp.grpc.v1.annotation.QueryAnnotationsResponse;
import com.ospreydcs.dp.service.annotation.handler.model.AnnotationQueryPageToken;
import com.ospreydcs.dp.service.annotation.handler.mongo.client.MongoAnnotationClientInterface;
import com.ospreydcs.dp.service.common.model.AnnotationQueryResult;
import com.ospreydcs.dp.service.common.handler.HandlerJob;
import com.ospreydcs.dp.service.annotation.handler.mongo.dispatch.QueryAnnotationsDispatcher;
import com.ospreydcs.dp.service.common.model.ResultStatus;
import io.grpc.stub.StreamObserver;
import org.bson.types.ObjectId;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class QueryAnnotationsJob extends HandlerJob {
    
    // static variables
    private static final Logger logger = LogManager.getLogger();

    // instance variables
    private final QueryAnnotationsRequest request;
    private final StreamObserver<QueryAnnotationsResponse> responseObserver;
    private final QueryAnnotationsDispatcher dispatcher;
    private final MongoAnnotationClientInterface mongoClient;

    public QueryAnnotationsJob(
            QueryAnnotationsRequest request,
            StreamObserver<QueryAnnotationsResponse> responseObserver,
            MongoAnnotationClientInterface mongoClient
    ) {
        this.request = request;
        this.responseObserver = responseObserver;
        this.mongoClient = mongoClient;
        dispatcher = new QueryAnnotationsDispatcher(responseObserver);
    }

    @Override
    public void execute() {

        logger.debug("executing QueryAnnotationsJob id: {}", this.responseObserver.hashCode());
        // a non-empty pageToken must be one this server issued for this query; unparseable and
        // wrong-query tokens are rejected per the API contract (isEmpty, not isBlank: a
        // whitespace token was never issued, so it must reject rather than silently reset)
        ObjectId resumeAfterId = null;
        if (!request.getPageToken().isEmpty()) {
            final AnnotationQueryPageToken pageToken = AnnotationQueryPageToken.decode(
                    request.getPageToken(), AnnotationQueryPageToken.QUERY_ANNOTATIONS);
            if (pageToken == null) {
                dispatcher.handleValidationError(new ResultStatus(
                        true, "QueryAnnotationsRequest.pageToken is not a valid page token"));
                return;
            }
            resumeAfterId = new ObjectId(pageToken.lastId());
        }

        final AnnotationQueryResult queryResult =
                this.mongoClient.executeQueryAnnotations(this.request, resumeAfterId);
        if (queryResult == null) {
            dispatcher.handleError("error executing annotations query");
            return;
        }

        logger.debug("dispatching QueryAnnotationsJob id: {}", this.responseObserver.hashCode());
        dispatcher.handleResult(queryResult);
    }
}
