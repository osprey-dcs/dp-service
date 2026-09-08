package com.ospreydcs.dp.service.annotation.handler.mongo.job;

import com.ospreydcs.dp.grpc.v1.annotation.QueryDataSetsRequest;
import com.ospreydcs.dp.grpc.v1.annotation.QueryDataSetsResponse;
import com.ospreydcs.dp.service.annotation.handler.model.AnnotationQueryPageToken;
import com.ospreydcs.dp.service.annotation.handler.mongo.client.MongoAnnotationClientInterface;
import com.ospreydcs.dp.service.annotation.handler.mongo.dispatch.QueryDataSetsDispatcher;
import com.ospreydcs.dp.service.common.handler.HandlerJob;
import com.ospreydcs.dp.service.common.model.DataSetQueryResult;
import com.ospreydcs.dp.service.common.model.ResultStatus;
import io.grpc.stub.StreamObserver;
import org.bson.types.ObjectId;
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

        // a non-empty pageToken must be one this server issued for this query; unparseable and
        // wrong-query tokens are rejected per the API contract (isEmpty, not isBlank: a
        // whitespace token was never issued, so it must reject rather than silently reset)
        ObjectId resumeAfterId = null;
        if (!request.getPageToken().isEmpty()) {
            final AnnotationQueryPageToken pageToken = AnnotationQueryPageToken.decode(
                    request.getPageToken(), AnnotationQueryPageToken.QUERY_DATA_SETS);
            if (pageToken == null) {
                dispatcher.handleValidationError(new ResultStatus(
                        true, "QueryDataSetsRequest.pageToken is not a valid page token"));
                return;
            }
            resumeAfterId = new ObjectId(pageToken.lastId());
        }

        final DataSetQueryResult queryResult =
                this.mongoClient.executeQueryDataSets(this.request, resumeAfterId);
        if (queryResult == null) {
            dispatcher.handleError("error executing dataSets query");
            return;
        }

        logger.debug("dispatching QueryDataSetsJob id: {}", this.responseObserver.hashCode());
        dispatcher.handleResult(queryResult);
    }
}
