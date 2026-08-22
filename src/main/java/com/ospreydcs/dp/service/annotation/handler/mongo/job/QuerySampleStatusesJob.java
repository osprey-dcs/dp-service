package com.ospreydcs.dp.service.annotation.handler.mongo.job;

import com.ospreydcs.dp.grpc.v1.annotation.QuerySampleStatusesRequest;
import com.ospreydcs.dp.grpc.v1.annotation.QuerySampleStatusesResponse;
import com.ospreydcs.dp.service.annotation.handler.SampleStatusValidationUtility;
import com.ospreydcs.dp.service.annotation.handler.model.SampleStatusPageToken;
import com.ospreydcs.dp.service.annotation.handler.mongo.MongoAnnotationHandler;
import com.ospreydcs.dp.service.annotation.handler.mongo.client.MongoAnnotationClientInterface;
import com.ospreydcs.dp.service.annotation.handler.mongo.dispatch.QuerySampleStatusesDispatcher;
import com.ospreydcs.dp.service.common.handler.HandlerJob;
import com.ospreydcs.dp.service.common.model.ResultStatus;
import com.ospreydcs.dp.service.common.model.SampleStatusQueryResult;
import io.grpc.stub.StreamObserver;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class QuerySampleStatusesJob extends HandlerJob {

    private static final Logger logger = LogManager.getLogger();

    private final QuerySampleStatusesRequest request;
    private final StreamObserver<QuerySampleStatusesResponse> responseObserver;
    private final MongoAnnotationClientInterface mongoClient;
    private final QuerySampleStatusesDispatcher dispatcher;

    public QuerySampleStatusesJob(
            QuerySampleStatusesRequest request,
            StreamObserver<QuerySampleStatusesResponse> responseObserver,
            MongoAnnotationClientInterface mongoClient
    ) {
        this.request = request;
        this.responseObserver = responseObserver;
        this.mongoClient = mongoClient;
        this.dispatcher = new QuerySampleStatusesDispatcher(responseObserver, request);
    }

    @Override
    public void execute() {
        logger.debug("executing QuerySampleStatusesJob id: {}", responseObserver.hashCode());

        final ResultStatus resultStatus =
                SampleStatusValidationUtility.validateQuerySampleStatusesRequest(request);
        if (resultStatus.isError) {
            dispatcher.handleValidationError(resultStatus);
            return;
        }

        // a non-empty pageToken must be one this server issued; unparseable tokens are rejected
        SampleStatusPageToken position = null;
        if (!request.getPageToken().isBlank()) {
            position = SampleStatusPageToken.decode(request.getPageToken());
            if (position == null) {
                dispatcher.handleValidationError(new ResultStatus(
                        true, "QuerySampleStatusesRequest.pageToken is not a valid page token"));
                return;
            }
        }

        final int limit = MongoAnnotationHandler.sampleStatusQueryPageSize(request.getLimit());

        final SampleStatusQueryResult queryResult =
                mongoClient.executeQuerySampleStatuses(request, limit, position);
        if (queryResult == null) {
            dispatcher.handleError("error executing sample status query");
            return;
        }
        dispatcher.handleResult(queryResult.getDocuments(), queryResult.getNextPageToken());
    }
}
