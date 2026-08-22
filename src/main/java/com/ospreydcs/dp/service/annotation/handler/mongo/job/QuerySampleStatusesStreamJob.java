package com.ospreydcs.dp.service.annotation.handler.mongo.job;

import com.ospreydcs.dp.grpc.v1.annotation.QuerySampleStatusesRequest;
import com.ospreydcs.dp.grpc.v1.annotation.QuerySampleStatusesResponse;
import com.ospreydcs.dp.service.annotation.handler.SampleStatusValidationUtility;
import com.ospreydcs.dp.service.annotation.handler.model.SampleStatusPageToken;
import com.ospreydcs.dp.service.annotation.handler.mongo.MongoAnnotationHandler;
import com.ospreydcs.dp.service.annotation.handler.mongo.client.MongoAnnotationClientInterface;
import com.ospreydcs.dp.service.annotation.handler.mongo.dispatch.QuerySampleStatusesStreamDispatcher;
import com.ospreydcs.dp.service.common.handler.HandlerJob;
import com.ospreydcs.dp.service.common.model.ResultStatus;
import com.ospreydcs.dp.service.common.model.SampleStatusQueryResult;
import io.grpc.stub.StreamObserver;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * Job for the server-streaming querySampleStatusesStream(): pages through the result internally
 * (keyset positions never cross the wire) and streams one response message per limit-sized chunk
 * until the result is exhausted. Streaming is fire-and-consume: a non-empty request pageToken is
 * rejected and streamed messages carry no continuation tokens.
 */
public class QuerySampleStatusesStreamJob extends HandlerJob {

    private static final Logger logger = LogManager.getLogger();

    private final QuerySampleStatusesRequest request;
    private final StreamObserver<QuerySampleStatusesResponse> responseObserver;
    private final MongoAnnotationClientInterface mongoClient;
    private final QuerySampleStatusesStreamDispatcher dispatcher;

    public QuerySampleStatusesStreamJob(
            QuerySampleStatusesRequest request,
            StreamObserver<QuerySampleStatusesResponse> responseObserver,
            MongoAnnotationClientInterface mongoClient
    ) {
        this.request = request;
        this.responseObserver = responseObserver;
        this.mongoClient = mongoClient;
        this.dispatcher = new QuerySampleStatusesStreamDispatcher(responseObserver, request);
    }

    @Override
    public void execute() {
        logger.debug("executing QuerySampleStatusesStreamJob id: {}", responseObserver.hashCode());

        final ResultStatus resultStatus =
                SampleStatusValidationUtility.validateQuerySampleStatusesRequest(request);
        if (resultStatus.isError) {
            dispatcher.handleValidationError(resultStatus);
            return;
        }

        if (!request.getPageToken().isBlank()) {
            dispatcher.handleValidationError(new ResultStatus(true,
                    "QuerySampleStatusesRequest.pageToken must be empty for querySampleStatusesStream"));
            return;
        }

        final int limit = MongoAnnotationHandler.sampleStatusQueryPageSize(request.getLimit());

        SampleStatusPageToken position = null;
        boolean firstChunk = true;
        while (true) {
            final SampleStatusQueryResult queryResult =
                    mongoClient.executeQuerySampleStatuses(request, limit, position);
            if (queryResult == null) {
                dispatcher.handleError("error executing sample status query");
                return;
            }

            // an empty overall result streams a single empty-result message (success, not
            // exceptional); an empty trailing page is not re-sent
            if (!queryResult.getDocuments().isEmpty() || firstChunk) {
                if (!dispatcher.handleChunk(queryResult.getDocuments())) {
                    return;
                }
            }
            firstChunk = false;

            if (queryResult.getNextPageToken().isEmpty()) {
                break;
            }
            position = SampleStatusPageToken.decode(queryResult.getNextPageToken());
            if (position == null) {
                dispatcher.handleError("error decoding internal page token");
                return;
            }
        }

        dispatcher.handleCompleted();
    }
}
