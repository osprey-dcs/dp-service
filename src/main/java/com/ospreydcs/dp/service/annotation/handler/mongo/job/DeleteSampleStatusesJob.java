package com.ospreydcs.dp.service.annotation.handler.mongo.job;

import com.ospreydcs.dp.grpc.v1.annotation.DeleteSampleStatusesRequest;
import com.ospreydcs.dp.grpc.v1.annotation.DeleteSampleStatusesResponse;
import com.ospreydcs.dp.service.annotation.handler.SampleStatusValidationUtility;
import com.ospreydcs.dp.service.annotation.handler.mongo.client.MongoAnnotationClientInterface;
import com.ospreydcs.dp.service.annotation.handler.mongo.dispatch.DeleteSampleStatusesDispatcher;
import com.ospreydcs.dp.service.common.handler.HandlerJob;
import com.ospreydcs.dp.service.common.model.MongoCountResult;
import com.ospreydcs.dp.service.common.model.ResultStatus;
import io.grpc.stub.StreamObserver;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class DeleteSampleStatusesJob extends HandlerJob {

    private static final Logger logger = LogManager.getLogger();

    private final DeleteSampleStatusesRequest request;
    private final StreamObserver<DeleteSampleStatusesResponse> responseObserver;
    private final MongoAnnotationClientInterface mongoClient;
    private final DeleteSampleStatusesDispatcher dispatcher;

    public DeleteSampleStatusesJob(
            DeleteSampleStatusesRequest request,
            StreamObserver<DeleteSampleStatusesResponse> responseObserver,
            MongoAnnotationClientInterface mongoClient
    ) {
        this.request = request;
        this.responseObserver = responseObserver;
        this.mongoClient = mongoClient;
        this.dispatcher = new DeleteSampleStatusesDispatcher(responseObserver, request);
    }

    @Override
    public void execute() {
        logger.debug("executing DeleteSampleStatusesJob id: {}", responseObserver.hashCode());

        final ResultStatus resultStatus =
                SampleStatusValidationUtility.validateDeleteSampleStatusesRequest(request);
        if (resultStatus.isError) {
            dispatcher.handleValidationError(resultStatus);
            return;
        }

        final MongoCountResult result = mongoClient.deleteSampleStatuses(request);
        dispatcher.handleResult(result);
    }
}
