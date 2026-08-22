package com.ospreydcs.dp.service.annotation.handler.mongo.job;

import com.ospreydcs.dp.grpc.v1.annotation.SaveSampleStatusesRequest;
import com.ospreydcs.dp.grpc.v1.annotation.SaveSampleStatusesResponse;
import com.ospreydcs.dp.service.annotation.handler.SampleStatusValidationUtility;
import com.ospreydcs.dp.service.annotation.handler.mongo.MongoAnnotationHandler;
import com.ospreydcs.dp.service.annotation.handler.mongo.client.MongoAnnotationClientInterface;
import com.ospreydcs.dp.service.annotation.handler.mongo.dispatch.SaveSampleStatusesDispatcher;
import com.ospreydcs.dp.service.common.handler.HandlerJob;
import com.ospreydcs.dp.service.common.model.MongoCountResult;
import com.ospreydcs.dp.service.common.model.ResultStatus;
import io.grpc.stub.StreamObserver;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class SaveSampleStatusesJob extends HandlerJob {

    private static final Logger logger = LogManager.getLogger();

    private final SaveSampleStatusesRequest request;
    private final StreamObserver<SaveSampleStatusesResponse> responseObserver;
    private final MongoAnnotationClientInterface mongoClient;
    private final SaveSampleStatusesDispatcher dispatcher;

    public SaveSampleStatusesJob(
            SaveSampleStatusesRequest request,
            StreamObserver<SaveSampleStatusesResponse> responseObserver,
            MongoAnnotationClientInterface mongoClient
    ) {
        this.request = request;
        this.responseObserver = responseObserver;
        this.mongoClient = mongoClient;
        this.dispatcher = new SaveSampleStatusesDispatcher(responseObserver, request);
    }

    @Override
    public void execute() {
        logger.debug("executing SaveSampleStatusesJob id: {}", responseObserver.hashCode());

        // the request is validated and rejected as a whole; nothing is persisted on rejection
        final ResultStatus resultStatus = SampleStatusValidationUtility.validateSaveSampleStatusesRequest(
                request, MongoAnnotationHandler.sampleStatusSaveMaxStatuses());
        if (resultStatus.isError) {
            dispatcher.handleValidationError(resultStatus);
            return;
        }

        final MongoCountResult result = mongoClient.saveSampleStatuses(request);
        dispatcher.handleResult(result);
    }
}
