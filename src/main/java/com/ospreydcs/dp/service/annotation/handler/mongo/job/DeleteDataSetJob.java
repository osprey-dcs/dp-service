package com.ospreydcs.dp.service.annotation.handler.mongo.job;

import com.ospreydcs.dp.grpc.v1.annotation.DeleteDataSetRequest;
import com.ospreydcs.dp.grpc.v1.annotation.DeleteDataSetResponse;
import com.ospreydcs.dp.service.annotation.handler.mongo.client.MongoAnnotationClientInterface;
import com.ospreydcs.dp.service.annotation.handler.mongo.dispatch.DeleteDataSetDispatcher;
import com.ospreydcs.dp.service.common.model.MongoDeleteResult;
import com.ospreydcs.dp.service.common.model.ResultStatus;
import com.ospreydcs.dp.service.common.handler.HandlerJob;
import io.grpc.stub.StreamObserver;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.bson.types.ObjectId;

public class DeleteDataSetJob extends HandlerJob {

    private static final Logger logger = LogManager.getLogger();

    private final DeleteDataSetRequest request;
    private final StreamObserver<DeleteDataSetResponse> responseObserver;
    private final MongoAnnotationClientInterface mongoClient;
    private final DeleteDataSetDispatcher dispatcher;

    public DeleteDataSetJob(
            DeleteDataSetRequest request,
            StreamObserver<DeleteDataSetResponse> responseObserver,
            MongoAnnotationClientInterface mongoClient
    ) {
        this.request = request;
        this.responseObserver = responseObserver;
        this.mongoClient = mongoClient;
        this.dispatcher = new DeleteDataSetDispatcher(responseObserver, request);
    }

    @Override
    public void execute() {
        logger.debug("executing DeleteDataSetJob id: {}", responseObserver.hashCode());

        if (request.getDataSetId().isBlank()) {
            dispatcher.handleValidationError(
                    new ResultStatus(true, "DeleteDataSetRequest.dataSetId must be specified"));
            return;
        }

        // A malformed id is a client mistake, rejected here (#248 plan D11). Unvalidated, it would
        // throw IllegalArgumentException from the ObjectId constructor inside the worker thread,
        // where QueueHandlerBase swallows it and the caller's stream hangs with no response.
        if (!ObjectId.isValid(request.getDataSetId())) {
            dispatcher.handleValidationError(
                    new ResultStatus(true,
                            "DeleteDataSetRequest.dataSetId is not a valid id: " + request.getDataSetId()));
            return;
        }

        final MongoDeleteResult result = mongoClient.deleteDataSet(request.getDataSetId());
        dispatcher.handleResult(result);
    }
}
