package com.ospreydcs.dp.service.annotation.handler.mongo.job;

import com.ospreydcs.dp.grpc.v1.annotation.GetDataSetRequest;
import com.ospreydcs.dp.grpc.v1.annotation.GetDataSetResponse;
import com.ospreydcs.dp.service.annotation.handler.mongo.client.MongoAnnotationClientInterface;
import com.ospreydcs.dp.service.annotation.handler.mongo.dispatch.GetDataSetDispatcher;
import com.ospreydcs.dp.service.common.bson.dataset.DataSetDocument;
import com.ospreydcs.dp.service.common.exception.DpException;
import com.ospreydcs.dp.service.common.handler.HandlerJob;
import com.ospreydcs.dp.service.common.model.ResultStatus;
import io.grpc.stub.StreamObserver;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.bson.types.ObjectId;

public class GetDataSetJob extends HandlerJob {

    private static final Logger logger = LogManager.getLogger();

    private final GetDataSetRequest request;
    private final StreamObserver<GetDataSetResponse> responseObserver;
    private final MongoAnnotationClientInterface mongoClient;
    private final GetDataSetDispatcher dispatcher;

    public GetDataSetJob(
            GetDataSetRequest request,
            StreamObserver<GetDataSetResponse> responseObserver,
            MongoAnnotationClientInterface mongoClient
    ) {
        this.request = request;
        this.responseObserver = responseObserver;
        this.mongoClient = mongoClient;
        this.dispatcher = new GetDataSetDispatcher(responseObserver, request);
    }

    @Override
    public void execute() {
        logger.debug("executing GetDataSetJob id: {}", responseObserver.hashCode());

        if (request.getDataSetId().isBlank()) {
            dispatcher.handleValidationError(
                    new ResultStatus(true, "GetDataSetRequest.dataSetId must be specified"));
            return;
        }

        // A malformed id is a client mistake, rejected here (#248 plan D11). Unvalidated, it would
        // throw IllegalArgumentException from the ObjectId constructor inside the worker thread,
        // where QueueHandlerBase swallows it and the caller's stream hangs with no response.
        if (!ObjectId.isValid(request.getDataSetId())) {
            dispatcher.handleValidationError(
                    new ResultStatus(true,
                            "GetDataSetRequest.dataSetId is not a valid id: " + request.getDataSetId()));
            return;
        }

        final DataSetDocument document;
        try {
            document = mongoClient.lookupDataSet(request.getDataSetId());
        } catch (DpException ex) {
            dispatcher.handleError("error looking up DataSet: " + ex.getMessage());
            return;
        }
        dispatcher.handleResult(document);
    }
}
