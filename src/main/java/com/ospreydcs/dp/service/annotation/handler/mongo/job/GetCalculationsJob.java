package com.ospreydcs.dp.service.annotation.handler.mongo.job;

import com.ospreydcs.dp.grpc.v1.annotation.GetCalculationsRequest;
import com.ospreydcs.dp.grpc.v1.annotation.GetCalculationsResponse;
import com.ospreydcs.dp.service.annotation.handler.AnnotationValidationUtility;
import com.ospreydcs.dp.service.annotation.handler.mongo.client.MongoAnnotationClientInterface;
import com.ospreydcs.dp.service.annotation.handler.mongo.dispatch.GetCalculationsDispatcher;
import com.ospreydcs.dp.service.common.bson.calculations.CalculationsDocument;
import com.ospreydcs.dp.service.common.exception.DpException;
import com.ospreydcs.dp.service.common.handler.HandlerJob;
import com.ospreydcs.dp.service.common.model.ResultStatus;
import io.grpc.stub.StreamObserver;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.bson.types.ObjectId;

public class GetCalculationsJob extends HandlerJob {

    private static final Logger logger = LogManager.getLogger();

    private final GetCalculationsRequest request;
    private final StreamObserver<GetCalculationsResponse> responseObserver;
    private final MongoAnnotationClientInterface mongoClient;
    private final GetCalculationsDispatcher dispatcher;

    public GetCalculationsJob(
            GetCalculationsRequest request,
            StreamObserver<GetCalculationsResponse> responseObserver,
            MongoAnnotationClientInterface mongoClient
    ) {
        this.request = request;
        this.responseObserver = responseObserver;
        this.mongoClient = mongoClient;
        this.dispatcher = new GetCalculationsDispatcher(responseObserver, request);
    }

    @Override
    public void execute() {
        logger.debug("executing GetCalculationsJob id: {}", responseObserver.hashCode());

        // blank/malformed id handling is shared: see validateRequiredObjectId (#248 plan D11)
        final ResultStatus idStatus = AnnotationValidationUtility.validateRequiredObjectId(
                "GetCalculationsRequest.calculationsId", request.getCalculationsId());
        if (idStatus.isError) {
            dispatcher.handleValidationError(idStatus);
            return;
        }

        final CalculationsDocument document;
        try {
            document = mongoClient.lookupCalculations(request.getCalculationsId());
        } catch (DpException ex) {
            dispatcher.handleError("error looking up Calculations: " + ex.getMessage());
            return;
        }
        dispatcher.handleResult(document);
    }
}
