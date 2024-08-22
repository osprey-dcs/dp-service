package com.ospreydcs.dp.service.ingest.handler.mongo.job;

import com.mongodb.client.result.UpdateResult;
import com.ospreydcs.dp.grpc.v1.ingestion.RegisterProviderRequest;
import com.ospreydcs.dp.grpc.v1.ingestion.RegisterProviderResponse;
import com.ospreydcs.dp.service.common.bson.ProviderDocument;
import com.ospreydcs.dp.service.common.handler.HandlerJob;
import com.ospreydcs.dp.service.common.mongo.UpdateResultWrapper;
import com.ospreydcs.dp.service.ingest.handler.model.FindProviderResult;
import com.ospreydcs.dp.service.ingest.handler.mongo.MongoIngestionHandler;
import com.ospreydcs.dp.service.ingest.handler.mongo.client.MongoIngestionClientInterface;
import com.ospreydcs.dp.service.ingest.handler.mongo.dispatch.RegisterProviderDispatcher;
import com.ospreydcs.dp.service.ingest.service.IngestionServiceImpl;
import io.grpc.stub.StreamObserver;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Objects;

public class RegisterProviderJob extends HandlerJob {

    // static variables
    private static final Logger logger = LogManager.getLogger();

    // instance variables
    private final RegisterProviderRequest request;
    private final StreamObserver<RegisterProviderResponse> responseObserver;
    private final MongoIngestionClientInterface client;
    private final MongoIngestionHandler handler;
    private RegisterProviderDispatcher dispatcher;

    public RegisterProviderJob(
            RegisterProviderRequest request,
            StreamObserver<RegisterProviderResponse> responseObserver,
            MongoIngestionClientInterface client,
            MongoIngestionHandler handler
    ) {
        this.request = request;
        this.responseObserver = responseObserver;
        this.client = client;
        this.handler = handler;
        this.dispatcher = new RegisterProviderDispatcher(responseObserver, request);
    }

    @Override
    public void execute() {

        // upsert ProviderDocument (update existing document or create new one)
        logger.debug("upserting document for RegisterProviderJob id: {}", this.responseObserver.hashCode());
        final UpdateResultWrapper updateResultWrapper = client.upsertProvider(request);

        // handle upsert exception
        if (updateResultWrapper.isException()) {
            final String errorMsg = "exception upserting ProviderDocument: " + updateResultWrapper.exception;
            dispatcher.handleError(errorMsg);
            return;
        }

        Objects.requireNonNull(updateResultWrapper.updateResult);
        final UpdateResult updateResult = updateResultWrapper.updateResult;

        // handle upsert not acknowledged
        if (!updateResult.wasAcknowledged()) {
            final String errorMsg = "ProviderDocument upsert failed, not acknowledged";
            dispatcher.handleError(errorMsg);
            return;
        }

        // dispatch result if new document inserted by upsert
        if (updateResult.getUpsertedId() != null) {
            final String providerId = updateResult.getUpsertedId().asObjectId().getValue().toString();
            dispatcher.handleResult(true, request.getProviderName(), providerId);
            return;
        }

        // find id if existing document updated by upsert (not returned in UpdateResult, unfortunately)
        logger.debug("finding document for RegisterProviderJob id: {}", this.responseObserver.hashCode());
        final FindProviderResult findProviderResult = client.findProvider(request.getProviderName());
        if (findProviderResult.isException) {
            Objects.requireNonNull(findProviderResult.errorMessage);
            dispatcher.handleError(findProviderResult.errorMessage);
            return;
        } else {
            Objects.requireNonNull(findProviderResult.providerDocument);
            dispatcher.handleResult(
                    false,
                    request.getProviderName(),
                    findProviderResult.providerDocument.getId().toString());
            return;
        }
    }
}
