package com.ospreydcs.dp.service.ingest.handler.mongo.dispatch;

import com.mongodb.client.result.UpdateResult;
import com.ospreydcs.dp.grpc.v1.ingestion.RegisterProviderRequest;
import com.ospreydcs.dp.grpc.v1.ingestion.RegisterProviderResponse;
import com.ospreydcs.dp.service.common.mongo.UpdateResultWrapper;
import com.ospreydcs.dp.service.ingest.service.IngestionServiceImpl;
import io.grpc.stub.StreamObserver;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class RegisterProviderDispatcher {

    // static variables
    private static final Logger logger = LogManager.getLogger();

    // instance variables
    private final RegisterProviderRequest request;
    private final StreamObserver<RegisterProviderResponse> responseObserver;

    public RegisterProviderDispatcher(
            StreamObserver<RegisterProviderResponse> responseObserver,
            RegisterProviderRequest request
    ) {
        this.responseObserver = responseObserver;
        this.request = request;
    }

    public void handleError(String errorMsg) {
        IngestionServiceImpl.sendRegisterProviderResponseError(errorMsg, responseObserver);
    }

    public void handleResult(boolean isNewProvider, String providerName, String providerId) {

        // send successful response with details about the provider
        IngestionServiceImpl.sendRegisterProviderResponseSuccess(
                providerName, providerId, isNewProvider, responseObserver);
    }
}
