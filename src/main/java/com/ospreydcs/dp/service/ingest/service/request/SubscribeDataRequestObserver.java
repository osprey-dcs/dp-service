package com.ospreydcs.dp.service.ingest.service.request;

import com.ospreydcs.dp.grpc.v1.ingestion.SubscribeDataRequest;
import com.ospreydcs.dp.grpc.v1.ingestion.SubscribeDataResponse;
import com.ospreydcs.dp.service.ingest.handler.interfaces.IngestionHandlerInterface;
import com.ospreydcs.dp.service.ingest.model.SourceMonitor;
import com.ospreydcs.dp.service.ingest.service.IngestionServiceImpl;
import io.grpc.stub.StreamObserver;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;

public class SubscribeDataRequestObserver implements StreamObserver<SubscribeDataRequest> {

    // static variables
    private static final Logger logger = LogManager.getLogger();

    // instance variables
    private final StreamObserver<SubscribeDataResponse> responseObserver;
    private final IngestionHandlerInterface handler;
    private SourceMonitor monitor = null;

    public SubscribeDataRequestObserver(
            StreamObserver<SubscribeDataResponse> responseObserver,
            IngestionHandlerInterface handler
    ) {
        this.responseObserver = responseObserver;
        this.handler = handler;
    }

    @Override
    public void onNext(SubscribeDataRequest subscribeDataRequest) {

        logger.info(
                "id: {} received {} request",
                responseObserver.hashCode(),
                subscribeDataRequest.getRequestCase().name());

        switch (subscribeDataRequest.getRequestCase()) {

            case NEWSUBSCRIPTION -> {

                if (monitor != null) {
                    // we don't support modifying the initial subscription, so send a reject to be clear that multiple
                    // new subscription messages in the request stream is not supported
                    final String errorMsg = "multiple NewSubscription messages not supported in request stream";
                    logger.debug(
                            "id: {} " + errorMsg,
                            responseObserver.hashCode());
                    monitor.handleReject(errorMsg);
                    initiateShutdown();
                    return;
                }

                final SubscribeDataRequest.NewSubscription newSubscription = subscribeDataRequest.getNewSubscription();

                // validate request
                final List<String> pvNames = newSubscription.getPvNamesList();

                if (pvNames.isEmpty()) {
                    final String errorMsg = "SubscribeDataRequest.NewSubscription.pvNames list must not be empty";
                    logger.debug(
                            "id: {} " + errorMsg,
                            responseObserver.hashCode());

                    // send reject directly since we don't yet have an EventMonitor
                    IngestionServiceImpl.sendSubscribeDataResponseReject(errorMsg, responseObserver);
                    responseObserver.onCompleted();
                    return;
                }

                // handle request
                monitor = handler.handleSubscribeData(subscribeDataRequest, responseObserver);

                logger.debug(
                        "id: {} created SourceMonitor: {}",
                        responseObserver.hashCode(),
                        monitor.hashCode());
            }

            case CANCELSUBSCRIPTION -> {
                initiateShutdown();
            }

            case REQUEST_NOT_SET -> {
                final String errorMsg =
                        "received unknown request, expected NewSubscription or CancelSubscription";
                logger.debug(
                        "id: {} " + errorMsg,
                        responseObserver.hashCode());
                IngestionServiceImpl.sendSubscribeDataResponseReject(errorMsg, responseObserver);
                if (monitor == null) {
                    responseObserver.onCompleted();
                }
                initiateShutdown();
            }

        }
    }

    @Override
    public void onError(Throwable throwable) {
        logger.debug(
                "id: {} onError",
                responseObserver.hashCode());
        initiateShutdown();
    }

    @Override
    public void onCompleted() {
        logger.debug(
                "id: {} onCompleted",
                responseObserver.hashCode());
        initiateShutdown();
    }

    private void initiateShutdown() {
        if (monitor != null) {
            handler.terminateSourceMonitor(this.monitor);
        }
    }
}
