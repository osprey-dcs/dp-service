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

        switch (subscribeDataRequest.getRequestCase()) {

            case NEWSUBSCRIPTION -> {

                if (monitor != null) {
                    // ignore subsequent requests after initial one to create subscription
                    logger.trace(
                            "id: {} ignoring NewSubscription request received after subscription created",
                            responseObserver.hashCode());
                    return;
                }

                final SubscribeDataRequest.NewSubscription newSubscription = subscribeDataRequest.getNewSubscription();

                // validate request
                final List<String> pvNames = newSubscription.getPvNamesList();

                if (pvNames.isEmpty()) {
                    final String errorMsg = "SubscribeDataRequest.NewSubscription.pvNames list must not be empty";
                    logger.debug("id: {} " + errorMsg, responseObserver.hashCode());
                    IngestionServiceImpl.sendSubscribeDataResponseReject(errorMsg, responseObserver);
                    return;
                }

//                // add a handler to remove subscription when client closes method connection
//                ServerCallStreamObserver<SubscribeDataResponse> serverCallStreamObserver =
//                        (ServerCallStreamObserver<SubscribeDataResponse>) responseObserver;
//                serverCallStreamObserver.setOnCancelHandler(
//                        () -> {
//                            logger.trace("onCancelHandler id: {}", responseObserver.hashCode());
//                            if (handler != null) {
//                                handler.cancelDataSubscriptions(responseObserver);
//                            }
//                        }
//                );

                // create SourceMonitor for request
                logger.debug("id: {} creating SourceMonitor", responseObserver.hashCode());
                monitor = new SourceMonitor(handler, pvNames, responseObserver);

                // handle request
                handler.addSourceMonitor(monitor);

            }

            case CANCELSUBSCRIPTION -> {
                logger.debug("id: {} received CancelSubscription request, requesting cancel", responseObserver.hashCode());
                monitor.requestCancel();
            }

            case REQUEST_NOT_SET -> {
                final String errorMsg =
                        "received unknown request, expected NewSubscription or CancelSubscription";
                logger.debug("id: {} " + errorMsg, responseObserver.hashCode());
                IngestionServiceImpl.sendSubscribeDataResponseReject(errorMsg, responseObserver);
            }

        }
    }

    @Override
    public void onError(Throwable throwable) {
        logger.debug("id: {} onError, requesting cancel", responseObserver.hashCode());
        monitor.requestCancel();
    }

    @Override
    public void onCompleted() {
        logger.debug("id: {} onCompleted, requesting cancel", responseObserver.hashCode());
        monitor.requestCancel();
    }
}
