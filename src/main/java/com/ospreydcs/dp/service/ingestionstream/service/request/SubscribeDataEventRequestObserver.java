package com.ospreydcs.dp.service.ingestionstream.service.request;

import com.ospreydcs.dp.grpc.v1.ingestionstream.SubscribeDataEventRequest;
import com.ospreydcs.dp.grpc.v1.ingestionstream.SubscribeDataEventResponse;
import com.ospreydcs.dp.service.common.model.ResultStatus;
import com.ospreydcs.dp.service.ingestionstream.handler.IngestionStreamValidationUtility;
import com.ospreydcs.dp.service.ingestionstream.handler.interfaces.IngestionStreamHandlerInterface;
import com.ospreydcs.dp.service.ingestionstream.handler.monitor.EventMonitor;
import com.ospreydcs.dp.service.ingestionstream.service.IngestionStreamServiceImpl;
import io.grpc.stub.StreamObserver;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class SubscribeDataEventRequestObserver implements StreamObserver<SubscribeDataEventRequest> {

    // static variables
    private static final Logger logger = LogManager.getLogger();

    // instance variables
    private final StreamObserver<SubscribeDataEventResponse> responseObserver;
    private final IngestionStreamHandlerInterface handler;
    private EventMonitor monitor = null;

    public SubscribeDataEventRequestObserver(
            StreamObserver<SubscribeDataEventResponse> responseObserver,
            IngestionStreamHandlerInterface handler
    ) {
        this.responseObserver = responseObserver;
        this.handler = handler;
    }

    @Override
    public void onNext(SubscribeDataEventRequest request) {

        logger.info(
                "id: {} received {} request",
                responseObserver.hashCode(),
                request.getRequestCase().name());

        switch (request.getRequestCase()) {

            case NEWSUBSCRIPTION -> {

                if (monitor != null) {
                    // we don't support modifying the initial subscription, so send a reject to be clear that multiple
                    // new subscription messages in the request stream is not supported
                    final String errorMsg = "multiple NewSubscription messages not supported in request stream";
                    logger.debug(
                            "id: {} " + errorMsg,
                            responseObserver.hashCode());
                    monitor.requestCancel();
                    IngestionStreamServiceImpl.sendSubscribeDataEventResponseReject(errorMsg, responseObserver);
                    return;
                }

                // TODO: validate request, create EventMonitor, handle request

                // validate request
                final ResultStatus resultStatus =
                        IngestionStreamValidationUtility.validateSubscribeDataEventRequest(request);
                if (resultStatus.isError) {
                    IngestionStreamServiceImpl.sendSubscribeDataEventResponseReject(
                            resultStatus.msg, responseObserver);
                    return;

                }

//                // add a handler to remove subscription when client closes method connection
//                ServerCallStreamObserver<SubscribeDataEventResponse> serverCallStreamObserver =
//                        (ServerCallStreamObserver<SubscribeDataEventResponse>) responseObserver;
//                serverCallStreamObserver.setOnCancelHandler(
//                        () -> {
//                            logger.trace("onCancelHandler id: {}", responseObserver.hashCode());
//                            if (handler != null) {
//                                handler.cancelDataEventSubscriptions(responseObserver);
//                            }
//                        }
//                );

                // dispatch to handler
                monitor = handler.handleSubscribeDataEvent(request, responseObserver);
            }

            case CANCELSUBSCRIPTION -> {
                monitor.requestCancel();
                responseObserver.onCompleted();
            }

            case REQUEST_NOT_SET -> {
                final String errorMsg =
                        "received unknown request, expected NewSubscription or CancelSubscription";
                logger.debug(
                        "id: {} " + errorMsg,
                        responseObserver.hashCode());
                IngestionStreamServiceImpl.sendSubscribeDataEventResponseReject(errorMsg, responseObserver);
            }

        }
    }

    @Override
    public void onError(Throwable throwable) {
        logger.debug(
                "id: {} onError, requesting cancel",
                responseObserver.hashCode());
        monitor.requestCancel();
        responseObserver.onCompleted();
    }

    @Override
    public void onCompleted() {
        logger.debug(
                "id: {} onCompleted, requesting cancel",
                responseObserver.hashCode());
        monitor.requestCancel();
        responseObserver.onCompleted();
    }
}
