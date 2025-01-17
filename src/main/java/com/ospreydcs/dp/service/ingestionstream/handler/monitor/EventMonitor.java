package com.ospreydcs.dp.service.ingestionstream.handler.monitor;

import com.ospreydcs.dp.grpc.v1.ingestion.SubscribeDataResponse;
import com.ospreydcs.dp.grpc.v1.ingestionstream.SubscribeDataEventRequest;
import com.ospreydcs.dp.grpc.v1.ingestionstream.SubscribeDataEventResponse;
import com.ospreydcs.dp.service.ingestionstream.handler.DataEventSubscriptionManager;
import com.ospreydcs.dp.service.ingestionstream.service.IngestionStreamServiceImpl;
import io.grpc.stub.StreamObserver;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;

public abstract class EventMonitor {

    // static variables
    private static final Logger logger = LogManager.getLogger();

    // instance variables
    protected final SubscribeDataEventRequest request;
    protected final StreamObserver<SubscribeDataEventResponse> responseObserver;
    protected final DataEventSubscriptionManager subscriptionManager;

    // abstract methods
    public abstract void handleSubscribeDataResult(SubscribeDataResponse.SubscribeDataResult result);

    public EventMonitor(
            SubscribeDataEventRequest request,
            StreamObserver<SubscribeDataEventResponse> responseObserver,
            DataEventSubscriptionManager subscriptionManager
    ) {
        this.request = request;
        this.responseObserver = responseObserver;
        this.subscriptionManager = subscriptionManager;
    }

    public List<String> getPvNames() {
        return request.getPvNamesList();
    }

    protected void handleError(
            String errorMsg
    ) {
        logger.debug("handleError msg: {}", errorMsg);
        subscriptionManager.cancelEventMonitor(this);
        IngestionStreamServiceImpl.sendSubscribeDataEventResponseError(errorMsg, responseObserver);
    }

}
