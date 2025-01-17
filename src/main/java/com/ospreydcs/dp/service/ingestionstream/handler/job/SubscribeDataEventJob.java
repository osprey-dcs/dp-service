package com.ospreydcs.dp.service.ingestionstream.handler.job;

import com.ospreydcs.dp.grpc.v1.ingestionstream.SubscribeDataEventRequest;
import com.ospreydcs.dp.grpc.v1.ingestionstream.SubscribeDataEventResponse;
import com.ospreydcs.dp.service.common.handler.HandlerJob;
import com.ospreydcs.dp.service.ingestionstream.handler.DataEventSubscriptionManager;
import com.ospreydcs.dp.service.ingestionstream.handler.monitor.ConditionMonitor;
import com.ospreydcs.dp.service.ingestionstream.handler.monitor.EventMonitor;
import com.ospreydcs.dp.service.ingestionstream.service.IngestionStreamServiceImpl;
import io.grpc.stub.StreamObserver;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Objects;

public class SubscribeDataEventJob extends HandlerJob {

    // static variables
    private static final Logger logger = LogManager.getLogger();

    // instance variables
    private final SubscribeDataEventRequest request;
    private final StreamObserver<SubscribeDataEventResponse> responseObserver;
    private final DataEventSubscriptionManager subscriptionManager;

    public SubscribeDataEventJob(
            SubscribeDataEventRequest request,
            StreamObserver<SubscribeDataEventResponse> responseObserver,
            DataEventSubscriptionManager subscriptionManager
    ) {
        this.request = request;
        this.responseObserver = responseObserver;
        this.subscriptionManager = subscriptionManager;
    }

    @Override
    public void execute() {

        // create an event monitor for the request
        EventMonitor eventMonitor = null;
        switch (request.getDataEventDefCase()) {
            case CONDITIONEVENTDEF -> {
                eventMonitor = new ConditionMonitor(request, responseObserver, subscriptionManager);
            }
            case DATAEVENTDEF_NOT_SET -> {
                final String errorMsg = "invalid request, SubscribeDataEventRequest.dataEventDef must be specified";
                logger.debug(errorMsg + " id: " + responseObserver.hashCode());
                IngestionStreamServiceImpl.sendSubscribeDataEventResponseError(errorMsg, responseObserver);
            }
        }
        Objects.requireNonNull(eventMonitor);

        // add event monitor to subscription manager
        subscriptionManager.addEventMonitor(eventMonitor);
    }
}
