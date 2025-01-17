package com.ospreydcs.dp.service.ingestionstream.handler.model;

import com.ospreydcs.dp.grpc.v1.ingestion.SubscribeDataResponse;
import com.ospreydcs.dp.service.ingestionstream.handler.DataEventSubscriptionManager;
import com.ospreydcs.dp.service.ingestionstream.handler.IngestionStreamHandler;
import com.ospreydcs.dp.service.ingestionstream.handler.job.EventMonitorJob;
import io.grpc.stub.StreamObserver;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class EventMonitorSubscribeDataResponseObserver implements StreamObserver<SubscribeDataResponse> {

    // static variables
    private static final Logger logger = LogManager.getLogger();

    // instance variables
    private final String pvName;
    private final DataEventSubscriptionManager subscriptionManager;
    private final IngestionStreamHandler handler;

    public EventMonitorSubscribeDataResponseObserver(
            final String pvName,
            final DataEventSubscriptionManager subscriptionManager,
            final IngestionStreamHandler handler
    ) {
        this.pvName = pvName;
        this.subscriptionManager = subscriptionManager;
        this.handler = handler;
    }

    @Override
    public void onNext(
            SubscribeDataResponse subscribeDataResponse
    ) {
        switch (subscribeDataResponse.getResultCase()) {
            case EXCEPTIONALRESULT -> {
                logger.trace("received exceptional result for pv: {}", pvName);
                // TODO: nothing else to do because stream should also be closed? confirm
            }
            case ACKRESULT -> {
                logger.trace("received ack result for pv: {}", pvName);
                // TODO: nothing necessarily to do, this is just informational
            }
            case SUBSCRIBEDATARESULT -> {
                logger.trace("received subscribeData result for pv: {}", pvName);
                final EventMonitorJob job = new EventMonitorJob(
                        pvName, subscriptionManager, subscribeDataResponse.getSubscribeDataResult());
                handler.addJob(job);
            }
            case RESULT_NOT_SET -> {
                logger.trace("received result not set for pv: {}", pvName);
                // TODO: maybe we should do the same as onError / onCompleted here to clean up?
                // this is not expected but not sure what subsequent messaging would be if anything
            }
        }
    }

    @Override
    public void onError(Throwable throwable) {
        // TODO - notify subscriptionManager of problem so it can clean up,
        // remove responseObserver and list of EventMonitors for PV name and
    }

    @Override
    public void onCompleted() {
        // TODO - notify subscriptionManager of problem so it can clean up,
        // remove responseObserver and list of EventMonitors for PV name and
    }
}
