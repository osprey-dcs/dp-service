package com.ospreydcs.dp.service.ingestionstream.handler;

import com.ospreydcs.dp.grpc.v1.ingestionstream.SubscribeDataEventRequest;
import com.ospreydcs.dp.grpc.v1.ingestionstream.SubscribeDataEventResponse;
import com.ospreydcs.dp.service.common.handler.QueueHandlerBase;
import com.ospreydcs.dp.service.common.model.ResultStatus;
import com.ospreydcs.dp.service.ingestionstream.handler.interfaces.IngestionStreamHandlerInterface;
import com.ospreydcs.dp.service.ingestionstream.handler.job.EventMonitorJob;
import com.ospreydcs.dp.service.ingestionstream.handler.job.SubscribeDataEventJob;
import com.ospreydcs.dp.service.ingestionstream.handler.monitor.EventMonitor;
import io.grpc.stub.StreamObserver;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class IngestionStreamHandler extends QueueHandlerBase implements IngestionStreamHandlerInterface {

    // static variables
    private static final Logger logger = LogManager.getLogger();

    // configuration
    public static final String CFG_KEY_NUM_WORKERS = "IngestionStreamHandler.numWorkers";
    public static final int DEFAULT_NUM_WORKERS = 7;

    // instance variables
    private final EventMonitorSubscriptionManager subscriptionManager =
            new EventMonitorSubscriptionManager(this);

    @Override
    protected boolean init_() {
        logger.trace("init_");
        return true;
    }

    @Override
    protected boolean fini_() {
        logger.trace("fini_");
        return true;
    }

    @Override
    protected int getNumWorkers_() {
        return configMgr().getConfigInteger(CFG_KEY_NUM_WORKERS, DEFAULT_NUM_WORKERS);
    }

    public void addJob(EventMonitorJob job) {
        try {
            requestQueue.put(job);
        } catch (InterruptedException e) {
            logger.error("InterruptedException waiting for requestQueue.put");
            Thread.currentThread().interrupt();
        }
    }

    @Override
    public EventMonitor handleSubscribeDataEvent(
            SubscribeDataEventRequest request,
            StreamObserver<SubscribeDataEventResponse> responseObserver
    ) {
        // create an event monitor for the request
        EventMonitor eventMonitor = new EventMonitor(
                request.getNewSubscription(), responseObserver, subscriptionManager);
        final SubscribeDataEventJob job = new SubscribeDataEventJob(this, eventMonitor);

        logger.debug("id: {} adding SubscribeDataEventJob to queue", responseObserver.hashCode());

        try {
            requestQueue.put(job);
        } catch (InterruptedException e) {
            logger.error(
                    "id: {} InterruptedException waiting for requestQueue.put",
                    responseObserver.hashCode());
            Thread.currentThread().interrupt();
        }

        return eventMonitor;
    }

    @Override
    public void cancelDataEventSubscriptions(
            StreamObserver<SubscribeDataEventResponse> responseObserver
    ) {
        // TODO
    }

    @Override
    public ResultStatus addEventMonitorSubscription(EventMonitor eventMonitor) {
        return subscriptionManager.addEventMonitor(eventMonitor);
    }

}
