package com.ospreydcs.dp.service.ingestionstream.handler;

import com.ospreydcs.dp.grpc.v1.ingestionstream.SubscribeDataEventRequest;
import com.ospreydcs.dp.grpc.v1.ingestionstream.SubscribeDataEventResponse;
import com.ospreydcs.dp.service.common.handler.QueueHandlerBase;
import com.ospreydcs.dp.service.ingestionstream.handler.interfaces.IngestionStreamHandlerInterface;
import com.ospreydcs.dp.service.ingestionstream.handler.job.SubscribeDataEventJob;
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
    private final DataEventSubscriptionManager subscriptionManager = new DataEventSubscriptionManager();

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

    @Override
    public void handleSubscribeDataEvent(
            SubscribeDataEventRequest request,
            StreamObserver<SubscribeDataEventResponse> responseObserver
    ) {
        final SubscribeDataEventJob job = new SubscribeDataEventJob(request, responseObserver, subscriptionManager);

        logger.debug("adding SubscribeDataEventJob id: {} to queue", responseObserver.hashCode());

        try {
            requestQueue.put(job);
        } catch (InterruptedException e) {
            logger.error("InterruptedException waiting for requestQueue.put");
            Thread.currentThread().interrupt();
        }
    }

    @Override
    public void cancelDataEventSubscriptions(
            StreamObserver<SubscribeDataEventResponse> responseObserver
    ) {
        // TODO
    }
}
