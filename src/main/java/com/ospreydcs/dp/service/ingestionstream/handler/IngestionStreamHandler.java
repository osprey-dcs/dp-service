package com.ospreydcs.dp.service.ingestionstream.handler;

import com.ospreydcs.dp.grpc.v1.ingestionstream.SubscribeDataEventRequest;
import com.ospreydcs.dp.grpc.v1.ingestionstream.SubscribeDataEventResponse;
import com.ospreydcs.dp.service.common.handler.QueueHandlerBase;
import com.ospreydcs.dp.service.ingest.utility.IngestionServiceClientUtility;
import com.ospreydcs.dp.service.ingestionstream.handler.interfaces.IngestionStreamHandlerInterface;
import com.ospreydcs.dp.service.ingestionstream.handler.job.EventMonitorSubscribeDataResponseJob;
import com.ospreydcs.dp.service.ingestionstream.handler.job.SubscribeDataEventJob;
import com.ospreydcs.dp.service.ingestionstream.handler.monitor.EventMonitor;
import io.grpc.ManagedChannel;
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

    private EventMonitorManager eventMonitorManager = null;
    private final IngestionServiceClientUtility.IngestionServiceGrpcClient ingestionServiceGrpcClient;

    public IngestionStreamHandler() {
        super();
        this.ingestionServiceGrpcClient = new IngestionServiceClientUtility.IngestionServiceGrpcClient();
        initializeSubscriptionManager(ingestionServiceGrpcClient);
    }

    public IngestionStreamHandler(ManagedChannel channel) {
        super();
        this.ingestionServiceGrpcClient = new IngestionServiceClientUtility.IngestionServiceGrpcClient(channel);
        initializeSubscriptionManager(ingestionServiceGrpcClient);
    }

    private void initializeSubscriptionManager(IngestionServiceClientUtility.IngestionServiceGrpcClient client) {
        this.eventMonitorManager = new EventMonitorManager(this);
    }

    @Override
    protected boolean init_() {
        logger.trace("init_");
        return true;
    }

    @Override
    protected boolean fini_() {
        logger.trace("fini_");
        this.eventMonitorManager.shutdown();
        return true;
    }

    @Override
    protected int getNumWorkers_() {
        return configMgr().getConfigInteger(CFG_KEY_NUM_WORKERS, DEFAULT_NUM_WORKERS);
    }

    public void addJob(EventMonitorSubscribeDataResponseJob job) {
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
                request.getNewSubscription(),
                responseObserver,
                this,
                this.ingestionServiceGrpcClient);

        // add EventMonitor to manager
        eventMonitorManager.addEventMonitor(eventMonitor);

        // create job for EventMonitor
        final SubscribeDataEventJob job = new SubscribeDataEventJob(this, eventMonitor, eventMonitorManager);

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
    public void terminateEventMonitor(EventMonitor eventMonitor) {
        logger.debug(
                "terminateEventMonitor id: {}",
                eventMonitor.responseObserver.hashCode());
        this.eventMonitorManager.terminateMonitor(eventMonitor);
    }

}
