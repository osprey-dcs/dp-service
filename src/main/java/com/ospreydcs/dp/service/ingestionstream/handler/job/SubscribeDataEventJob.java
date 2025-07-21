package com.ospreydcs.dp.service.ingestionstream.handler.job;

import com.ospreydcs.dp.service.common.handler.HandlerJob;
import com.ospreydcs.dp.service.common.model.ResultStatus;
import com.ospreydcs.dp.service.ingestionstream.handler.EventMonitorManager;
import com.ospreydcs.dp.service.ingestionstream.handler.interfaces.IngestionStreamHandlerInterface;
import com.ospreydcs.dp.service.ingestionstream.handler.monitor.EventMonitor;
import com.ospreydcs.dp.service.ingestionstream.service.IngestionStreamServiceImpl;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class SubscribeDataEventJob extends HandlerJob {

    // static variables
    private static final Logger logger = LogManager.getLogger();

    // instance variables
    private final IngestionStreamHandlerInterface handler;
    private final EventMonitor eventMonitor;
    private final EventMonitorManager eventMonitorManager;

    public SubscribeDataEventJob(
            IngestionStreamHandlerInterface handler,
            EventMonitor eventMonitor,
            EventMonitorManager manager
    ) {
        this.handler = handler;
        this.eventMonitor = eventMonitor;
        this.eventMonitorManager = manager;
    }

    @Override
    public void execute() {

        logger.debug("executing EventMonitorSubscribeDataResponseJob id: {}", eventMonitor.hashCode());

        // initiate EventMonitor subscription
        ResultStatus subscriptionStatue = eventMonitor.initiateSubscription();

        // send a reject if there is an error initiating subscribeData() subscription
        if (subscriptionStatue.isError) {
            eventMonitor.handleReject(subscriptionStatue.msg);
            return;

        } else {
            // send ack response
            IngestionStreamServiceImpl.sendSubscribeDataEventResponseAck(eventMonitor.responseObserver);
        }

        // add monitor to manager
        eventMonitorManager.addEventMonitor(eventMonitor);
    }
}
