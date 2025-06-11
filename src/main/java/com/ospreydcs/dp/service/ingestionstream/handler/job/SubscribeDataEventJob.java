package com.ospreydcs.dp.service.ingestionstream.handler.job;

import com.ospreydcs.dp.service.common.handler.HandlerJob;
import com.ospreydcs.dp.service.common.model.ResultStatus;
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

    public SubscribeDataEventJob(
            IngestionStreamHandlerInterface handler,
            EventMonitor eventMonitor
    ) {
        this.handler = handler;
        this.eventMonitor = eventMonitor;
    }

    @Override
    public void execute() {

        // add event monitor to subscription manager
        ResultStatus addEventStatus = handler.addEventMonitorSubscription(eventMonitor);
        if (addEventStatus.isError) {
            IngestionStreamServiceImpl.sendSubscribeDataEventResponseReject(
                    addEventStatus.msg,
                    eventMonitor.responseObserver);

        } else {
            // send ack response
            IngestionStreamServiceImpl.sendSubscribeDataEventResponseAck(eventMonitor.responseObserver);
        }
    }
}
