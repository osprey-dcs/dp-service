package com.ospreydcs.dp.service.ingestionstream.handler.job;

import com.ospreydcs.dp.grpc.v1.ingestion.SubscribeDataResponse;
import com.ospreydcs.dp.service.common.handler.HandlerJob;
import com.ospreydcs.dp.service.ingestionstream.handler.monitor.EventMonitor;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class EventMonitorSubscribeDataResponseJob extends HandlerJob {

    // static variables
    private static final Logger logger = LogManager.getLogger();

    // instance variables
    private final EventMonitor eventMonitor;
    private final SubscribeDataResponse subscribeDataResponse;
    private final boolean isError;
    private final boolean isCompleted;

    public EventMonitorSubscribeDataResponseJob(
            EventMonitor eventMonitor,
            SubscribeDataResponse subscribeDataResponse
    ) {
        super();
        this.eventMonitor = eventMonitor;
        this.subscribeDataResponse = subscribeDataResponse;
        this.isError = false;
        this.isCompleted = false;
    }

    public EventMonitorSubscribeDataResponseJob(
            EventMonitor eventMonitor,
            boolean isError,
            boolean isCompleted
    ) {
        super();
        this.eventMonitor = eventMonitor;
        this.subscribeDataResponse = null;
        this.isError = isError;
        this.isCompleted = isCompleted;
    }

    @Override
    public void execute() {

        logger.debug("executing EventMonitorSubscribeDataResponseJob id: {}", eventMonitor.hashCode());

        if (subscribeDataResponse != null) {
            eventMonitor.handleSubscribeDataResponse(subscribeDataResponse);

        } else if (isError) {
            eventMonitor.handleError("unexpected grpc error in subscribeData() response stream");

        } else if (isCompleted) {
            eventMonitor.handleError("subscribeData() response stream unexpectedly closed");
        }
    }
}
