package com.ospreydcs.dp.service.ingestionstream.handler.job;

import com.ospreydcs.dp.grpc.v1.ingestion.SubscribeDataResponse;
import com.ospreydcs.dp.service.common.handler.HandlerJob;
import com.ospreydcs.dp.service.ingestionstream.handler.EventMonitorSubscriptionManager;

public class EventMonitorSubscribeDataResponseJob extends HandlerJob {

    // instance variables
    private final String pvName;
    private final EventMonitorSubscriptionManager subscriptionManager;
    private final SubscribeDataResponse subscribeDataResponse;
    private final boolean isError;
    private final boolean isCompleted;

    public EventMonitorSubscribeDataResponseJob(
            String pvName,
            EventMonitorSubscriptionManager subscriptionManager,
            SubscribeDataResponse subscribeDataResponse
    ) {
        super();
        this.pvName = pvName;
        this.subscriptionManager = subscriptionManager;
        this.subscribeDataResponse = subscribeDataResponse;
        this.isError = false;
        this.isCompleted = false;
    }

    public EventMonitorSubscribeDataResponseJob(
            String pvName,
            EventMonitorSubscriptionManager subscriptionManager,
            boolean isError,
            boolean isCompleted
    ) {
        super();
        this.pvName = pvName;
        this.subscriptionManager = subscriptionManager;
        this.subscribeDataResponse = null;
        this.isError = isError;
        this.isCompleted = isCompleted;
    }

    @Override
    public void execute() {
        if (subscribeDataResponse != null) {
            subscriptionManager.handleSubscribeDataResponse(pvName, subscribeDataResponse);
        } else if (isError) {
            subscriptionManager.handleExceptionalResult(
                    pvName, "subscribeData() unexpected error in response stream for pv: " + pvName);
        } else if (isCompleted) {
            subscriptionManager.handleExceptionalResult(
                    pvName, "subscribeData() response stream closed for pv: " + pvName);
        }
    }
}
