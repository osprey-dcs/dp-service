package com.ospreydcs.dp.service.ingestionstream.handler.job;

import com.ospreydcs.dp.grpc.v1.ingestion.SubscribeDataResponse;
import com.ospreydcs.dp.service.common.handler.HandlerJob;
import com.ospreydcs.dp.service.ingestionstream.handler.DataEventSubscriptionManager;

public class EventMonitorJob  extends HandlerJob {

    // instance variables
    private final String pvName;
    private final DataEventSubscriptionManager subscriptionManager;
    private final SubscribeDataResponse.SubscribeDataResult subscribeDataResult;

    public EventMonitorJob(
            String pvName,
            DataEventSubscriptionManager subscriptionManager,
            SubscribeDataResponse.SubscribeDataResult subscribeDataResult
    ) {
        super();
        this.pvName = pvName;
        this.subscriptionManager = subscriptionManager;
        this.subscribeDataResult = subscribeDataResult;
    }

    @Override
    public void execute() {
        subscriptionManager.handleSubscribeDataResult(pvName, subscribeDataResult);
    }
}
