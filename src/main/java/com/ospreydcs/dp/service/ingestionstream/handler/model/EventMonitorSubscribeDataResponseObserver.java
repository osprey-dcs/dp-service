package com.ospreydcs.dp.service.ingestionstream.handler.model;

import com.ospreydcs.dp.grpc.v1.ingestion.SubscribeDataResponse;
import com.ospreydcs.dp.service.ingestionstream.handler.DataEventSubscriptionManager;
import io.grpc.stub.StreamObserver;

public class EventMonitorSubscribeDataResponseObserver implements StreamObserver<SubscribeDataResponse> {

    private final DataEventSubscriptionManager subscriptionManager;

    public EventMonitorSubscribeDataResponseObserver(final DataEventSubscriptionManager subscriptionManager) {
        this.subscriptionManager = subscriptionManager;
    }

    @Override
    public void onNext(SubscribeDataResponse subscribeDataResponse) {

    }

    @Override
    public void onError(Throwable throwable) {

    }

    @Override
    public void onCompleted() {

    }
}
