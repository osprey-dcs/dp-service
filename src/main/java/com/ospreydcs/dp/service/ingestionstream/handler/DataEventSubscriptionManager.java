package com.ospreydcs.dp.service.ingestionstream.handler;

import com.ospreydcs.dp.grpc.v1.ingestion.SubscribeDataResponse;
import com.ospreydcs.dp.service.ingestionstream.handler.monitor.EventMonitor;
import io.grpc.stub.StreamObserver;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class DataEventSubscriptionManager {

    // instance variables
    private final Map<String, StreamObserver<SubscribeDataResponse>> pvDataSubscriptions = new HashMap<>();
    private final Map<String, List<EventMonitor>> pvMonitors = new HashMap<>();

    public void addEventMonitor(EventMonitor eventMonitor) {

        // add subscriptions for event monitor
        for (String pvName : eventMonitor.getPvNames()) {

            // check to see if we already have a subscription for pvName
            if (!pvDataSubscriptions.containsKey(pvName)) {
                // create a new subscription for pvName

            }
        }

    }
}
