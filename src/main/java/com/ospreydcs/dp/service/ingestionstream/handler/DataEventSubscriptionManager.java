package com.ospreydcs.dp.service.ingestionstream.handler;

import com.ospreydcs.dp.grpc.v1.ingestion.DpIngestionServiceGrpc;
import com.ospreydcs.dp.grpc.v1.ingestion.SubscribeDataRequest;
import com.ospreydcs.dp.service.ingest.utility.IngestionServiceClientUtility;
import com.ospreydcs.dp.service.ingest.utility.SubscribeDataUtility;
import com.ospreydcs.dp.service.ingestionstream.handler.model.EventMonitorSubscribeDataResponseObserver;
import com.ospreydcs.dp.service.ingestionstream.handler.monitor.EventMonitor;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class DataEventSubscriptionManager {

    // instance variables
    private final Map<String, EventMonitorSubscribeDataResponseObserver> pvDataSubscriptions = new HashMap<>();
    private final Map<String, List<EventMonitor>> pvMonitors = new HashMap<>();

    public void addEventMonitor(final EventMonitor eventMonitor) {

        // add subscriptions for event monitor
        for (String pvName : eventMonitor.getPvNames()) {

            // check to see if we already have a subscription for pvName
            if (!pvDataSubscriptions.containsKey(pvName)) {
                // create a new subscription for pvName

                // build an API request
                final SubscribeDataRequest request = SubscribeDataUtility.buildSubscribeDataRequest(List.of(pvName));

                // create observer for subscribeData() API method response stream
                final EventMonitorSubscribeDataResponseObserver responseObserver =
                        new EventMonitorSubscribeDataResponseObserver(this);

                // add entry to map for tracking subscriptions by PV name
                pvDataSubscriptions.put(pvName, responseObserver);

                // use singleton ingestion client to create an API stub
                IngestionServiceClientUtility.IngestionServiceClient client =
                        IngestionServiceClientUtility.IngestionServiceClient.getInstance();
                final DpIngestionServiceGrpc.DpIngestionServiceStub stub = client.newStub();

                // call subscribeData() API method to receive data for specified PV from Ingestion Service
                stub.subscribeData(request, responseObserver);
            }

            // update map of PV name to list of EventMonitors that subscribe to PV data
            List<EventMonitor> pvEventMonitorList = pvMonitors.get(pvName);
            if (pvEventMonitorList == null) {
                // this is the first EventMonitor for specified PV, so create a new list
                pvEventMonitorList = new ArrayList<>();
                pvMonitors.put(pvName, pvEventMonitorList);
            }
            pvEventMonitorList.add(eventMonitor);
        }
    }

}
