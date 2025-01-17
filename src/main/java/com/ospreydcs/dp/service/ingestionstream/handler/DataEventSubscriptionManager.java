package com.ospreydcs.dp.service.ingestionstream.handler;

import com.ospreydcs.dp.grpc.v1.common.DataColumn;
import com.ospreydcs.dp.grpc.v1.ingestion.DpIngestionServiceGrpc;
import com.ospreydcs.dp.grpc.v1.ingestion.SubscribeDataRequest;
import com.ospreydcs.dp.grpc.v1.ingestion.SubscribeDataResponse;
import com.ospreydcs.dp.service.ingest.utility.IngestionServiceClientUtility;
import com.ospreydcs.dp.service.ingest.utility.SubscribeDataUtility;
import com.ospreydcs.dp.service.ingestionstream.handler.model.EventMonitorSubscribeDataResponseObserver;
import com.ospreydcs.dp.service.ingestionstream.handler.monitor.EventMonitor;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class DataEventSubscriptionManager {

    // static variables
    private static final Logger logger = LogManager.getLogger();

    // instance variables
    private final IngestionStreamHandler handler;
    private final Map<String, EventMonitorSubscribeDataResponseObserver> pvDataSubscriptions = new HashMap<>();
    private final Map<String, List<EventMonitor>> pvMonitors = new HashMap<>();
    private final ReentrantReadWriteLock rwLock = new ReentrantReadWriteLock();
    private final Lock readLock = rwLock.readLock();
    private final Lock writeLock = rwLock.writeLock();

    public DataEventSubscriptionManager(IngestionStreamHandler ingestionStreamHandler) {
        this.handler = ingestionStreamHandler;
    }

    public void addEventMonitor(final EventMonitor eventMonitor) {

        // acquire write lock since method will be called from different threads handling grpc requests/responses
        writeLock.lock();
        try {

            // add subscriptions for event monitor
            for (String pvName : eventMonitor.getPvNames()) {

                // check to see if we already have a subscription for pvName
                if (!pvDataSubscriptions.containsKey(pvName)) {
                    // create a new subscription for pvName

                    // build an API request
                    final SubscribeDataRequest request = SubscribeDataUtility.buildSubscribeDataRequest(List.of(pvName));

                    // create observer for subscribeData() API method response stream
                    final EventMonitorSubscribeDataResponseObserver responseObserver =
                            new EventMonitorSubscribeDataResponseObserver(pvName, this, handler);

                    // add entry to map for tracking subscriptions by PV name
                    pvDataSubscriptions.put(pvName, responseObserver);

                    // use singleton ingestion client to create an API stub
                    final IngestionServiceClientUtility.IngestionServiceClient client =
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

        } finally {
            // make sure we always unlock by using finally
            writeLock.unlock();
        }
    }

    public void handleSubscribeDataResult(
            String pvName,
            SubscribeDataResponse.SubscribeDataResult result
    ) {
        // acquire read lock since method will be called from different threads handling grpc requests/responses
        readLock.lock();

        // get list of EventMonitors that use data for specified PV
        List<EventMonitor> pvEventMonitorList = null;
        try {
            pvEventMonitorList = pvMonitors.get(pvName);
        } finally {
            // make sure we always unlock by using finally, release lock before invoking EventMonitor processing
            readLock.unlock();
        }
        if (pvEventMonitorList != null) {
            logger.error("no EventMonitors found for pvName: {}", pvName);
            return;
        }

        // sanity check that result data is for the specified PV
        for (DataColumn dataColumn : result.getDataColumnsList()) {
            if (dataColumn.getName() != pvName) {
                logger.error("result DataColumn.name: {} mismatch expected pvName: {}",
                        dataColumn.getName(),
                        pvName);
                return;
            }
        }

        // invoke each EventMonitor that uses specified PV
        for (final EventMonitor eventMonitor : pvEventMonitorList) {
            eventMonitor.handleSubscribeDataResult(result);
        }
    }

    public void cancelEventMonitor(EventMonitor eventMonitor) {
        // TODO
    }
}
