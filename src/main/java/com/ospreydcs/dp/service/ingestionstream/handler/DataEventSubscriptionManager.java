package com.ospreydcs.dp.service.ingestionstream.handler;

import com.ospreydcs.dp.grpc.v1.common.DataColumn;
import com.ospreydcs.dp.grpc.v1.ingestion.DpIngestionServiceGrpc;
import com.ospreydcs.dp.grpc.v1.ingestion.SubscribeDataRequest;
import com.ospreydcs.dp.grpc.v1.ingestion.SubscribeDataResponse;
import com.ospreydcs.dp.service.common.model.ResultStatus;
import com.ospreydcs.dp.service.ingest.utility.IngestionServiceClientUtility;
import com.ospreydcs.dp.service.ingest.utility.SubscribeDataUtility;
import com.ospreydcs.dp.service.ingestionstream.handler.model.EventMonitorSubscribeDataResponseObserver;
import com.ospreydcs.dp.service.ingestionstream.handler.monitor.EventMonitor;
import io.grpc.stub.StreamObserver;
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
    private final Map<String, SubscribeDataUtility.SubscribeDataCall> pvDataSubscriptions = new HashMap<>();
    private final Map<String, List<EventMonitor>> pvMonitors = new HashMap<>();
    private final ReentrantReadWriteLock rwLock = new ReentrantReadWriteLock();
    private final Lock readLock = rwLock.readLock();
    private final Lock writeLock = rwLock.writeLock();

    public DataEventSubscriptionManager(IngestionStreamHandler ingestionStreamHandler) {
        this.handler = ingestionStreamHandler;
    }

    public ResultStatus addEventMonitor(final EventMonitor eventMonitor) {

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
                    final CallSubscribeDataResult result = callSubscribeData(pvName);
                    if (result.isError()) {
                        return new ResultStatus(true, "time out calling data subscription API");

                    } else {
                        // add entry to map for tracking subscriptions by PV name
                        pvDataSubscriptions.put(pvName, result.subscribeDataCall());
                    }
                }

                // add EventMonitor to list of subscribers for specified pvName
                final List<EventMonitor> pvEventMonitorList =
                        pvMonitors.computeIfAbsent(pvName, k -> new ArrayList<>());
                pvEventMonitorList.add(eventMonitor);
            }

        } finally {
            // make sure we always unlock by using finally
            writeLock.unlock();
        }

        return new ResultStatus(false, "");
    }

    private record CallSubscribeDataResult(
            boolean isError,
            SubscribeDataUtility.SubscribeDataCall subscribeDataCall
    ) {
    }

    private CallSubscribeDataResult callSubscribeData(String pvName) {

        final EventMonitorSubscribeDataResponseObserver responseObserver =
                new EventMonitorSubscribeDataResponseObserver(pvName, this, handler);

        // use singleton ingestion client to create an API stub
        final IngestionServiceClientUtility.IngestionServiceClient client =
                IngestionServiceClientUtility.IngestionServiceClient.getInstance();
        final DpIngestionServiceGrpc.DpIngestionServiceStub stub = client.newStub();

        // call subscribeData() API method to receive data for specified PV from Ingestion Service
        final StreamObserver<SubscribeDataRequest> requestObserver = stub.subscribeData(responseObserver);
        requestObserver.onNext(SubscribeDataUtility.buildSubscribeDataRequest(List.of(pvName)));

        if ( ! responseObserver.awaitAckLatch()) {
            return new CallSubscribeDataResult(true, null);
        } else {
            return new CallSubscribeDataResult(
                    false,
                    new SubscribeDataUtility.SubscribeDataCall(requestObserver, responseObserver));
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
        if (pvEventMonitorList == null) {
            logger.error("no EventMonitors found for pvName: {}", pvName);
            return;
        }

        // sanity check that result data is for the specified PV
        for (DataColumn dataColumn : result.getDataColumnsList()) {
            if (!dataColumn.getName().equals(pvName)) {
                logger.error("result DataColumn.name: {} mismatch expected pvName: {}",
                        dataColumn.getName(),
                        pvName);
                return;
            }
        }

        // invoke each EventMonitor that uses specified PV
        for (EventMonitor eventMonitor : pvEventMonitorList) {
            eventMonitor.handleSubscribeDataResponse(result);
        }
    }

    public void cancelEventMonitor(EventMonitor eventMonitor) {

        // acquire write lock since method will be called from different threads handling grpc requests/responses
        writeLock.lock();
        try {

            // Iterate through EventMonitor's PVs, close calls to subscribeData() if no other EventMonitor uses the PV,
            // and remove entries for EventMonitor from data structures.
            for (String pvName : eventMonitor.getPvNames()) {

                // remove EventMonitor from subscribed list for pvName
                final List<EventMonitor> monitorList = pvMonitors.get(pvName);
                if (monitorList != null) {
                    monitorList.remove(eventMonitor);

                    // if list is empty, cancel the pv data subscription and clean up data structures
                    if (monitorList.isEmpty()) {
                        pvMonitors.remove(pvName);
                        final SubscribeDataUtility.SubscribeDataCall subscribeDataCall = pvDataSubscriptions.get(pvName);
                        if (subscribeDataCall != null) {
                            // close call to subscribeData() API for this PV
                            subscribeDataCall.requestObserver().onCompleted();
                        }
                        pvDataSubscriptions.remove(pvName);
                    }
                }
            }

        } finally {
            // make sure we always unlock by using finally
            writeLock.unlock();
        }
    }

}
