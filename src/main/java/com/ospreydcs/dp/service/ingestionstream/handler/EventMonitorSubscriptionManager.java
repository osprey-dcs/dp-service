package com.ospreydcs.dp.service.ingestionstream.handler;

import com.ospreydcs.dp.grpc.v1.common.DataColumn;
import com.ospreydcs.dp.grpc.v1.common.SerializedDataColumn;
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

public class EventMonitorSubscriptionManager {

    // static variables
    private static final Logger logger = LogManager.getLogger();

    // instance variables
    private final IngestionStreamHandler handler;
    private final Map<String, SubscribeDataUtility.SubscribeDataCall> pvDataSubscriptions = new HashMap<>();
    private final Map<String, List<EventMonitor>> pvMonitors = new HashMap<>();
    private final ReentrantReadWriteLock rwLock = new ReentrantReadWriteLock();
    private final Lock readLock = rwLock.readLock();
    private final Lock writeLock = rwLock.writeLock();
    private final IngestionServiceClientUtility.IngestionServiceClient ingestionServiceClient;

    public EventMonitorSubscriptionManager(
            IngestionStreamHandler ingestionStreamHandler,
            IngestionServiceClientUtility.IngestionServiceClient ingestionServiceClient
    ) {
        this.handler = ingestionStreamHandler;
        this.ingestionServiceClient = ingestionServiceClient;
    }

    public ResultStatus addEventMonitor(final EventMonitor eventMonitor) {

        logger.debug("addEventMonitor id: {}", eventMonitor.responseObserver.hashCode());

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

        final DpIngestionServiceGrpc.DpIngestionServiceStub stub = this.ingestionServiceClient.newStub();

        // call subscribeData() API method to receive data for specified PV from Ingestion Service
        final StreamObserver<SubscribeDataRequest> requestObserver = stub.subscribeData(responseObserver);
        requestObserver.onNext(SubscribeDataUtility.buildSubscribeDataRequest(List.of(pvName)));

        if ( ! responseObserver.awaitAckLatch()) {
            return new CallSubscribeDataResult(true, null);
        } else {
            if (responseObserver.isError()) {
                return new CallSubscribeDataResult(
                        true,
                        null);
            } else {
                return new CallSubscribeDataResult(
                        false,
                        new SubscribeDataUtility.SubscribeDataCall(requestObserver, responseObserver));
            }
        }
    }

    public void handleSubscribeDataResponse(String pvName, SubscribeDataResponse subscribeDataResponse) {
        switch (subscribeDataResponse.getResultCase()) {
            case EXCEPTIONALRESULT -> {
                logger.trace("received exceptional result for pv: {}", pvName);
                final String errorMsg =
                        "subscribeData() exceptional result msg: "
                                + subscribeDataResponse.getExceptionalResult().getMessage();
                handleExceptionalResult(pvName, errorMsg);
            }
            case ACKRESULT -> {
                logger.trace("received ack result for pv: {}", pvName);
                // nothing to do
                return;
            }
            case SUBSCRIBEDATARESULT -> {
                logger.trace("received subscribeData result for pv: {}", pvName);
                handleSubscribeDataResult(pvName, subscribeDataResponse.getSubscribeDataResult());
                return;
            }
            case RESULT_NOT_SET -> {
                logger.trace("received result not set for pv: {}", pvName);
                final String errorMsg = "subscribeData() empty response for pv: " + pvName;
                handleExceptionalResult(pvName, errorMsg);
            }
        }
    }

    public void handleExceptionalResult(String pvName, String errorMsg) {

        logger.debug("handleExceptionalResult pv: {} message: {}", pvName, errorMsg);

        // Acquire read lock since method will be called from different threads handling grpc requests/responses.
        // The writeLock is re-entrant.  We'll be acquiring it again for each EventMonitor where handleError()
        // calls cancelEventMonitor().
        writeLock.lock();

        try {
            // get list of EventMonitors that use data for specified PV
            List<EventMonitor> pvEventMonitorList = new ArrayList<>(pvMonitors.get(pvName));

            for (EventMonitor eventMonitor : pvEventMonitorList) {
                cancelEventMonitor_(eventMonitor);
                eventMonitor.handleError(errorMsg, false);
            }

        } finally {
            // make sure we always unlock by using finally, release lock before invoking EventMonitor processing
            writeLock.unlock();
        }
    }

    private void handleSubscribeDataResult(
            String pvName,
            SubscribeDataResponse.SubscribeDataResult result
    ) {
        logger.debug("handleSubscribeDataResult pv: {}", pvName);

        // acquire read lock since method will be called from different threads handling grpc requests/responses
        readLock.lock();

        try {
            // get list of EventMonitors that use data for specified PV
            final List<EventMonitor> pvEventMonitorList = pvMonitors.get(pvName);

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

            for (SerializedDataColumn serializedDataColumn : result.getSerializedDataColumnsList()) {
                if (!serializedDataColumn.getName().equals(pvName)) {
                    logger.error("result SerializedDataColumn.name: {} mismatch expected pvName: {}",
                            serializedDataColumn.getName(),
                            pvName);
                    return;
                }
            }

            // Pass data directly to each EventMonitor for individual buffering
            for (EventMonitor eventMonitor : pvEventMonitorList) {
                eventMonitor.handleSubscribeDataResponse(result);
            }
        } finally {
            // make sure we always unlock by using finally, release lock before invoking EventMonitor processing
            readLock.unlock();
        }
    }

    private void cancelEventMonitor_(EventMonitor eventMonitor) {

        // Method assumes that caller has acquired writeLock.

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
    }

    public void cancelEventMonitor(EventMonitor eventMonitor) {

        logger.debug("cancelEventMonitor id: {}", eventMonitor.responseObserver.hashCode());

        // acquire write lock since method will be called from different threads handling grpc requests/responses
        writeLock.lock();
        try {
            cancelEventMonitor_(eventMonitor);

        } finally {
            // make sure we always unlock by using finally
            writeLock.unlock();
        }
    }

    public void shutdown() {
        logger.debug("shutdown");

        // Shutdown all EventMonitors which will handle their own buffer cleanup
        writeLock.lock();
        try {
            for (List<EventMonitor> monitorList : pvMonitors.values()) {
                for (EventMonitor eventMonitor : monitorList) {
                    eventMonitor.requestCancel();
                    eventMonitor.close();
                }
            }
        } finally {
            writeLock.unlock();
        }
    }

}
