package com.ospreydcs.dp.service.ingest.handler.mongo;

import com.ospreydcs.dp.grpc.v1.common.DataColumn;
import com.ospreydcs.dp.grpc.v1.common.DataTimestamps;
import com.ospreydcs.dp.grpc.v1.ingestion.IngestDataRequest;
import com.ospreydcs.dp.grpc.v1.ingestion.SubscribeDataResponse;
import com.ospreydcs.dp.service.ingest.model.SourceMonitor;
import com.ospreydcs.dp.service.ingest.service.IngestionServiceImpl;
import io.grpc.stub.ServerCallStreamObserver;
import io.grpc.stub.StreamObserver;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.*;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class DataSubscriptionManager {

    // static variables
    private static final Logger logger = LogManager.getLogger();

    // instance variables
    private final Map<String, List<SourceMonitor>> subscriptionMap = new HashMap<>();
    private final ReentrantReadWriteLock rwLock = new ReentrantReadWriteLock();
    private final Lock readLock = rwLock.readLock();
    private final Lock writeLock = rwLock.writeLock();


    public boolean init() {
        return true;
    }

    public boolean fini() {

        logger.debug("DataSubscriptionManager fini");

        readLock.lock();

        try {
            // create a set of all SourceMonitors, eliminating duplicates for subscriptions to multiple PVs
            Set<SourceMonitor> sourceMonitors = new HashSet<>();
            for (List<SourceMonitor> monitorList : subscriptionMap.values()) {
                for (SourceMonitor monitor : monitorList) {
                    sourceMonitors.add(monitor);
                }
            }

            // close each response stream in set
            for (SourceMonitor monitor : sourceMonitors) {
                monitor.close();
            }

        } finally {
            readLock.unlock();
        }

        return true;
    }

    /**
     * Add a subscription entry to map data structure.  We use a write lock for thread safety between calling threads
     * (e.g., threads handling registration of subscriptions).
     */
    public void addSubscription(SourceMonitor monitor) {

        writeLock.lock();
        try {
            // use try...finally to make sure we unlock

            for (String pvName : monitor.pvNames) {
                List<SourceMonitor> sourceMonitors = subscriptionMap.get(pvName);
                if (sourceMonitors == null) {
                    sourceMonitors = new ArrayList<>();
                    subscriptionMap.put(pvName, sourceMonitors);
                }
                sourceMonitors.add(monitor);
            }

        } finally {
            writeLock.unlock();
        }
    }

    /**
     * Send a response to subscribers of PVs contained in this ingestion request.  We use a read lock for thread safety
     * between calling threads (e.g., workers processing ingestion requests).
     * @param request
     */
    public void publishDataSubscriptions(IngestDataRequest request) {

        readLock.lock();
        try {
            // use try...finally to make sure we unlock

            final DataTimestamps requestDataTimestamps = request.getIngestionDataFrame().getDataTimestamps();
            // iterate request columns and send response to column PV subscribers
            for (DataColumn requestDataColumn : request.getIngestionDataFrame().getDataColumnsList()) {

                final String pvName = requestDataColumn.getName();
                final List<SourceMonitor> sourceMonitors = subscriptionMap.get(pvName);
                if (sourceMonitors != null) {
                    // publish data via monitors
                    final List<DataColumn> responseDataColumns = List.of(requestDataColumn);
                    for (SourceMonitor monitor : sourceMonitors) {
                        // publish data to subscriber if response stream is active
                        monitor.publishData(pvName, requestDataTimestamps, responseDataColumns);
                    }
                }
            }

        } finally {
            readLock.unlock();
        }
    }

    /**
     * Remove all subscriptions from map for specified SourceMonitor.
     * We use a write lock for thread safety between calling threads
     * (e.g., threads handling registration of subscriptions).
     */
    public void removeSubscriptions(SourceMonitor monitor) {

        writeLock.lock();
        try {
            // use try...finally to make sure we unlock

            for (String pvName : monitor.pvNames) {
                List<SourceMonitor> sourceMonitors = subscriptionMap.get(pvName);
                if (sourceMonitors != null) {
                    logger.debug(
                            "removeSubscriptions removing subscription for id: {} pv: {}",
                            monitor.responseObserver.hashCode(), pvName);
                    sourceMonitors.remove(monitor);
                }
            }

        } finally {
            writeLock.unlock();
        }
    }
}
