package com.ospreydcs.dp.service.ingest.handler.mongo;

import com.ospreydcs.dp.grpc.v1.common.DataColumn;
import com.ospreydcs.dp.grpc.v1.common.DataTimestamps;
import com.ospreydcs.dp.grpc.v1.common.SerializedDataColumn;
import com.ospreydcs.dp.grpc.v1.ingestion.IngestDataRequest;
import com.ospreydcs.dp.service.ingest.model.SourceMonitor;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.*;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class SourceMonitorManager {

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

        logger.debug("fini");

        readLock.lock();

        try {
            // create a set of all SourceMonitors, eliminating duplicates for subscriptions to multiple PVs
            Set<SourceMonitor> sourceMonitors = new HashSet<>();
            for (List<SourceMonitor> monitorList : subscriptionMap.values()) {
                sourceMonitors.addAll(monitorList);
            }

            // close each response stream in set
            for (SourceMonitor monitor : sourceMonitors) {
                monitor.requestShutdown();
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
    public void addMonitor(SourceMonitor monitor) {

        writeLock.lock();
        try {
            // use try...finally to make sure we unlock

            for (String pvName : monitor.pvNames) {
                List<SourceMonitor> sourceMonitors = subscriptionMap.computeIfAbsent(pvName, k -> new ArrayList<>());
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

            // publish regular DataColumns in request that have subscribers
            for (DataColumn requestDataColumn : request.getIngestionDataFrame().getDataColumnsList()) {

                final String pvName = requestDataColumn.getName();
                final List<SourceMonitor> sourceMonitors = subscriptionMap.get(pvName);
                if (sourceMonitors != null) {
                    // publish data via monitors
                    final List<DataColumn> responseDataColumns = List.of(requestDataColumn);
                    for (SourceMonitor monitor : sourceMonitors) {
                        // publish data to subscriber if response stream is active
                        monitor.publishDataColumns(pvName, requestDataTimestamps, responseDataColumns);
                    }
                }
            }

            // publish SerializedDataColumns in request that have subscribers
            for (SerializedDataColumn requestSerializedColumn : request.getIngestionDataFrame().getSerializedDataColumnsList()) {

                final String pvName = requestSerializedColumn.getName();
                final List<SourceMonitor> sourceMonitors = subscriptionMap.get(pvName);
                if (sourceMonitors != null) {
                    // publish data via monitors
                    final List<SerializedDataColumn> responseSerializedColumns = List.of(requestSerializedColumn);
                    for (SourceMonitor monitor : sourceMonitors) {
                        // publish data to subscriber if response stream is active
                        monitor.publishSerializedDataColumns(pvName, requestDataTimestamps, responseSerializedColumns);
                    }
                }
            }

        } finally {
            readLock.unlock();
        }
    }

    /**
     * Remove all subscriptions from map for specified SourceMonitor, and then request shutdown.
     * We use a write lock for thread safety between calling threads
     * (e.g., threads handling registration of subscriptions).
     */
    public void terminateMonitor(SourceMonitor monitor) {

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

        monitor.requestShutdown();
    }
}
