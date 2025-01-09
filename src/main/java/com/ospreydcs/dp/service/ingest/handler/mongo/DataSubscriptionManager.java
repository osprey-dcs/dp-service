package com.ospreydcs.dp.service.ingest.handler.mongo;

import com.ospreydcs.dp.grpc.v1.common.DataColumn;
import com.ospreydcs.dp.grpc.v1.common.DataTimestamps;
import com.ospreydcs.dp.grpc.v1.ingestion.IngestDataRequest;
import com.ospreydcs.dp.grpc.v1.ingestion.SubscribeDataResponse;
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

    private final Map<String, List<StreamObserver<SubscribeDataResponse>>> subscriptionMap = new HashMap<>();
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
            // create a set of all responseObservers, eliminating duplicates for subscriptions to multiple PVs
            Set<StreamObserver<SubscribeDataResponse>> responseObserverSet = new HashSet<>();
            for (List<StreamObserver<SubscribeDataResponse>> responseObserverList : subscriptionMap.values()) {
                for (StreamObserver<SubscribeDataResponse> responseObserver : responseObserverList) {
                    responseObserverSet.add(responseObserver);
                }
            }

            // close each response stream in set
            for (StreamObserver<SubscribeDataResponse> responseObserver : responseObserverSet) {
                ServerCallStreamObserver<SubscribeDataResponse> serverCallStreamObserver =
                        (ServerCallStreamObserver<SubscribeDataResponse>) responseObserver;
                if (!serverCallStreamObserver.isCancelled()) {
                    logger.debug(
                            "DataSubscriptionManager fini calling responseObserver.onCompleted id: {}",
                            responseObserver.hashCode());
                    responseObserver.onCompleted();
                } else {
                    logger.debug(
                            "DataSubscriptionManager fini responseObserver already closed id: {}",
                            responseObserver.hashCode());
                }
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
    public void addSubscription(List<String> pvNames, StreamObserver<SubscribeDataResponse> observer) {
        writeLock.lock();
        try {
            // use try...finally to make sure we unlock
            for (String pvName : pvNames) {
                List<StreamObserver<SubscribeDataResponse>> observers = subscriptionMap.get(pvName);
                if (observers == null) {
                    observers = new ArrayList<>();
                    subscriptionMap.put(pvName, observers);
                }
                observers.add(observer);
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
                final List<StreamObserver<SubscribeDataResponse>> observers =
                        subscriptionMap.get(requestDataColumn.getName());

                if (observers == null) {
                    // no subscribers for this PV
                    continue;

                } else {
                    final List<DataColumn> responseDataColumns = List.of(requestDataColumn);

                    for (StreamObserver<SubscribeDataResponse> observer : observers) {
                        // publish data to subscriber if response stream is active

                        final ServerCallStreamObserver<SubscribeDataResponse> serverCallStreamObserver =
                                (ServerCallStreamObserver<SubscribeDataResponse>) observer;

                        if (!serverCallStreamObserver.isCancelled()) {
                            logger.debug(
                                    "publishDataSubscriptions publishing data for id: {} pv: {}",
                                    observer.hashCode(),
                                    requestDataColumn.getName());
                            IngestionServiceImpl.sendSubscribeDataResponse(
                                    requestDataTimestamps, responseDataColumns, observer);

                        } else {
                            logger.trace(
                                    "publishDataSubscriptions skipping closed responseObserver id: {} pv: {}",
                                    observer.hashCode(),
                                    requestDataColumn.getName());
                        }
                    }
                }
            }
        } finally {
            readLock.unlock();
        }
    }

    /**
     * Remove all subscriptions from map for specified responseObserver.
     * We use a write lock for thread safety between calling threads
     * (e.g., threads handling registration of subscriptions).
     */
    public void removeSubscriptions(StreamObserver<SubscribeDataResponse> responseObserver) {
        writeLock.lock();
        try {
            // use try...finally to make sure we unlock
            for (var mapEntries : subscriptionMap.entrySet()) {
                final String pvName = mapEntries.getKey();
                final List<StreamObserver<SubscribeDataResponse>> observers = mapEntries.getValue();
                if (observers.contains(responseObserver)) {
                    logger.debug(
                            "removeSubscriptions removing subscription for id: {} pv: {}",
                            responseObserver.hashCode(), pvName);
                    observers.remove(responseObserver);
                }
            }
        } finally {
            writeLock.unlock();
        }
    }
}
