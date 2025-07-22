package com.ospreydcs.dp.service.ingest.model;

import com.ospreydcs.dp.grpc.v1.common.DataColumn;
import com.ospreydcs.dp.grpc.v1.common.DataTimestamps;
import com.ospreydcs.dp.grpc.v1.common.SerializedDataColumn;
import com.ospreydcs.dp.grpc.v1.ingestion.SubscribeDataResponse;
import com.ospreydcs.dp.service.ingest.handler.interfaces.IngestionHandlerInterface;
import com.ospreydcs.dp.service.ingest.service.IngestionServiceImpl;
import com.ospreydcs.dp.service.ingestionstream.service.IngestionStreamServiceImpl;
import io.grpc.stub.ServerCallStreamObserver;
import io.grpc.stub.StreamObserver;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class SourceMonitor {

    // static variables
    private static final Logger logger = LogManager.getLogger();

    // instance variables
    private final IngestionHandlerInterface handler;
    public final List<String> pvNames;
    public final StreamObserver<SubscribeDataResponse> responseObserver;
    public final AtomicBoolean shutdownRequested = new AtomicBoolean(false);
    private final ReentrantReadWriteLock rwLock = new ReentrantReadWriteLock();
    private final Lock readLock = rwLock.readLock();
    private final Lock writeLock = rwLock.writeLock();

    public SourceMonitor(
            IngestionHandlerInterface handler,
            List<String> pvNames,
            StreamObserver<SubscribeDataResponse> responseObserver
    ) {
        this.handler = handler;
        this.pvNames = pvNames;
        this.responseObserver = responseObserver;
    }

    public void publishDataColumns(
            final String pvName,
            final DataTimestamps requestDataTimestamps,
            final List<DataColumn> responseDataColumns
    ) {
        readLock.lock();
        try {
            if (!shutdownRequested.get()) {

                final ServerCallStreamObserver<SubscribeDataResponse> serverCallStreamObserver =
                        (ServerCallStreamObserver<SubscribeDataResponse>) responseObserver;

                if (!serverCallStreamObserver.isCancelled()) {
                    logger.debug(
                            "publishing DataColumns for id: {} pv: {}",
                            responseObserver.hashCode(),
                            pvName);
                    IngestionServiceImpl.sendSubscribeDataResponse(
                            requestDataTimestamps, responseDataColumns, responseObserver);

                } else {
                    logger.trace(
                            "not publishing DataColumns, subscription already closed for id: {} pv: {}",
                            responseObserver.hashCode(),
                            pvName);
                }
            }

        } finally {
            readLock.unlock();
        }

    }

    public void publishSerializedDataColumns(
            final String pvName,
            final DataTimestamps requestDataTimestamps,
            final List<SerializedDataColumn> responseSerializedColumns
    ) {
        readLock.lock();
        try {
            if (!shutdownRequested.get()) {

                final ServerCallStreamObserver<SubscribeDataResponse> serverCallStreamObserver =
                        (ServerCallStreamObserver<SubscribeDataResponse>) responseObserver;

                if (!serverCallStreamObserver.isCancelled()) {
                    logger.debug(
                            "publishing SerializedDataColumns for id: {} pv: {}",
                            responseObserver.hashCode(),
                            pvName);
                    IngestionServiceImpl.sendSubscribeDataResponseSerializedColumns(
                            requestDataTimestamps, responseSerializedColumns, responseObserver);

                } else {
                    logger.trace(
                            "not publishing SerializedDataColumns, subscription already closed for id: {} pv: {}",
                            responseObserver.hashCode(),
                            pvName);
                }
            }

        } finally {
            readLock.unlock();
        }
    }

    public void handleReject(String errorMsg) {

        logger.debug(
                "handleReject id: {} msg: {}",
                responseObserver.hashCode(),
                errorMsg);

        readLock.lock();
        try {
            if (!shutdownRequested.get()) {

                // dispatch error message but don't close response stream with onCompleted()
                IngestionServiceImpl.sendSubscribeDataResponseReject(errorMsg, responseObserver);
            }
        } finally {
            readLock.unlock();
        }
    }

    public void handleError(String errorMsg) {

        logger.debug(
                "handleError id: {} msg: {}",
                responseObserver.hashCode(),
                errorMsg);

        readLock.lock();
        try {
            if (!shutdownRequested.get()) {

                // dispatch error message but don't close response stream with onCompleted()
                IngestionServiceImpl.sendSubscribeDataResponseError(errorMsg, responseObserver);
            }
        } finally {
            readLock.unlock();
        }
    }

    public void requestShutdown() {

        logger.debug("requestShutdown id: {}", responseObserver.hashCode());

        // acquire write lock since method will be called from different threads handling grpc requests/responses
        writeLock.lock();
        try {

            // use AtomicBoolean flag to control cancel, we only need one caller thread cleaning things up
            if (shutdownRequested.compareAndSet(false, true)) {

                // close API response stream
                ServerCallStreamObserver<SubscribeDataResponse> serverCallStreamObserver =
                        (ServerCallStreamObserver<SubscribeDataResponse>) responseObserver;
                if (!serverCallStreamObserver.isCancelled()) {
                    logger.debug(
                            "SourceMonitor.close() calling responseObserver.onCompleted id: {}",
                            responseObserver.hashCode());
                    responseObserver.onCompleted();
                } else {
                    logger.debug(
                            "SourceMonitor.close() responseObserver already closed id: {}",
                            responseObserver.hashCode());
                }
            }

        } finally {
            // make sure we always unlock by using finally
            writeLock.unlock();
        }
    }
}
