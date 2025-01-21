package com.ospreydcs.dp.service.ingest.model;

import com.ospreydcs.dp.grpc.v1.common.DataColumn;
import com.ospreydcs.dp.grpc.v1.common.DataTimestamps;
import com.ospreydcs.dp.grpc.v1.ingestion.SubscribeDataResponse;
import com.ospreydcs.dp.service.ingest.handler.interfaces.IngestionHandlerInterface;
import com.ospreydcs.dp.service.ingest.service.IngestionServiceImpl;
import io.grpc.stub.ServerCallStreamObserver;
import io.grpc.stub.StreamObserver;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

public class SourceMonitor {

    // static variables
    private static final Logger logger = LogManager.getLogger();

    // instance variables
    private final IngestionHandlerInterface handler;
    public final List<String> pvNames;
    public final StreamObserver<SubscribeDataResponse> responseObserver;
    public final AtomicBoolean canceled = new AtomicBoolean(false);

    public SourceMonitor(
            IngestionHandlerInterface handler,
            List<String> pvNames,
            StreamObserver<SubscribeDataResponse> responseObserver
    ) {
        this.handler = handler;
        this.pvNames = pvNames;
        this.responseObserver = responseObserver;
    }

    public void sendAck() {
        IngestionServiceImpl.sendSubscribeDataResponseAck(responseObserver);
    }

    public void close() {

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

    public void publishData(
            final String pvName,
            final DataTimestamps requestDataTimestamps,
            final List<DataColumn> responseDataColumns) {

        final ServerCallStreamObserver<SubscribeDataResponse> serverCallStreamObserver =
                (ServerCallStreamObserver<SubscribeDataResponse>) responseObserver;

        if (!serverCallStreamObserver.isCancelled()) {
            logger.debug(
                    "publishDataSubscriptions publishing data for id: {} pv: {}",
                    responseObserver.hashCode(),
                    pvName);
            IngestionServiceImpl.sendSubscribeDataResponse(
                    requestDataTimestamps, responseDataColumns, responseObserver);

        } else {
            logger.trace(
                    "publishDataSubscriptions skipping closed responseObserver id: {} pv: {}",
                    responseObserver.hashCode(),
                    pvName);
        }

    }

    public void requestCancel() {
        // use AtomicBoolean flag to control cancel, we only need one caller thread cleaning things up
        if (canceled.compareAndSet(false, true)) {
            handler.removeSourceMonitor(this);
        }
    }
}
