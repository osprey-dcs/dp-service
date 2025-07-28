package com.ospreydcs.dp.service.ingestionstream.handler.monitor;

import com.ospreydcs.dp.grpc.v1.ingestion.DpIngestionServiceGrpc;
import com.ospreydcs.dp.grpc.v1.ingestion.SubscribeDataRequest;
import com.ospreydcs.dp.service.common.model.ResultStatus;
import com.ospreydcs.dp.service.ingest.utility.IngestionServiceClientUtility;
import com.ospreydcs.dp.service.ingest.utility.SubscribeDataUtility;
import com.ospreydcs.dp.service.ingestionstream.handler.IngestionStreamHandler;
import com.ospreydcs.dp.service.ingestionstream.handler.model.EventMonitorSubscribeDataResponseObserver;
import io.grpc.stub.StreamObserver;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.List;

public class SubscribeDataCallManager {

    // static variables
    private static final Logger logger = LogManager.getLogger();

    // instance variables
    private final EventMonitor eventMonitor;
    private final IngestionStreamHandler handler;
    private IngestionServiceClientUtility.IngestionServiceGrpcClient ingestionServiceGrpcClient;
    private SubscribeDataUtility.SubscribeDataCall subscribeDataCall = null;

    private record CallSubscribeDataResult(
            boolean isError,
            String errorMessage,
            SubscribeDataUtility.SubscribeDataCall subscribeDataCall
    ) {
    }

    public SubscribeDataCallManager(
            EventMonitor eventMonitor,
            IngestionStreamHandler handler,
            IngestionServiceClientUtility.IngestionServiceGrpcClient ingestionServiceGrpcClient
    ) {
        this.eventMonitor = eventMonitor;
        this.handler = handler;
        this.ingestionServiceGrpcClient = ingestionServiceGrpcClient;
    }

    private CallSubscribeDataResult callSubscribeData(List<String> pvNames) {

        // build SubscribeDataRequest for EventMonitor PVs
        final SubscribeDataRequest request =
                SubscribeDataUtility.buildSubscribeDataRequest(pvNames);

        final EventMonitorSubscribeDataResponseObserver responseObserver =
                new EventMonitorSubscribeDataResponseObserver(eventMonitor, handler);

        final DpIngestionServiceGrpc.DpIngestionServiceStub stub = this.ingestionServiceGrpcClient.newStub();

        // call subscribeData() API method to receive data for specified PV from Ingestion Service
        final StreamObserver<SubscribeDataRequest> requestObserver = stub.subscribeData(responseObserver);
        requestObserver.onNext(request);

        if ( ! responseObserver.awaitAckLatch()) {
            return new CallSubscribeDataResult(
                    true,
                    "time out waiting for subscribeData() ack response",
                    null);
        } else {
            if (responseObserver.isError()) {
                return new CallSubscribeDataResult(
                        true,
                        responseObserver.getErrorMessage(),
                        null);
            } else {
                return new CallSubscribeDataResult(
                        false,
                        "",
                        new SubscribeDataUtility.SubscribeDataCall(requestObserver, responseObserver));
            }
        }
    }

    public ResultStatus initiateSubscription() {

        logger.debug("initiating subscribeData() subscription for monitor: {}", eventMonitor.hashCode());

        // create observer for subscribeData() API method response stream
        final CallSubscribeDataResult result = callSubscribeData(new ArrayList<>(eventMonitor.getPvNames()));
        if (result.isError()) {
            return new ResultStatus(true, result.errorMessage);

        } else {
            this.subscribeDataCall = result.subscribeDataCall;
            return new ResultStatus(false, "");
        }
    }

    public void terminateSubscription() {

        logger.debug("terminating subscribeData() subscription for monitor: {}", eventMonitor.hashCode());

        // terminate subscribeData() subscription
        if (this.subscribeDataCall != null) {
            this.subscribeDataCall.requestObserver().onCompleted();
        }
    }

}
