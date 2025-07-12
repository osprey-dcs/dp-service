package com.ospreydcs.dp.service.ingestionstream.handler.interfaces;

import com.ospreydcs.dp.grpc.v1.ingestionstream.SubscribeDataEventRequest;
import com.ospreydcs.dp.grpc.v1.ingestionstream.SubscribeDataEventResponse;
import com.ospreydcs.dp.service.common.model.ResultStatus;
import com.ospreydcs.dp.service.ingestionstream.handler.monitor.EventMonitor;
import io.grpc.stub.StreamObserver;

public interface IngestionStreamHandlerInterface
{
    boolean init();
    boolean fini();
    boolean start();
    boolean stop();
    
    EventMonitor handleSubscribeDataEvent(
            SubscribeDataEventRequest request,
            StreamObserver<SubscribeDataEventResponse> responseObserver);

    void cancelDataEventSubscriptions(EventMonitor eventMonitor);

    ResultStatus addEventMonitorSubscription(EventMonitor eventMonitor);
}
