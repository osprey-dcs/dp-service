package com.ospreydcs.dp.service.ingestionstream.handler.interfaces;

import com.ospreydcs.dp.grpc.v1.ingestionstream.SubscribeDataEventRequest;
import com.ospreydcs.dp.grpc.v1.ingestionstream.SubscribeDataEventResponse;
import io.grpc.stub.StreamObserver;

public interface IngestionStreamHandlerInterface
{
    boolean init();
    boolean fini();
    boolean start();
    boolean stop();
    
    void handleSubscribeDataEvent(
            SubscribeDataEventRequest request,
            StreamObserver<SubscribeDataEventResponse> responseObserver);

    void cancelDataEventSubscriptions(StreamObserver<SubscribeDataEventResponse> responseObserver);

}
