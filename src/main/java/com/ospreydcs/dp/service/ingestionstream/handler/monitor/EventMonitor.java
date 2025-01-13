package com.ospreydcs.dp.service.ingestionstream.handler.monitor;

import com.ospreydcs.dp.grpc.v1.ingestionstream.SubscribeDataEventRequest;
import com.ospreydcs.dp.grpc.v1.ingestionstream.SubscribeDataEventResponse;
import io.grpc.stub.StreamObserver;

import java.util.List;

public abstract class EventMonitor {

    // instance variables
    private final SubscribeDataEventRequest request;
    private final StreamObserver<SubscribeDataEventResponse> responseObserver;

    public EventMonitor(
            SubscribeDataEventRequest request,
            StreamObserver<SubscribeDataEventResponse> responseObserver
    ) {
        this.request = request;
        this.responseObserver = responseObserver;
    }

    public List<String> getPvNames() {
        return request.getPvNamesList();
    }
}
