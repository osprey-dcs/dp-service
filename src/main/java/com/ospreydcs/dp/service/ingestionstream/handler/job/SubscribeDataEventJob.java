package com.ospreydcs.dp.service.ingestionstream.handler.job;

import com.ospreydcs.dp.grpc.v1.ingestionstream.SubscribeDataEventRequest;
import com.ospreydcs.dp.grpc.v1.ingestionstream.SubscribeDataEventResponse;
import com.ospreydcs.dp.service.common.handler.HandlerJob;
import io.grpc.stub.StreamObserver;

public class SubscribeDataEventJob extends HandlerJob {

    // instance variables
    private final SubscribeDataEventRequest request;
    private final StreamObserver<SubscribeDataEventResponse> responseObserver;

    public SubscribeDataEventJob(
            SubscribeDataEventRequest request,
            StreamObserver<SubscribeDataEventResponse> responseObserver
    ) {
        this.request = request;
        this.responseObserver = responseObserver;
    }

    @Override
    public void execute() {
        // TODO
    }
}
