package com.ospreydcs.dp.service.ingest.utility;

import com.ospreydcs.dp.grpc.v1.ingestion.SubscribeDataRequest;
import com.ospreydcs.dp.grpc.v1.ingestion.SubscribeDataResponse;
import io.grpc.stub.StreamObserver;

import java.util.List;

public class SubscribeDataUtility {

    public static class SubscribeDataCall {

        // instance variables
        public final StreamObserver<SubscribeDataRequest> requestObserver;
        public final StreamObserver<SubscribeDataResponse> responseObserver;

        public SubscribeDataCall(
                StreamObserver<SubscribeDataRequest> requestObserver,
                StreamObserver<SubscribeDataResponse> responseObserver
        ) {
            this.requestObserver = requestObserver;
            this.responseObserver = responseObserver;
        }
    }

    public static SubscribeDataRequest buildSubscribeDataRequest(final List<String> pvNames) {

        final SubscribeDataRequest.NewSubscription subscription =
                SubscribeDataRequest.NewSubscription.newBuilder()
                .addAllPvNames(pvNames)
                .build();

        final SubscribeDataRequest request = SubscribeDataRequest.newBuilder()
                .setNewSubscription(subscription)
                .build();

        return request;
    }

    public static SubscribeDataRequest buildSubscribeDataCancelRequest() {

        final SubscribeDataRequest.CancelSubscription subscription =
                SubscribeDataRequest.CancelSubscription.newBuilder()
                .build();

        final SubscribeDataRequest request = SubscribeDataRequest.newBuilder()
                .setCancelSubscription(subscription)
                .build();

        return request;
    }

}
