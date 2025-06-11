package com.ospreydcs.dp.service.ingest.utility;

import com.ospreydcs.dp.grpc.v1.ingestion.SubscribeDataRequest;
import com.ospreydcs.dp.grpc.v1.ingestion.SubscribeDataResponse;
import io.grpc.stub.StreamObserver;

import java.util.List;

public class SubscribeDataUtility {

    /**
     * @param requestObserver instance variables
     */
    public record SubscribeDataCall(StreamObserver<SubscribeDataRequest> requestObserver,
                                    StreamObserver<SubscribeDataResponse> responseObserver) {

    }

    public static SubscribeDataRequest buildSubscribeDataRequest(final List<String> pvNames) {

        final SubscribeDataRequest.NewSubscription subscription =
                SubscribeDataRequest.NewSubscription.newBuilder()
                .addAllPvNames(pvNames)
                .build();

        return SubscribeDataRequest.newBuilder()
                .setNewSubscription(subscription)
                .build();
    }

    public static SubscribeDataRequest buildSubscribeDataCancelRequest() {

        final SubscribeDataRequest.CancelSubscription subscription =
                SubscribeDataRequest.CancelSubscription.newBuilder()
                .build();

        return SubscribeDataRequest.newBuilder()
                .setCancelSubscription(subscription)
                .build();
    }

}
