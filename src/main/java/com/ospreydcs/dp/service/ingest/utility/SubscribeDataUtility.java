package com.ospreydcs.dp.service.ingest.utility;

import com.ospreydcs.dp.grpc.v1.ingestion.SubscribeDataRequest;

import java.util.List;

public class SubscribeDataUtility {

    public static SubscribeDataRequest buildSubscribeDataRequest(final List<String> pvNames) {

        final SubscribeDataRequest.NewSubscription subscription = SubscribeDataRequest.NewSubscription.newBuilder()
                .addAllPvNames(pvNames)
                .build();

        final SubscribeDataRequest request = SubscribeDataRequest.newBuilder()
                .setNewSubscription(subscription)
                .build();

        return request;
    }

}
