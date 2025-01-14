package com.ospreydcs.dp.service.ingest.utility;

import com.ospreydcs.dp.grpc.v1.ingestion.SubscribeDataRequest;

import java.util.List;

public class SubscribeDataUtility {

    public static SubscribeDataRequest buildSubscribeDataRequest(final List<String> pvNames) {
        final SubscribeDataRequest request = SubscribeDataRequest.newBuilder()
                .addAllPvNames(pvNames)
                .build();
        return request;
    }

}
