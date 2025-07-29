package com.ospreydcs.dp.client;

import io.grpc.ManagedChannel;

public class ServiceApiClientBase {

    // instance variables
    protected final ManagedChannel channel;

    public ServiceApiClientBase(ManagedChannel channel) {
        this.channel = channel;
    }

}
