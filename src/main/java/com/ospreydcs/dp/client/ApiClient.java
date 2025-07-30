package com.ospreydcs.dp.client;

import io.grpc.ManagedChannel;

public class ApiClient {

    public final IngestionClient ingestionClient;

    public ApiClient(
            ManagedChannel ingestionChannel
    ) {
        this.ingestionClient = new IngestionClient(ingestionChannel);
    }

    public boolean init() {
        return true;
    }

    public void fini() {
        
    }
}
