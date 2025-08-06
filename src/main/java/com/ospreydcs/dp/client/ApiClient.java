package com.ospreydcs.dp.client;

import io.grpc.ManagedChannel;

public class ApiClient {

    // instance variables
    public final IngestionClient ingestionClient;
    public final QueryClient queryClient;

    public ApiClient(
            ManagedChannel ingestionChannel,
            ManagedChannel queryChannel
    ) {
        this.ingestionClient = new IngestionClient(ingestionChannel);
        this.queryClient = new QueryClient(queryChannel);
    }

    public boolean init() {
        return true;
    }

    public void fini() {
    }
}
