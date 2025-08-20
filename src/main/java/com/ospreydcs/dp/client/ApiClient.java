package com.ospreydcs.dp.client;

import io.grpc.ManagedChannel;

public class ApiClient {

    // instance variables
    public final IngestionClient ingestionClient;
    public final QueryClient queryClient;
    public final AnnotationClient annotationClient;

    public ApiClient(
            ManagedChannel ingestionChannel,
            ManagedChannel queryChannel,
            ManagedChannel annotationChannel
    ) {
        this.ingestionClient = new IngestionClient(ingestionChannel);
        this.queryClient = new QueryClient(queryChannel);
        this.annotationClient = new AnnotationClient(annotationChannel);
    }

    public boolean init() {
        return true;
    }

    public void fini() {
    }
}
