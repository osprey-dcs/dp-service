package com.ospreydcs.dp.service.inprocess;

import com.ospreydcs.dp.client.mongo.MongoDemoClient;
import io.grpc.BindableService;
import io.grpc.ManagedChannel;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import io.grpc.testing.GrpcCleanupRule;

import java.io.IOException;

import static org.junit.Assert.fail;

public abstract class InprocessServiceBase<T extends BindableService> {

    // common instance variables
    protected MongoDemoClient mongoClient;
    protected T service;
    protected T serviceMock;
    protected ManagedChannel channel;

    protected abstract boolean initService();
    protected abstract void finiService();
    protected abstract T createServiceMock(T service);

    public void init(MongoDemoClient mongoClient) {
        this.mongoClient = mongoClient;

        if (!initService()) {
            fail(getServiceName() + ".init failed");
        }

        serviceMock = createServiceMock(service);

        // Generate a unique in-process server name.
        String serverName = InProcessServerBuilder.generateName();

        // Create a server, add service, start, and register for automatic graceful shutdown.
        try {
            getGrpcCleanupRule().register(InProcessServerBuilder
                    .forName(serverName).directExecutor().addService(serviceMock).build().start());
        } catch (IOException e) {
            fail("IOException creating grpc server");
        }

        // Create a client channel and register for automatic graceful shutdown.
        channel = getGrpcCleanupRule().register(
                InProcessChannelBuilder.forName(serverName).directExecutor().build());
    }

    public void fini() {
        finiService();
        service = null;
        serviceMock = null;
        channel = null;
    }

    public ManagedChannel getChannel() {
        return this.channel;
    }

    protected abstract GrpcCleanupRule getGrpcCleanupRule();
    protected abstract String getServiceName();

}
