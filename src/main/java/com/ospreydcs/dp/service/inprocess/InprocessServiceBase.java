package com.ospreydcs.dp.service.inprocess;

import com.ospreydcs.dp.client.MongoInterface;
import io.grpc.BindableService;
import io.grpc.ManagedChannel;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import io.grpc.testing.GrpcCleanupRule;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;

public abstract class InprocessServiceBase<T extends BindableService> {

    // static variables
    private static final Logger logger = LogManager.getLogger();

    // common instance variables
    protected MongoInterface mongoClient;
    protected T service;
    protected T serviceMock;
    protected ManagedChannel channel;

    protected abstract boolean initService();
    protected abstract void finiService();
    protected abstract T createServiceMock(T service);

    public boolean init(MongoInterface mongoClient) {

        this.mongoClient = mongoClient;

        if (!initService()) {
            logger.error("initService() failed for: {}", getServiceName());
            return false;
        }

        serviceMock = createServiceMock(service);

        // Generate a unique in-process server name.
        String serverName = InProcessServerBuilder.generateName();

        // Create a server, add service, start, and register for automatic graceful shutdown.
        try {
            getGrpcCleanupRule().register(InProcessServerBuilder
                    .forName(serverName).directExecutor().addService(serviceMock).build().start());
        } catch (IOException e) {
            logger.error("IOException creating grpc server for: {}", getServiceName());
            return false;
        }

        // Create a client channel and register for automatic graceful shutdown.
        channel = getGrpcCleanupRule().register(
                InProcessChannelBuilder.forName(serverName).directExecutor().build());

        return true;
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
