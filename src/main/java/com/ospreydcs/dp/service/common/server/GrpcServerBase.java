package com.ospreydcs.dp.service.common.server;

import com.ospreydcs.dp.service.common.config.ConfigurationManager;
import com.ospreydcs.dp.service.common.exception.DpRuntimeException;
import io.grpc.BindableService;
import io.grpc.Grpc;
import io.grpc.InsecureServerCredentials;
import io.grpc.Server;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

public abstract class GrpcServerBase {

    // constants
    private static final int TIMEOUT_TERMINATION_SECS = 30;
    private static final String CFG_KEY_INCOMING_MESSAGE_SIZE_LIMIT_BYTES = "GrpcServer.incomingMessageSizeLimitBytes";
    private static final int DEFAULT_INCOMING_MESSAGE_SIZE_LIMIT_BYTES = 4_096_000;
    private static final String CFG_KEY_SERVER_KEEP_ALIVE_TIME_SECONDS = "GrpcServer.keepAliveTimeSeconds";
    private static final int DEFAULT_SERVER_KEEP_ALIVE_TIME_SECONDS = 60;
    private static final String CFG_KEY_SERVER_KEEP_ALIVE_TIMEOUT_SECONDS = "GrpcServer.keepAliveTimeoutSeconds";
    private static final int DEFAULT_SERVER_KEEP_ALIVE_TIMEOUT_SECONDS = 20;
    private static final String CFG_KEY_SERVER_PERMIT_KEEP_ALIVE_TIME_SECONDS = "GrpcServer.permitKeepAliveTimeSeconds";
    private static final int DEFAULT_SERVER_PERMIT_KEEP_ALIVE_TIME_SECONDS = 30;
    private static final String CFG_KEY_SERVER_PERMIT_KEEP_ALIVE_WITHOUT_CALLS = "GrpcServer.permitKeepAliveWithoutCalls";
    private static final boolean DEFAULT_SERVER_PERMIT_KEEP_ALIVE_WITHOUT_CALLS = true;

    // static variables
    private static final Logger LOGGER = LogManager.getLogger();

    // instance variables
    private Server server;
    private final BindableService serviceImpl;

    public GrpcServerBase(BindableService serviceImpl) {
        this.serviceImpl = serviceImpl;
    }

    protected static ConfigurationManager configMgr() {
        return ConfigurationManager.getInstance();
    }

    // abstract methods

    /**
     * Initializes the service implementation and its handler.
     *
     * <p>Returns a boolean rather than void so that a failure actually stops startup. Before this
     * returned {@code void} and implementations logged and returned, which exited only
     * {@code initService_()} — {@link #start()} then bound the port and served requests against an
     * uninitialized handler. Any implementation must return the result of its
     * {@code serviceImpl.init(...)} call rather than swallowing it.
     *
     * @return true if the service is ready to serve requests
     */
    protected abstract boolean initService_();
    protected abstract void finiService_();
    protected abstract int getPort_();

    private static int getIncomingMessageSizeLimitBytes() {
        return configMgr().getConfigInteger(
                CFG_KEY_INCOMING_MESSAGE_SIZE_LIMIT_BYTES,
                DEFAULT_INCOMING_MESSAGE_SIZE_LIMIT_BYTES);
    }

    protected void start() throws IOException {

        if (!initService_()) {
            // Throw rather than return. main() calls start() then blockUntilShutdown(), and a
            // silent return would leave `server` null, so blockUntilShutdown() falls straight
            // through to finiService_() and the process exits 0 — a supervisor would read a failed
            // migration or a failed database connection as a clean shutdown and never alert.
            throw new DpRuntimeException(
                    "service initialization failed; not starting the server. See the preceding log "
                            + "entries for the cause.");
        }

        int port = getPort_();

        int keepAliveTimeSeconds = configMgr().getConfigInteger(
                CFG_KEY_SERVER_KEEP_ALIVE_TIME_SECONDS,
                DEFAULT_SERVER_KEEP_ALIVE_TIME_SECONDS
        );
        int keepAliveTimeoutSeconds = configMgr().getConfigInteger(
                CFG_KEY_SERVER_KEEP_ALIVE_TIMEOUT_SECONDS,
                DEFAULT_SERVER_KEEP_ALIVE_TIMEOUT_SECONDS
        );
        int permitKeepAliveTime = configMgr().getConfigInteger(
                CFG_KEY_SERVER_PERMIT_KEEP_ALIVE_TIME_SECONDS,
                DEFAULT_SERVER_PERMIT_KEEP_ALIVE_TIME_SECONDS
        );
        boolean permitKeepAliveWithoutCalls = configMgr().getConfigBoolean(
                CFG_KEY_SERVER_PERMIT_KEEP_ALIVE_WITHOUT_CALLS,
                DEFAULT_SERVER_PERMIT_KEEP_ALIVE_WITHOUT_CALLS
        );

        server = Grpc.newServerBuilderForPort(port, InsecureServerCredentials.create())
                .addService(serviceImpl)
                .maxInboundMessageSize(getIncomingMessageSizeLimitBytes())
                .keepAliveTime(keepAliveTimeSeconds, TimeUnit.SECONDS)
                .keepAliveTimeout(keepAliveTimeoutSeconds, TimeUnit.SECONDS)
                .permitKeepAliveTime(permitKeepAliveTime, TimeUnit.SECONDS)
                .permitKeepAliveWithoutCalls(permitKeepAliveWithoutCalls)
                .build()
                .start();

        LOGGER.info("Server started, listening on " + port);

        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                // Use stderr here since the logger may have been reset by its JVM shutdown hook.
                System.err.println("*** shutting down gRPC server since JVM is shutting down");
                try {
                    stopServer();
                } catch (InterruptedException e) {
                    e.printStackTrace(System.err);
                }
                System.err.println("*** server shut down");
            }
        });
    }

    protected void stopServer() throws InterruptedException {
        if (server != null) {
            server.shutdown().awaitTermination(TIMEOUT_TERMINATION_SECS, TimeUnit.SECONDS);
        }
    }

    /**
     * Await termination on the main thread since the grpc library uses daemon threads.
     */
    protected void blockUntilShutdown() throws InterruptedException {
        if (server != null) {
            server.awaitTermination();
        }
        finiService_();
    }

}
