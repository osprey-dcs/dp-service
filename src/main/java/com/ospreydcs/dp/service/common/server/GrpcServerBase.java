package com.ospreydcs.dp.service.common.server;

import com.ospreydcs.dp.service.common.config.ConfigurationManager;
import com.ospreydcs.dp.service.common.exception.DpRuntimeException;
import com.ospreydcs.dp.service.common.telemetry.DpTelemetry;
import io.grpc.BindableService;
import io.grpc.Grpc;
import io.grpc.InsecureServerCredentials;
import io.grpc.Server;
import io.grpc.ServerBuilder;
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

    /**
     * This server's short service name, used for {@code otel.service.name} and as the
     * {@code dp.service} attribute value (issue #212). Implementations return the matching
     * {@code DpMetrics.SERVICE_*} constant, so that the resource attribute a scrape carries and
     * the attribute the handler metrics carry are the same string.
     */
    protected abstract String getServiceName_();

    /**
     * Port for this server's Prometheus scrape endpoint (issue #212, D7).
     *
     * <p>Each service has its own default because {@code doc/running.md} runs all four on one
     * host; a shared default would mean the second service to start failed to bind and, per
     * {@link DpTelemetry#init}, failed to start at all.
     */
    protected abstract int getMetricsPort_();

    private static int getIncomingMessageSizeLimitBytes() {
        return configMgr().getConfigInteger(
                CFG_KEY_INCOMING_MESSAGE_SIZE_LIMIT_BYTES,
                DEFAULT_INCOMING_MESSAGE_SIZE_LIMIT_BYTES);
    }

    protected void start() throws IOException {

        // Telemetry is initialized before initService_() and that ordering is load-bearing: the
        // Mongo client and the request handlers create their instruments during their own init,
        // and DpMetrics builds each instrument lazily against whatever meter DpTelemetry holds at
        // first use. Initialized afterwards, every instrument created during service init would be
        // bound to the no-op meter and would silently record nothing for the life of the process.
        DpTelemetry.init(getServiceName_(), getMetricsPort_());

        if (!initService_()) {
            // Release the Prometheus port before throwing. In production the JVM is about to exit
            // and the OS would reclaim it anyway, but stopServer() -- the only other place that
            // shuts telemetry down -- is unreachable here: the shutdown hook that calls it is
            // registered further down, past this throw. Without this, an in-process caller that
            // catches the failure and retries would hit a port its own previous attempt still
            // holds, and report a telemetry bind error in place of the real initialization
            // failure.
            DpTelemetry.shutdown();

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

        final ServerBuilder<?> serverBuilder =
                Grpc.newServerBuilderForPort(port, InsecureServerCredentials.create())
                        .addService(serviceImpl)
                        .maxInboundMessageSize(getIncomingMessageSizeLimitBytes())
                        .keepAliveTime(keepAliveTimeSeconds, TimeUnit.SECONDS)
                        .keepAliveTimeout(keepAliveTimeoutSeconds, TimeUnit.SECONDS)
                        .permitKeepAliveTime(permitKeepAliveTime, TimeUnit.SECONDS)
                        .permitKeepAliveWithoutCalls(permitKeepAliveWithoutCalls);

        // Installs gRPC's own server-side metrics (grpc.server.call.duration and friends). This
        // must happen before build(): the interceptors it adds cannot be applied to a built
        // server. The integration-test wrapper calls the same helper on its in-process builder, so
        // what the ITs exercise is the configuration production runs.
        DpTelemetry.configureServerBuilder(serverBuilder);

        try {
            server = serverBuilder.build().start();
        } catch (IOException | RuntimeException e) {
            // Same reasoning as the initService_() branch above: the metrics port is already bound
            // and the shutdown hook that would release it is registered below, past this throw.
            // Without this, an in-process caller that catches and retries hits a metrics port its
            // own previous attempt still holds, and reports a telemetry bind error in place of the
            // real gRPC bind failure.
            DpTelemetry.shutdown();
            throw e;
        }

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

        // Drain the handler before shutting telemetry down. The gRPC server terminating is NOT
        // when in-flight work finishes: finiService_() is where QueueHandlerBase.fini() runs
        // executorService.awaitTermination(), and the jobs still draining there record
        // dp.handler.*, dp.query.* and dp.ingest.* as they complete. Shutting the SDK down first
        // sent every one of those measurements -- and the workers.max gauge close -- into a closed
        // provider, losing exactly the tail of work an operator investigating a shutdown wants.
        finiService_();

        DpTelemetry.shutdown();
    }

    /**
     * Await termination on the main thread since the grpc library uses daemon threads.
     *
     * <p>{@code finiService_()} is called by {@code stopServer()} rather than here, so that the
     * handler drain happens before telemetry shuts down. It is idempotent
     * ({@code QueueHandlerBase.fini()} returns early once {@code shutdownRequested} is set), so
     * the JVM shutdown hook and this path cannot double-drain.
     */
    protected void blockUntilShutdown() throws InterruptedException {
        if (server != null) {
            server.awaitTermination();
        }
        finiService_();
    }

}
