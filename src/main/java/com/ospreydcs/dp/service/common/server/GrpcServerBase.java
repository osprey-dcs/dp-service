package com.ospreydcs.dp.service.common.server;

import com.ospreydcs.dp.service.common.config.ConfigurationManager;
import com.ospreydcs.dp.service.common.grpc.GrpcHealthService;
import com.ospreydcs.dp.service.common.grpc.GrpcHealthService.ServingStatus;
import com.ospreydcs.dp.service.common.grpc.ReadinessGate;
import com.ospreydcs.dp.service.common.handler.QueueHandlerBase;
import com.ospreydcs.dp.service.common.telemetry.DpMetrics;
import com.ospreydcs.dp.service.common.telemetry.DpTelemetry;
import io.grpc.BindableService;
import io.grpc.Grpc;
import io.grpc.InsecureServerCredentials;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.ServerInterceptors;
import io.grpc.ServerServiceDefinition;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * Base of every dp gRPC server: binds the port, serves the gRPC health service, and initializes
 * the service on a background thread (issue #293).
 *
 * <p><b>The port is bound before the service is initialized.</b> Initialization includes the
 * schema migrations, which on a large archive take hours. With nothing listening for that long a
 * Kubernetes probe kills the process that is migrating. So {@link #start()} binds at once, and
 * until initialization succeeds the health service reports {@code NOT_SERVING} and a
 * {@link ReadinessGate} ends every other call with {@code UNAVAILABLE}, saying why.
 *
 * <p><b>A process that cannot initialize must still exit non-zero.</b> Before #254 the failure
 * mode was a server serving from an uninitialized handler. Now it would be a process that is
 * alive, bound and {@code NOT_SERVING} for ever, which a supervisor that does not probe readiness
 * (every deployment outside Kubernetes) would never notice. {@link #run()} is the one place that
 * turns a failed initialization into an exit status; every {@code main()} calls it.
 *
 * <p>The lifecycle is one field under one lock:
 * <ul>
 *   <li>{@code INITIALIZING} → {@code READY}: the init thread, when {@link #initService_()}
 *       returns true. It opens the gate, sets health to {@code SERVING}, and logs.</li>
 *   <li>{@code INITIALIZING} → {@code FAILED}: the init thread, on a false return or anything
 *       thrown. <b>{@code FAILED} is terminal</b>: the shutdown hook leaves it, because
 *       {@code System.exit()} runs the hook and {@link #run()} reads this field to choose the
 *       exit status.</li>
 *   <li>{@code INITIALIZING} or {@code READY} → {@code STOPPING}: {@link #stopServer()}.</li>
 * </ul>
 * {@link #finiService_()} is called by the thread that stops the server only when it found
 * {@code READY}. While {@code INITIALIZING}, the handler is half built and belongs to the init
 * thread, which cleans up after itself if it finishes into {@code STOPPING}.
 */
public abstract class GrpcServerBase {

    /** Where the process is; see the class comment for the transitions. */
    protected enum Lifecycle { NEW, INITIALIZING, READY, FAILED, STOPPING }

    /**
     * The health service name that is {@code SERVING} from the bind until the server stops.
     *
     * <p>It exists because a Kubernetes gRPC probe succeeds only on {@code SERVING}. A gRPC
     * liveness probe with no service name would therefore kill a process for the whole of a
     * migration, which is the incident #293 was filed for. A deployment that wants a gRPC
     * liveness probe names this service; it says only that the process is up and answering.
     */
    public static final String HEALTH_SERVICE_LIVENESS = "liveness";

    /** Exit status of a process whose initialization failed. */
    public static final int EXIT_STATUS_INIT_FAILED = 1;

    /** Exit status after a fatal error, the one {@code -XX:+ExitOnOutOfMemoryError} uses. */
    public static final int EXIT_STATUS_FATAL_ERROR = 3;

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
    private volatile Server server;
    private final BindableService serviceImpl;
    private final ReadinessGate gate = new ReadinessGate(InitializationStatus::describe);
    private volatile GrpcHealthService healthService;
    private String healthServiceName;

    private final Object lifecycleLock = new Object();
    private Lifecycle lifecycle = Lifecycle.NEW; // guarded by lifecycleLock

    private volatile long startNanos;
    // the final initialization time, or -1 until the service is ready
    private volatile long initElapsedNanos = -1;

    private final List<AutoCloseable> gauges = new ArrayList<>(); // guarded by itself

    private volatile Thread initThread;

    public GrpcServerBase(BindableService serviceImpl) {
        this.serviceImpl = serviceImpl;
    }

    protected static ConfigurationManager configMgr() {
        return ConfigurationManager.getInstance();
    }

    // abstract methods

    /**
     * Initializes the service implementation and its handler. Called on the init thread, after
     * the gRPC port is bound (issue #293).
     *
     * <p>Returns a boolean rather than void so that a failure stops the process. Any
     * implementation must return the result of its {@code serviceImpl.init(...)} call rather than
     * swallowing it: a false return, like anything thrown, moves the server to {@code FAILED} and
     * ends the process with a non-zero status through {@link #run()}. An implementation that
     * logged and returned true would leave the gate open in front of an uninitialized handler,
     * the #254 defect.
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

    /**
     * Builds the health service with every status this server reports before it is ready.
     *
     * <p><b>All three statuses are set here, before any server is built</b>, so that there is no
     * moment after the bind in which a readiness probe could pass on a service that has no
     * handler. One method does it so that the order cannot be got wrong at the call site, and the
     * test can read its result without racing a bind.
     *
     * @param serviceName the full name of the gRPC service, from its descriptor
     */
    static GrpcHealthService newHealthService(String serviceName) {
        final GrpcHealthService health = new GrpcHealthService();
        health.setStatus(GrpcHealthService.SERVICE_NAME_ALL_SERVICES, ServingStatus.NOT_SERVING);
        health.setStatus(serviceName, ServingStatus.NOT_SERVING);
        health.setStatus(HEALTH_SERVICE_LIVENESS, ServingStatus.SERVING);
        return health;
    }

    /**
     * Starts the server, blocks until it stops, and ends the process with a non-zero status if
     * initialization failed. Every {@code main()} is this one call.
     *
     * <p>{@code System.exit()}, not a throw out of {@code main()}: a throw ends the process only
     * if no non-daemon thread remains, and an exit status that depends on which threads a failed
     * initialization happened to start is not a contract. The exit runs the shutdown hook, which
     * finds {@code FAILED} and has nothing left to do for the service.
     */
    public void run() throws IOException, InterruptedException {
        start();
        blockUntilShutdown();
        if (getLifecycle() == Lifecycle.FAILED) {
            closeGauges();
            DpTelemetry.shutdown();
            exitProcess(EXIT_STATUS_INIT_FAILED);
        }
    }

    /**
     * Binds the gRPC port, then starts initialization on a background thread and returns.
     *
     * <p>Throws for what fails before that thread starts: the metrics bind and the gRPC bind. A
     * failed initialization does not throw from here; see {@link #run()}.
     */
    protected void start() throws IOException {

        // Telemetry is initialized first, and before initService_() in particular, and that
        // ordering is load-bearing: the Mongo client and the request handlers create their
        // instruments during their own init, and DpMetrics builds each instrument lazily against
        // whatever meter DpTelemetry holds at first use. Initialized afterwards, every instrument
        // created during service init would be bound to the no-op meter and would silently record
        // nothing for the life of the process.
        DpTelemetry.init(getServiceName_(), getMetricsPort_());

        startNanos = System.nanoTime();

        try {
            registerGauges();

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

            // The service goes behind the gate; the health service does not, since it is what
            // answers while the gate is closed.
            final ServerServiceDefinition serviceDefinition = serviceImpl.bindService();
            healthServiceName = serviceDefinition.getServiceDescriptor().getName();
            healthService = newHealthService(healthServiceName);

            final ServerBuilder<?> serverBuilder =
                    Grpc.newServerBuilderForPort(port, InsecureServerCredentials.create())
                            .addService(ServerInterceptors.intercept(serviceDefinition, gate))
                            .addService(healthService.bindService())
                            .maxInboundMessageSize(getIncomingMessageSizeLimitBytes())
                            .keepAliveTime(keepAliveTimeSeconds, TimeUnit.SECONDS)
                            .keepAliveTimeout(keepAliveTimeoutSeconds, TimeUnit.SECONDS)
                            .permitKeepAliveTime(permitKeepAliveTime, TimeUnit.SECONDS)
                            .permitKeepAliveWithoutCalls(permitKeepAliveWithoutCalls);

            // Installs gRPC's own server-side metrics (grpc.server.call.duration and friends).
            // This must happen before build(): the interceptors it adds cannot be applied to a
            // built server. The integration-test wrapper calls the same helper on its in-process
            // builder, so what the ITs exercise is the configuration production runs.
            DpTelemetry.configureServerBuilder(serverBuilder);

            server = serverBuilder.build().start();

            LOGGER.info("Server started, listening on {}; initializing the service", server.getPort());

        } catch (IOException | RuntimeException e) {
            // The metrics port is already bound, and the shutdown hook that would release it is
            // registered below, past this throw. Without this, an in-process caller that catches
            // and retries hits a metrics port its own previous attempt still holds, and reports a
            // telemetry bind error in place of the real gRPC bind failure.
            closeGauges();
            DpTelemetry.shutdown();
            throw e;
        }

        synchronized (lifecycleLock) {
            lifecycle = Lifecycle.INITIALIZING;
        }

        // Registered before initialization starts, so that a SIGTERM during a migration stops the
        // server and telemetry in order. It does not touch the migration: the claim is left to
        // the lease, and the next process takes it over (#293 D4).
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

        // Not a daemon: while it runs, the process is doing its startup work.
        final Thread thread = new Thread(this::initialize, "dp-init-" + getServiceName_());
        thread.setDaemon(false);
        initThread = thread;
        thread.start();
    }

    /**
     * The init thread's body: runs {@link #initService_()} and makes the transition out of
     * {@code INITIALIZING}.
     *
     * <p><b>Nothing may escape this method, and the failure path depends on nothing that can
     * fail.</b> The whole failure signal crosses to {@link #run()} through the server
     * terminating. If logging the failure threw first (it formats an arbitrary exception and
     * allocates, the PR #318 case), this thread would end with the server bound and
     * {@code main()} parked: alive, bound and {@code NOT_SERVING} for ever. So the move to
     * {@code FAILED} and the server shutdown are in a {@code finally}, the log call is under its
     * own catch, and the thread does nothing else on that path: no telemetry shutdown, no
     * {@link #finiService_()}.
     *
     * <p>A fatal error ({@link QueueHandlerBase#isFatal}) halts the process with status 3, as it
     * does on a handler worker. An {@code OutOfMemoryError} is not local to the thread that
     * reports it, so a handoff to another thread and an orderly exit are the wrong things to
     * depend on, and one process should not report the same condition with two statuses.
     */
    private void initialize() {

        boolean initialized = false;
        Throwable failure = null;
        try {
            initialized = initService_();
            if (initialized && becomeReady()) {
                return;
            }
        } catch (Throwable throwable) {
            failure = throwable;
        }

        if (initialized && failure == null) {
            // Initialized, but the server is stopping: the gate stays closed, and what was just
            // built is this thread's to take down, since stopServer() left it alone.
            try {
                LOGGER.info("service initialized while the server was stopping; shutting it down");
                finiService_();
            } catch (Throwable throwable) {
                if (QueueHandlerBase.isFatal(throwable)) {
                    haltProcess(EXIT_STATUS_FATAL_ERROR);
                }
            }
            return;
        }

        final boolean fatal = failure != null && QueueHandlerBase.isFatal(failure);
        try {
            if (fatal) {
                System.err.println("fatal error during service initialization; halting");
            }
            LOGGER.error(
                    "service initialization failed{}; the process will exit. See the preceding "
                            + "log entries for the cause.",
                    failure == null ? "" : ": " + failure, failure);
        } catch (Throwable reportingFailure) {
            // nothing more can be reported; what follows is what matters
        } finally {
            if (fatal) {
                haltProcess(EXIT_STATUS_FATAL_ERROR);
            } else {
                failInitialization();
            }
        }
    }

    /**
     * Opens the service, if the server is still initializing.
     *
     * @return false if the server is stopping, in which case nothing was opened
     */
    private boolean becomeReady() {
        final long elapsedNanos;
        synchronized (lifecycleLock) {
            if (lifecycle != Lifecycle.INITIALIZING) {
                return false;
            }
            // In this order: a call must not be let in before the gate's own state says so, and
            // health must not say SERVING while calls are still refused.
            gate.open();
            healthService.setStatus(
                    GrpcHealthService.SERVICE_NAME_ALL_SERVICES, ServingStatus.SERVING);
            healthService.setStatus(healthServiceName, ServingStatus.SERVING);
            elapsedNanos = System.nanoTime() - startNanos;
            initElapsedNanos = elapsedNanos;
            lifecycle = Lifecycle.READY;
        }
        LOGGER.info(
                "service ready after {} s of initialization",
                String.format("%.1f", DpMetrics.nanosToSeconds(elapsedNanos)));
        return true;
    }

    /**
     * Moves to {@code FAILED} and stops the server, which is what releases {@link #run()}. If the
     * server is already stopping the JVM is on its way down: the state stays {@code STOPPING},
     * and nothing here may call {@code System.exit()}, which blocks for ever when called during
     * shutdown.
     */
    private void failInitialization() {
        final Server toShutDown;
        synchronized (lifecycleLock) {
            if (lifecycle != Lifecycle.INITIALIZING) {
                return;
            }
            lifecycle = Lifecycle.FAILED;
            toShutDown = server;
        }
        toShutDown.shutdown();
    }

    /**
     * Stops the server. Called by the shutdown hook, and directly by tests.
     *
     * <p>Safe in every state, including while the service is still initializing: then the handler
     * is half built and belongs to the init thread, so {@link #finiService_()} is not called here.
     * The JVM halts when the hooks finish, whatever the init thread is doing, and a migration it
     * was running is left to the lease.
     */
    protected void stopServer() throws InterruptedException {

        final Lifecycle found;
        synchronized (lifecycleLock) {
            found = lifecycle;
            if (found == Lifecycle.INITIALIZING || found == Lifecycle.READY) {
                lifecycle = Lifecycle.STOPPING;
            }
        }

        // First, so that a draining process leaves the load balancer's endpoints while it still
        // has a handler to drain into. This sets every name NOT_SERVING, "liveness" included; a
        // stopping process has no use for a liveness probe.
        final GrpcHealthService health = healthService;
        if (health != null) {
            health.enterTerminalState();
        }

        final Server toStop = server;
        if (toStop != null) {
            toStop.shutdown().awaitTermination(TIMEOUT_TERMINATION_SECS, TimeUnit.SECONDS);
        }

        // Drain the handler before shutting telemetry down. The gRPC server terminating is NOT
        // when in-flight work finishes: finiService_() is where QueueHandlerBase.fini() runs
        // executorService.awaitTermination(), and the jobs still draining there record
        // dp.handler.*, dp.query.* and dp.ingest.* as they complete. Shutting the SDK down first
        // sent every one of those measurements -- and the workers.max gauge close -- into a closed
        // provider, losing exactly the tail of work an operator investigating a shutdown wants.
        // This is also the only place the handler is stopped on a shutdown: it has no hook of its
        // own (#293), so it stops after the server has drained, not beside it.
        if (found == Lifecycle.READY) {
            finiService_();
        }

        closeGauges();
        DpTelemetry.shutdown();
    }

    /**
     * Await termination on the main thread since the grpc library uses daemon threads.
     *
     * <p>Returns when the server has terminated: after {@link #stopServer()}, or after a failed
     * initialization shut it down. {@code finiService_()} is called by {@code stopServer()}, so
     * that the handler drain happens before telemetry shuts down; it is called here only for a
     * server that terminated while {@code READY} without {@code stopServer()} having run.
     */
    protected void blockUntilShutdown() throws InterruptedException {
        final Server toAwait = server;
        if (toAwait != null) {
            toAwait.awaitTermination();
        }
        final boolean wasReady;
        synchronized (lifecycleLock) {
            wasReady = lifecycle == Lifecycle.READY;
            if (wasReady) {
                lifecycle = Lifecycle.STOPPING;
            }
        }
        if (wasReady) {
            finiService_();
        }
    }

    /**
     * Ends the process. Overridable so a test can observe the call without ending the test JVM;
     * production code must not override it.
     */
    protected void exitProcess(int status) {
        System.exit(status);
    }

    /**
     * Ends the process after a fatal error on the init thread. {@code halt}, not
     * {@code System.exit}, for the reason {@code QueueHandlerBase.exitOnFatalError()} gives: the
     * JVM cannot be assumed to be working, and {@code -XX:+ExitOnOutOfMemoryError} skips the
     * hooks as well. Overridable for the test only.
     */
    protected void haltProcess(int status) {
        Runtime.getRuntime().halt(status);
    }

    protected Lifecycle getLifecycle() {
        synchronized (lifecycleLock) {
            return lifecycle;
        }
    }

    /** The thread running initialization, or null before {@link #start()}; for the test. */
    Thread getInitThread() {
        return initThread;
    }

    /** True once the gate is open. */
    protected boolean isGateOpen() {
        return gate.isOpen();
    }

    /** The port the server is listening on, which differs from the configured one when that is 0. */
    protected int getBoundPort() {
        return server.getPort();
    }

    // The gauges are registered after DpTelemetry.init() and before the server is built, and
    // closed before DpTelemetry.shutdown().
    private void registerGauges() {
        final String serviceName = getServiceName_();
        synchronized (gauges) {
            gauges.add(DpMetrics.registerServiceReady(
                    serviceName, () -> getLifecycle() == Lifecycle.READY));
            gauges.add(DpMetrics.registerServiceInitElapsed(serviceName, this::initElapsedSeconds));
            gauges.add(DpMetrics.registerSchemaMigrationActive(
                    serviceName,
                    () -> InitializationStatus.current().phase()
                            == InitializationStatus.Phase.MIGRATING));
            gauges.add(DpMetrics.registerSchemaMigrationTakeoverEnabled(
                    serviceName, () -> InitializationStatus.current().takeoverEnabled()));
        }
    }

    private double initElapsedSeconds() {
        final long finalNanos = initElapsedNanos;
        return DpMetrics.nanosToSeconds(finalNanos >= 0 ? finalNanos : System.nanoTime() - startNanos);
    }

    private void closeGauges() {
        synchronized (gauges) {
            for (AutoCloseable gauge : gauges) {
                try {
                    gauge.close();
                } catch (Exception ex) {
                    LOGGER.warn("error closing an initialization gauge: {}", ex.getMessage(), ex);
                }
            }
            gauges.clear();
        }
    }

}
