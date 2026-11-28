package com.ospreydcs.dp.service.common.server;

import com.ospreydcs.dp.grpc.v1.query.DpQueryServiceGrpc;
import com.ospreydcs.dp.grpc.v1.query.QueryDataRequest;
import com.ospreydcs.dp.grpc.v1.query.QueryDataResponse;
import com.ospreydcs.dp.service.common.grpc.GrpcHealthService;
import com.ospreydcs.dp.service.common.grpc.GrpcHealthService.ServingStatus;
import com.ospreydcs.dp.service.common.telemetry.DpMetrics;
import com.ospreydcs.dp.service.common.telemetry.DpTelemetry;
import io.grpc.CallOptions;
import io.grpc.Grpc;
import io.grpc.InsecureChannelCredentials;
import io.grpc.ManagedChannel;
import io.grpc.Server;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import io.grpc.stub.ClientCalls;
import io.grpc.stub.StreamObserver;
import io.opentelemetry.sdk.OpenTelemetrySdk;
import io.opentelemetry.sdk.metrics.SdkMeterProvider;
import io.opentelemetry.sdk.metrics.data.MetricData;
import io.opentelemetry.sdk.testing.exporter.InMemoryMetricReader;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.net.ServerSocket;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Drives the real {@link GrpcServerBase#start()} (issue #293): the port is bound before the
 * service is initialized, the gate and the health service say so until it is, a failed
 * initialization ends the process non-zero, and a stop during initialization is safe.
 *
 * <p>The server under test is a subclass whose {@code initService_()} blocks on a latch and then
 * returns, or throws, what the test told it to. No migration is involved; the wait policy is
 * covered in {@code SchemaMigrationLeaseTest}.
 *
 * <p><b>The cases split by how telemetry is set up</b>, because {@code DpTelemetry.init()} does
 * nothing once {@code initForTest()} has run. The cases that read a gauge install an in-memory
 * reader, and {@code start()} then binds no metrics port. The cases that assert the ports are
 * released install nothing, so that {@code start()} binds a real metrics port. Asserting port
 * release after {@code initForTest()} would pass without a metrics port ever having been bound.
 */
public class GrpcServerBaseStartupTest {

    private static final String SERVICE_FULL_NAME = DpQueryServiceGrpc.SERVICE_NAME;

    private InMemoryMetricReader metricReader;
    private OpenTelemetrySdk telemetrySdk;
    private TestServer server;
    private ManagedChannel channel;

    /** Answers the one method the test calls; a call that arrives here passed the gate. */
    private static class TestService extends DpQueryServiceGrpc.DpQueryServiceImplBase {

        final AtomicInteger calls = new AtomicInteger();

        @Override
        public void queryData(QueryDataRequest request, StreamObserver<QueryDataResponse> observer) {
            calls.incrementAndGet();
            observer.onNext(QueryDataResponse.getDefaultInstance());
            observer.onCompleted();
        }
    }

    /** An exception that cannot be printed, as an arbitrary failure's {@code toString()} may not be. */
    private static class UnprintableException extends RuntimeException {

        @Override
        public String toString() {
            throw new IllegalStateException("deliberate failure in toString()");
        }

        @Override
        public String getMessage() {
            throw new IllegalStateException("deliberate failure in getMessage()");
        }
    }

    private static class TestServer extends GrpcServerBase {

        final TestService service;
        final int metricsPort;

        final CountDownLatch initEntered = new CountDownLatch(1);
        final CountDownLatch release = new CountDownLatch(1);
        volatile boolean initResult = true;
        volatile Throwable initFailure = null;

        final AtomicInteger finiCalls = new AtomicInteger();
        final AtomicReference<String> finiThread = new AtomicReference<>();
        final AtomicReference<Integer> exitStatus = new AtomicReference<>();
        final CountDownLatch exited = new CountDownLatch(1);
        final AtomicReference<Integer> haltStatus = new AtomicReference<>();
        final CountDownLatch halted = new CountDownLatch(1);

        TestServer(TestService service, int metricsPort) {
            super(service);
            this.service = service;
            this.metricsPort = metricsPort;
        }

        @Override
        protected boolean initService_() {
            initEntered.countDown();
            try {
                assertTrue("the test never released initService_()", release.await(60, TimeUnit.SECONDS));
            } catch (InterruptedException ex) {
                Thread.currentThread().interrupt();
                return false;
            }
            final Throwable failure = initFailure;
            if (failure instanceof Error error) {
                throw error;
            }
            if (failure instanceof RuntimeException runtimeException) {
                throw runtimeException;
            }
            return initResult;
        }

        @Override
        protected void finiService_() {
            finiCalls.incrementAndGet();
            finiThread.set(Thread.currentThread().getName());
        }

        @Override
        protected int getPort_() {
            return 0; // ephemeral; read back through getBoundPort()
        }

        @Override
        protected String getServiceName_() {
            return DpMetrics.SERVICE_QUERY;
        }

        @Override
        protected int getMetricsPort_() {
            return metricsPort;
        }

        @Override
        protected void exitProcess(int status) {
            exitStatus.set(status);
            exited.countDown();
        }

        @Override
        protected void haltProcess(int status) {
            haltStatus.set(status);
            halted.countDown();
        }

        void awaitInitThread() throws InterruptedException {
            getInitThread().join(TimeUnit.SECONDS.toMillis(30));
            assertFalse("the init thread did not end", getInitThread().isAlive());
        }
    }

    @Before
    public void setUp() {
        InitializationStatus.resetForTest();
    }

    @After
    public void tearDown() throws Exception {
        if (channel != null) {
            channel.shutdownNow().awaitTermination(10, TimeUnit.SECONDS);
        }
        if (server != null) {
            server.release.countDown();
            server.stopServer();
        }
        DpTelemetry.resetForTest();
        if (telemetrySdk != null) {
            telemetrySdk.close();
        }
        InitializationStatus.resetForTest();
    }

    private void installMetricReader() {
        metricReader = InMemoryMetricReader.create();
        telemetrySdk = OpenTelemetrySdk.builder()
                .setMeterProvider(SdkMeterProvider.builder().registerMetricReader(metricReader).build())
                .build();
        DpTelemetry.initForTest(telemetrySdk);
    }

    private static int freePort() throws IOException {
        try (ServerSocket socket = new ServerSocket(0)) {
            return socket.getLocalPort();
        }
    }

    private static void assertPortFree(String what, int port) {
        try (ServerSocket socket = new ServerSocket(port)) {
            assertTrue(socket.isBound());
        } catch (IOException ex) {
            fail(what + " port " + port + " is still bound: " + ex.getMessage());
        }
    }

    private TestServer startServer(int metricsPort) throws Exception {
        server = new TestServer(new TestService(), metricsPort);
        server.start();
        assertTrue(server.initEntered.await(30, TimeUnit.SECONDS));
        channel = Grpc.newChannelBuilder(
                "localhost:" + server.getBoundPort(), InsecureChannelCredentials.create()).build();
        return server;
    }

    /** Runs {@code run()}, which blocks until the server stops, on a thread of its own. */
    private Thread runInBackground(TestServer testServer) {
        final Thread thread = new Thread(() -> {
            try {
                testServer.run();
            } catch (IOException | InterruptedException ex) {
                throw new IllegalStateException(ex);
            }
        }, "test-main");
        thread.setDaemon(true);
        thread.start();
        return thread;
    }

    private ServingStatus health(String serviceName) {
        return ClientCalls.blockingUnaryCall(
                channel, GrpcHealthService.CHECK_METHOD,
                CallOptions.DEFAULT.withDeadlineAfter(30, TimeUnit.SECONDS), serviceName);
    }

    private void callService() {
        DpQueryServiceGrpc.newBlockingStub(channel)
                .withDeadlineAfter(30, TimeUnit.SECONDS)
                .queryData(QueryDataRequest.getDefaultInstance());
    }

    /** The value of a gauge's single point, or null if it reported none. */
    private Number gaugeValue(String metricName) {
        for (MetricData metric : metricReader.collectAllMetrics()) {
            if (!metric.getName().equals(metricName)) {
                continue;
            }
            if (!metric.getLongGaugeData().getPoints().isEmpty()) {
                final var points = List.copyOf(metric.getLongGaugeData().getPoints());
                assertEquals(1, points.size());
                assertEquals(
                        "the gauge must carry dp.service and nothing else",
                        1, points.get(0).getAttributes().size());
                assertEquals(
                        DpMetrics.SERVICE_QUERY,
                        points.get(0).getAttributes().get(DpMetrics.ATTR_SERVICE));
                return points.get(0).getValue();
            }
            if (!metric.getDoubleGaugeData().getPoints().isEmpty()) {
                final var points = List.copyOf(metric.getDoubleGaugeData().getPoints());
                assertEquals(1, points.size());
                return points.get(0).getValue();
            }
        }
        return null;
    }

    // ------------------------------------------------------------------
    // bind order, gate, health, gauges
    // ------------------------------------------------------------------

    @Test
    public void testPortIsBoundAndTheServiceRefusesCallsUntilInitializationSucceeds() throws Exception {

        installMetricReader();
        startServer(freePort());

        // while initService_() is blocked
        assertEquals(GrpcServerBase.Lifecycle.INITIALIZING, server.getLifecycle());
        assertEquals(ServingStatus.NOT_SERVING, health(""));
        assertEquals(ServingStatus.NOT_SERVING, health(SERVICE_FULL_NAME));
        assertEquals(
                "liveness must pass during initialization, or a gRPC liveness probe kills the "
                        + "process that is migrating",
                ServingStatus.SERVING, health(GrpcServerBase.HEALTH_SERVICE_LIVENESS));

        try {
            callService();
            fail("a service that has not initialized must refuse the call");
        } catch (StatusRuntimeException ex) {
            assertEquals(Status.Code.UNAVAILABLE, ex.getStatus().getCode());
            assertEquals("service is initializing", ex.getStatus().getDescription());
        }

        InitializationStatus.setMigrating(4, 5);
        try {
            callService();
            fail("a service that has not initialized must refuse the call");
        } catch (StatusRuntimeException ex) {
            assertEquals(
                    "service is initializing: applying schema migration version 4 (target 5)",
                    ex.getStatus().getDescription());
        }
        assertEquals(1L, gaugeValue(DpMetrics.METRIC_SCHEMA_MIGRATION_ACTIVE));

        InitializationStatus.setWaiting();
        try {
            callService();
            fail("a service that has not initialized must refuse the call");
        } catch (StatusRuntimeException ex) {
            assertEquals(
                    "service is initializing: waiting for a schema migration running in another "
                            + "process",
                    ex.getStatus().getDescription());
        }
        assertEquals(0L, gaugeValue(DpMetrics.METRIC_SCHEMA_MIGRATION_ACTIVE));
        InitializationStatus.setStarting();

        assertEquals("no call may reach the service", 0, server.service.calls.get());
        assertEquals(0L, gaugeValue(DpMetrics.METRIC_SERVICE_READY));
        final double elapsedEarly = gaugeValue(DpMetrics.METRIC_SERVICE_INIT_ELAPSED).doubleValue();
        Thread.sleep(50);
        assertTrue(
                "init.elapsed must climb while the service is initializing",
                gaugeValue(DpMetrics.METRIC_SERVICE_INIT_ELAPSED).doubleValue() > elapsedEarly);

        // after it returns true
        server.release.countDown();
        server.awaitInitThread();

        assertEquals(GrpcServerBase.Lifecycle.READY, server.getLifecycle());
        assertTrue(server.isGateOpen());
        assertEquals(ServingStatus.SERVING, health(""));
        assertEquals(ServingStatus.SERVING, health(SERVICE_FULL_NAME));
        assertEquals(ServingStatus.SERVING, health(GrpcServerBase.HEALTH_SERVICE_LIVENESS));
        callService();
        assertEquals(1, server.service.calls.get());
        assertEquals(1L, gaugeValue(DpMetrics.METRIC_SERVICE_READY));

        final double elapsedFinal = gaugeValue(DpMetrics.METRIC_SERVICE_INIT_ELAPSED).doubleValue();
        Thread.sleep(50);
        assertEquals(
                "init.elapsed is the final initialization time once the service is ready",
                elapsedFinal, gaugeValue(DpMetrics.METRIC_SERVICE_INIT_ELAPSED).doubleValue(), 0.0);

        // a stop from READY drains the handler, on the stopping thread
        server.stopServer();
        assertEquals(1, server.finiCalls.get());
        assertEquals(Thread.currentThread().getName(), server.finiThread.get());
    }

    @Test
    public void testTakeoverEnabledGaugeReportsNothingUntilTheCheckHasRun() throws Exception {

        installMetricReader();
        startServer(freePort());

        assertNull(
                "not yet known must not read as refused",
                gaugeValue(DpMetrics.METRIC_SCHEMA_MIGRATION_TAKEOVER_ENABLED));

        InitializationStatus.setTakeoverEnabled(true);
        assertEquals(1L, gaugeValue(DpMetrics.METRIC_SCHEMA_MIGRATION_TAKEOVER_ENABLED));

        InitializationStatus.setTakeoverEnabled(false);
        assertEquals(0L, gaugeValue(DpMetrics.METRIC_SCHEMA_MIGRATION_TAKEOVER_ENABLED));

        // a change of phase must not lose it
        InitializationStatus.setWaiting();
        InitializationStatus.setStarting();
        assertEquals(0L, gaugeValue(DpMetrics.METRIC_SCHEMA_MIGRATION_TAKEOVER_ENABLED));
    }

    @Test
    public void testHealthServiceAsBuiltReportsNotServingBeforeAnyServerExists() throws Exception {

        // Were the statuses set after the bind, a readiness probe could pass on a service with
        // no handler. This reads the health service the production method returns, so no bind
        // is raced.
        final GrpcHealthService health = GrpcServerBase.newHealthService(SERVICE_FULL_NAME);

        final String name = InProcessServerBuilder.generateName();
        final Server healthOnly = InProcessServerBuilder.forName(name).directExecutor()
                .addService(health.bindService()).build().start();
        channel = InProcessChannelBuilder.forName(name).directExecutor().build();
        try {
            assertEquals(ServingStatus.NOT_SERVING, health(""));
            assertEquals(ServingStatus.NOT_SERVING, health(SERVICE_FULL_NAME));
            assertEquals(ServingStatus.SERVING, health(GrpcServerBase.HEALTH_SERVICE_LIVENESS));
        } finally {
            healthOnly.shutdownNow().awaitTermination(10, TimeUnit.SECONDS);
        }
    }

    // ------------------------------------------------------------------
    // a failed initialization exits non-zero
    // ------------------------------------------------------------------

    /** Starts through run() with real telemetry, fails initialization, and returns the ports used. */
    private void assertFailedInitializationExits(boolean result, Throwable failure) throws Exception {

        final int metricsPort = freePort();
        server = new TestServer(new TestService(), metricsPort);
        server.initResult = result;
        server.initFailure = failure;

        final Thread main = runInBackground(server);
        assertTrue(server.initEntered.await(30, TimeUnit.SECONDS));
        final int grpcPort = server.getBoundPort();
        assertTrue("start() must have bound a real metrics port", DpTelemetry.isInitialized());
        try (ServerSocket socket = new ServerSocket(metricsPort)) {
            fail("the metrics port should be bound while the server runs");
        } catch (IOException expected) {
            // bound, as it must be for the release below to mean anything
        }

        server.release.countDown();

        assertTrue(
                "a failed initialization must end the process",
                server.exited.await(30, TimeUnit.SECONDS));
        assertEquals(Integer.valueOf(GrpcServerBase.EXIT_STATUS_INIT_FAILED), server.exitStatus.get());
        main.join(TimeUnit.SECONDS.toMillis(30));
        assertFalse(main.isAlive());

        assertEquals(GrpcServerBase.Lifecycle.FAILED, server.getLifecycle());
        assertFalse(server.isGateOpen());
        assertEquals(
                "the init thread must not take the handler down on the failure path",
                0, server.finiCalls.get());
        assertPortFree("gRPC", grpcPort);
        assertPortFree("metrics", metricsPort);

        // System.exit() runs the shutdown hook. It must leave FAILED alone, since run() chose the
        // exit status from it, and has nothing left to do for the service.
        server.stopServer();
        assertEquals(GrpcServerBase.Lifecycle.FAILED, server.getLifecycle());
        assertEquals(0, server.finiCalls.get());
        assertNull(server.haltStatus.get());
    }

    @Test
    public void testFalseReturnFromInitializationExitsNonZero() throws Exception {
        assertFailedInitializationExits(false, null);
    }

    @Test
    public void testExceptionFromInitializationExitsNonZero() throws Exception {
        assertFailedInitializationExits(true, new IllegalStateException("deliberate test failure"));
    }

    @Test
    public void testFailureThatCannotBeLoggedStillExitsNonZero() throws Exception {
        // The failure reaches main() only through the server terminating. If reporting it could
        // prevent that, the process would stay alive, bound and NOT_SERVING for ever.
        assertFailedInitializationExits(true, new UnprintableException());
    }

    @Test
    public void testFatalErrorFromInitializationHalts() throws Exception {

        installMetricReader();
        server = new TestServer(new TestService(), freePort());
        server.initFailure = new OutOfMemoryError("deliberate test error");
        server.start();
        server.release.countDown();

        assertTrue(server.halted.await(30, TimeUnit.SECONDS));
        assertEquals(Integer.valueOf(GrpcServerBase.EXIT_STATUS_FATAL_ERROR), server.haltStatus.get());
        server.awaitInitThread();
        assertNull("a fatal error halts; it does not take the exit path", server.exitStatus.get());
        assertFalse(server.isGateOpen());
    }

    // ------------------------------------------------------------------
    // stopping during initialization
    // ------------------------------------------------------------------

    @Test
    public void testStopDuringInitializationLeavesTheHandlerToTheInitThread() throws Exception {

        installMetricReader();
        startServer(freePort());

        server.stopServer();
        assertEquals(GrpcServerBase.Lifecycle.STOPPING, server.getLifecycle());
        assertEquals(
                "a half-built handler belongs to the init thread, not to the thread stopping the "
                        + "server",
                0, server.finiCalls.get());

        // initialization then succeeds: the gate stays closed, and the init thread takes down
        // what it built
        server.release.countDown();
        server.awaitInitThread();

        assertFalse(server.isGateOpen());
        assertEquals(GrpcServerBase.Lifecycle.STOPPING, server.getLifecycle());
        assertEquals(1, server.finiCalls.get());
        assertEquals("dp-init-" + DpMetrics.SERVICE_QUERY, server.finiThread.get());
        assertNull(server.exitStatus.get());
    }

    @Test
    public void testFailureAfterAStopDoesNotCallExit() throws Exception {

        installMetricReader();
        startServer(freePort());
        server.initResult = false;

        server.stopServer();
        server.release.countDown();
        server.awaitInitThread();

        // System.exit() blocks for ever when called during shutdown, which is where a real stop is
        assertNull(server.exitStatus.get());
        assertEquals(GrpcServerBase.Lifecycle.STOPPING, server.getLifecycle());
        assertEquals(0, server.finiCalls.get());
        assertNotNull(server.getInitThread());
    }
}
