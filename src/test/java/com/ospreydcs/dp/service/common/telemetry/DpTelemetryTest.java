package com.ospreydcs.dp.service.common.telemetry;

import io.grpc.ManagedChannel;
import io.grpc.Server;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.sdk.OpenTelemetrySdk;
import io.opentelemetry.sdk.metrics.SdkMeterProvider;
import io.opentelemetry.sdk.metrics.data.MetricData;
import io.opentelemetry.sdk.testing.exporter.InMemoryMetricReader;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.util.Collection;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

/**
 * Covers the telemetry bootstrap's contract with everything that records into it (issue #212):
 * that instrumentation is safe before initialization, that an installed SDK actually receives the
 * measurements, and that a reset unbinds the instruments so the next test's SDK receives them
 * instead.
 *
 * <p>Deliberately does not call {@link DpTelemetry#init} — that binds a Prometheus port, which is
 * a shared machine resource and an unrelated failure mode for a unit test. The export path is
 * covered by {@code DpTelemetryPrometheusTest}, which is the only test that binds a port.
 */
@RunWith(JUnit4.class)
public class DpTelemetryTest {

    private InMemoryMetricReader metricReader;
    private OpenTelemetrySdk telemetrySdk;

    @Before
    public void setUp() {
        DpTelemetry.resetForTest();
    }

    @After
    public void tearDown() {
        DpTelemetry.resetForTest();
        if (telemetrySdk != null) {
            telemetrySdk.close();
            telemetrySdk = null;
        }
        metricReader = null;
    }

    private void installTestSdk() {
        metricReader = InMemoryMetricReader.create();
        telemetrySdk = OpenTelemetrySdk.builder()
                .setMeterProvider(
                        SdkMeterProvider.builder().registerMetricReader(metricReader).build())
                .build();
        DpTelemetry.initForTest(telemetrySdk);
    }

    private static MetricData metricNamed(Collection<MetricData> metrics, String name) {
        return metrics.stream()
                .filter(metric -> metric.getName().equals(name))
                .findFirst()
                .orElse(null);
    }

    /**
     * Before initialization the meter must be the no-op one and recording into it must not throw.
     * This is the state of every unit test in the repo that never bootstraps telemetry, so a
     * regression here breaks the whole suite rather than only the metrics.
     */
    @Test
    public void testUninitializedIsNoopAndRecordingIsSafe() {

        assertFalse(DpTelemetry.isInitialized());
        assertSame(OpenTelemetry.noop(), DpTelemetry.openTelemetry());
        assertNotNull(DpTelemetry.meter());

        // must not throw against the no-op meter
        DpMetrics.handlerQueueWait().record(0.5);
        DpMetrics.ingestBuckets().add(1);
        DpMetrics.queryRequests().add(1);
    }

    /** An installed SDK receives the measurements, with the D9 ladder and the seconds unit. */
    @Test
    public void testInitForTestReceivesMeasurements() {

        installTestSdk();
        assertTrue(DpTelemetry.isInitialized());
        assertSame(telemetrySdk, DpTelemetry.openTelemetry());

        DpMetrics.handlerQueueWait().record(0.042);

        final MetricData metric =
                metricNamed(metricReader.collectAllMetrics(), DpMetrics.METRIC_HANDLER_QUEUE_WAIT);
        assertNotNull("queue wait metric was not recorded", metric);
        assertEquals(DpMetrics.UNIT_SECONDS, metric.getUnit());
        assertEquals(
                DpTelemetry.INSTRUMENTATION_SCOPE_NAME, metric.getInstrumentationScopeInfo().getName());

        final var point = metric.getHistogramData().getPoints().iterator().next();
        assertEquals(1, point.getCount());
        assertEquals(DpMetrics.DURATION_BUCKET_BOUNDARIES_SECONDS, point.getBoundaries());
    }

    /**
     * After a reset the instruments must rebind to the newly installed SDK, and nothing may reach
     * the discarded one. This is what lets the integration tests run a fresh reader per test: were
     * the instruments to stay bound, every test after the first would record into a closed
     * provider and its assertions would see an empty reader.
     */
    @Test
    public void testResetRebindsInstrumentsToTheNextSdk() {

        installTestSdk();
        DpMetrics.ingestBuckets().add(7);
        final InMemoryMetricReader firstReader = metricReader;
        final OpenTelemetrySdk firstSdk = telemetrySdk;

        DpTelemetry.resetForTest();
        installTestSdk();

        DpMetrics.ingestBuckets().add(3);

        final MetricData secondMetric =
                metricNamed(metricReader.collectAllMetrics(), DpMetrics.METRIC_INGEST_BUCKETS);
        assertNotNull("instrument did not rebind to the new SDK", secondMetric);
        assertEquals(
                3L,
                secondMetric.getLongSumData().getPoints().iterator().next().getValue());

        // the discarded SDK saw only what was recorded before the reset
        final MetricData firstMetric =
                metricNamed(firstReader.collectAllMetrics(), DpMetrics.METRIC_INGEST_BUCKETS);
        assertNotNull(firstMetric);
        assertEquals(7L, firstMetric.getLongSumData().getPoints().iterator().next().getValue());
        firstSdk.close();
    }

    /**
     * The {@code workers.max} registration stops reporting once closed. An observable gauge is a
     * registered callback rather than something recorded into, so it is the one instrument that
     * outlives its owner if nothing closes it — and an integration test builds many handlers in
     * one JVM, each of which would otherwise keep contributing a worker count forever.
     */
    @Test
    public void testWorkersMaxGaugeRegistrationStopsReportingWhenClosed() throws Exception {

        installTestSdk();

        final AutoCloseable registration =
                DpMetrics.registerHandlerWorkersMax(DpMetrics.SERVICE_QUERY, () -> 7);

        MetricData metric =
                metricNamed(metricReader.collectAllMetrics(), DpMetrics.METRIC_HANDLER_WORKERS_MAX);
        assertNotNull("workers.max gauge did not report", metric);
        assertEquals(
                7L, metric.getLongGaugeData().getPoints().iterator().next().getValue());
        assertEquals(
                DpMetrics.SERVICE_QUERY,
                metric.getLongGaugeData().getPoints().iterator().next().getAttributes()
                        .get(DpMetrics.ATTR_SERVICE));

        registration.close();

        assertNull(
                "gauge kept reporting after its registration was closed",
                metricNamed(metricReader.collectAllMetrics(), DpMetrics.METRIC_HANDLER_WORKERS_MAX));
    }

    /**
     * {@code configureServerBuilder} must be applicable to a builder and leave a working server:
     * it installs interceptors, and interceptors cannot be added to a built server, so the only
     * way this fails in production is at startup.
     *
     * <p>It deliberately does not assert that {@code grpc.server.*} series appear. The in-process
     * transport does not produce them (measured during Task 7) — what is verified here is that the
     * configuration applies cleanly and the server still serves, which is the part that would
     * break a real server's startup.
     */
    @Test
    public void testConfigureServerBuilderLeavesAServableServer() throws Exception {

        installTestSdk();

        final String serverName = InProcessServerBuilder.generateName();
        final InProcessServerBuilder serverBuilder =
                InProcessServerBuilder.forName(serverName).directExecutor();

        DpTelemetry.configureServerBuilder(serverBuilder);

        final Server server = serverBuilder.build().start();
        try {
            final ManagedChannel channel =
                    InProcessChannelBuilder.forName(serverName).directExecutor().build();
            try {
                assertNotNull(channel.authority());
            } finally {
                channel.shutdownNow();
                channel.awaitTermination(5, TimeUnit.SECONDS);
            }
        } finally {
            server.shutdownNow();
            server.awaitTermination(5, TimeUnit.SECONDS);
        }
    }

    /**
     * {@code shutdown()} must discard the cached instruments, not merely close the SDK.
     *
     * <p>This is the production analogue of {@link #testResetRebindsInstrumentsToTheNextSdk()} and
     * it guards a real startup path: {@code GrpcServerBase.start()} calls {@code DpTelemetry.init()},
     * and if {@code initService_()} then fails it calls {@code shutdown()} so an in-process caller
     * can retry. Before this, {@code shutdown()} left every instrument bound to the closed
     * provider, so the retry's fresh SDK received nothing and the process ran blind for its whole
     * life while reporting a healthy startup.
     *
     * <p>Asserts on the meter each instrument is bound to immediately after {@code shutdown()},
     * deliberately <em>without</em> installing another SDK first. Routing through
     * {@link #installTestSdk()} would call {@code initForTest}, which discards instruments itself
     * and would mask a {@code shutdown()} that had failed to — the assertion would then hold
     * whether or not the behavior under test was present.
     */
    @Test
    public void testShutdownDiscardsCachedInstruments() {

        installTestSdk();
        // Build an instrument against the SDK, exactly as a handler does during init.
        DpMetrics.ingestBuckets().add(11);
        assertNotNull(
                "precondition: the installed SDK should have received the recording",
                metricNamed(metricReader.collectAllMetrics(), DpMetrics.METRIC_INGEST_BUCKETS));

        // The production teardown path, not the test hook.
        DpTelemetry.shutdown();

        // The instrument handed out now must be a freshly built one bound to the no-op meter, not
        // the cached one still pointing at the provider shutdown() just closed.
        assertSame(
                "after shutdown() the meter must be the no-op instance",
                OpenTelemetry.noop().getMeter(DpTelemetry.INSTRUMENTATION_SCOPE_NAME).getClass(),
                DpTelemetry.meter().getClass());

        // Recording now must not reach the closed provider. The reader still reports the point it
        // collected before shutdown(), so the check is on the VALUE: a cached instrument still
        // bound to the closed SDK would add to it and carry the sum to 16.
        DpMetrics.ingestBuckets().add(5);
        final MetricData afterShutdown =
                metricNamed(metricReader.collectAllMetrics(), DpMetrics.METRIC_INGEST_BUCKETS);
        assertNotNull("precondition: reader still holds its pre-shutdown point", afterShutdown);
        assertEquals(
                "recording after shutdown() reached the closed provider's reader, so the "
                        + "instrument was never discarded",
                11L,
                afterShutdown.getLongSumData().getPoints().iterator().next().getValue());
    }

    /**
     * {@code initForTest} must discard instruments too, so a test class is self-correcting rather
     * than dependent on a previous test's {@code tearDown} having run. An instrument built earlier
     * in the same JVM fork — by a unit test that never bootstrapped, so against the no-op meter —
     * would otherwise stay cached and the new reader would legitimately see nothing.
     */
    @Test
    public void testInitForTestDiscardsInstrumentsCachedAgainstTheNoopMeter() {

        // Record against the no-op meter, caching an instrument bound to it.
        DpMetrics.ingestSamples().add(99);

        installTestSdk();
        DpMetrics.ingestSamples().add(4);

        final MetricData metric =
                metricNamed(metricReader.collectAllMetrics(), DpMetrics.METRIC_INGEST_SAMPLES);
        assertNotNull(
                "instrument stayed bound to the no-op meter after initForTest()", metric);
        assertEquals(4L, metric.getLongSumData().getPoints().iterator().next().getValue());
    }
}
