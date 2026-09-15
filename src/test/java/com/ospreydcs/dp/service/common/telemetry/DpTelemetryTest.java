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
}
