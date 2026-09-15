package com.ospreydcs.dp.service.common.telemetry;

import com.ospreydcs.dp.service.common.exception.DpRuntimeException;
import io.opentelemetry.api.common.Attributes;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.io.IOException;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.ServerSocket;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * The only test that exercises the real export path (issue #212): a genuine SDK built by
 * {@link DpTelemetry#init}, a Prometheus endpoint bound on a real port, and an HTTP scrape whose
 * text is asserted against. Everything else in the suite reads measurements out of an in-memory
 * reader, which cannot catch a failure in the exporter, the renderer, or the endpoint — the three
 * pieces between a recorded measurement and what an operator actually sees.
 */
@RunWith(JUnit4.class)
public class DpTelemetryPrometheusTest {

    private static final String SERVICE_NAME = "dp-telemetry-prometheus-test";

    @Before
    public void setUp() {
        DpTelemetry.shutdown();
        DpTelemetry.resetForTest();
    }

    @After
    public void tearDown() {
        DpTelemetry.shutdown();
        DpTelemetry.resetForTest();
    }

    /**
     * A port free at this instant. Inherently a race — something else could take it before
     * {@code init()} binds — but the alternative is a fixed port, which collides with a developer's
     * running service or a parallel build far more often than this window loses.
     */
    private static int freePort() throws IOException {
        try (ServerSocket socket = new ServerSocket(0)) {
            socket.setReuseAddress(true);
            return socket.getLocalPort();
        }
    }

    private static String scrape(int port) throws IOException {
        final HttpURLConnection connection = (HttpURLConnection)
                URI.create("http://127.0.0.1:" + port + "/metrics").toURL().openConnection();
        connection.setConnectTimeout(10_000);
        connection.setReadTimeout(10_000);
        try {
            assertEquals(200, connection.getResponseCode());
            try (InputStream stream = connection.getInputStream()) {
                return new String(stream.readAllBytes(), StandardCharsets.UTF_8);
            }
        } finally {
            connection.disconnect();
        }
    }

    /** The line for one metric name, or null; the scrape carries hundreds of unrelated JVM lines. */
    private static String lineStartingWith(String scrape, String prefix) {
        return scrape.lines()
                .filter(line -> line.startsWith(prefix))
                .findFirst()
                .orElse(null);
    }

    /**
     * Records one observation and asserts the rendered histogram: the name Prometheus derives from
     * the OTel name plus the seconds unit, the {@code dp_} attribute rendering, and — the part only
     * this test can see — that the D9 boundaries reach the exposition as {@code le} buckets. A
     * histogram exported with the SDK's default millisecond-oriented boundaries would render a
     * complete, plausible set of buckets, which is why the ladder is asserted here rather than
     * inferred from the in-memory point.
     */
    @Test
    public void testPrometheusEndpointRendersDpHistogramWithD9Boundaries() throws Exception {

        final int port = freePort();
        DpTelemetry.init(SERVICE_NAME, port);
        assertTrue(DpTelemetry.isInitialized());

        // lands in the (0.01, 0.05] bucket of the D9 ladder
        DpMetrics.handlerQueueWait().record(0.042, Attributes.of(
                DpMetrics.ATTR_SERVICE, DpMetrics.SERVICE_QUERY));

        final String scrape = scrape(port);

        // Prometheus renames dp.handler.queue.wait with unit s to dp_handler_queue_wait_seconds
        final String bucketPrefix = "dp_handler_queue_wait_seconds_bucket";
        assertTrue("scrape did not contain " + bucketPrefix, scrape.contains(bucketPrefix));

        final String countLine = lineStartingWith(scrape, "dp_handler_queue_wait_seconds_count");
        assertNotNull("no _count line in the scrape", countLine);
        assertTrue(
                "the dp.service attribute did not render: " + countLine,
                countLine.contains("dp_service=\"" + DpMetrics.SERVICE_QUERY + "\""));

        // Asserted as separate substrings rather than one contiguous {dp_service=...,le=...}
        // because the exporter inserts otel_scope_name between them; matching the pair as written
        // would fail against a correct export.
        assertTrue(
                "the instrumentation scope did not render: " + countLine,
                countLine.contains(
                        "otel_scope_name=\"" + DpTelemetry.INSTRUMENTATION_SCOPE_NAME + "\""));

        // every D9 boundary must appear as an le bucket, and the observation must be at or below
        // the 0.05 edge and above the 0.01 one
        for (Double boundary : DpMetrics.DURATION_BUCKET_BOUNDARIES_SECONDS) {
            final String le = "le=\"" + formatBoundary(boundary) + "\"";
            assertTrue(
                    "D9 boundary " + boundary + " missing from the exposition",
                    scrape.lines().anyMatch(
                            line -> line.startsWith(bucketPrefix) && line.contains(le)));
        }
        assertEquals(0.0, bucketValue(scrape, bucketPrefix, 0.01), 0.0);
        assertEquals(1.0, bucketValue(scrape, bucketPrefix, 0.05), 0.0);

        // the JVM runtime metrics DpTelemetry registers alongside the dp instruments
        assertTrue("runtime telemetry did not register", scrape.contains("jvm_memory_used_bytes"));
    }

    /**
     * An already-bound Prometheus port must stop startup with the bind failure named (D6). The
     * alternative the exception prevents is a service that starts and serves with silently absent
     * metrics — discovered by an operator only when they go looking for a number that is not there.
     */
    @Test
    public void testInitFailsClosedOnABoundPort() throws Exception {

        try (ServerSocket holder = new ServerSocket(0)) {
            final int port = holder.getLocalPort();
            try {
                DpTelemetry.init(SERVICE_NAME, port);
                fail("init() succeeded against a bound port");
            } catch (DpRuntimeException ex) {
                // The message must carry the root cause: the autoconfigure module's own wrapper
                // says "Unexpected configuration error", and this exception's message is the line
                // an operator reads first.
                assertTrue(
                        "the bind failure was not named: " + ex.getMessage(),
                        ex.getMessage().contains("Address already in use"));
                assertTrue(
                        "the endpoint was not named: " + ex.getMessage(),
                        ex.getMessage().contains(String.valueOf(port)));
            }
        }

        assertTrue("a failed init must leave telemetry uninitialized", !DpTelemetry.isInitialized());
    }

    /**
     * A second {@code init()} is a no-op rather than a replacement: the benchmark programs run a
     * server and a client in one JVM, and swapping the SDK underneath instruments that already
     * hold the old meter would leave them recording into a shut-down provider.
     */
    @Test
    public void testSecondInitIsANoOp() throws Exception {

        final int firstPort = freePort();
        DpTelemetry.init(SERVICE_NAME, firstPort);

        final int secondPort = freePort();
        DpTelemetry.init(SERVICE_NAME, secondPort); // must not throw, must not rebind

        DpMetrics.ingestBuckets().add(4);

        assertTrue(
                "the first endpoint stopped serving",
                scrape(firstPort).contains("dp_ingest_buckets_total"));

        try {
            scrape(secondPort);
            fail("the second init bound a second endpoint");
        } catch (IOException expected) {
            // nothing is listening on the second port, which is the point
        }
    }

    /** Renders a boundary the way the Prometheus exporter does, for the {@code le} match. */
    private static String formatBoundary(double boundary) {
        if (boundary == Math.rint(boundary)) {
            return String.valueOf((long) boundary) + ".0";
        }
        return String.valueOf(boundary);
    }

    /** Value of the cumulative bucket whose {@code le} is the given boundary. */
    private static double bucketValue(String scrape, String metricPrefix, double boundary) {
        final String le = "le=\"" + formatBoundary(boundary) + "\"";
        final List<String> matches = scrape.lines()
                .filter(line -> line.startsWith(metricPrefix) && line.contains(le))
                .toList();
        assertEquals("expected exactly one bucket line for " + le, 1, matches.size());
        final String line = matches.get(0);
        return Double.parseDouble(line.substring(line.lastIndexOf(' ') + 1).trim());
    }
}
