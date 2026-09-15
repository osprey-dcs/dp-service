package com.ospreydcs.dp.service.integration.ingest;

import com.ospreydcs.dp.grpc.v1.common.ExceptionalResult;
import com.ospreydcs.dp.grpc.v1.ingestion.IngestDataRequest;
import com.ospreydcs.dp.grpc.v1.ingestion.IngestDataResponse;
import com.ospreydcs.dp.service.common.telemetry.DpMetrics;
import com.ospreydcs.dp.service.ingest.IngestionTestBase;
import com.ospreydcs.dp.service.integration.GrpcIntegrationTestBase;
import io.opentelemetry.sdk.metrics.data.HistogramPointData;
import io.opentelemetry.sdk.metrics.data.LongPointData;
import io.opentelemetry.sdk.metrics.data.MetricData;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/**
 * End-to-end coverage of the ingestion telemetry (issue #212, Task 8): that a request handled
 * asynchronously on a worker thread is counted with the right outcome, that the counters report
 * the payload the request actually carried, and that a rejection is distinguishable from a
 * success.
 *
 * <p>{@code dp.ingest.duration} is the measurement with no substitute here. Ingestion acknowledges
 * a request as soon as it is validated and enqueued, so the RPC duration covers validation and
 * enqueue only — persistence happens afterwards. An operator reading only the gRPC call duration
 * would conclude ingestion was healthy while the queue behind it fell arbitrarily far behind.
 */
@RunWith(JUnit4.class)
public class IngestionMetricsIT extends GrpcIntegrationTestBase {

    private static final int NUM_ROWS = 3;
    private static final List<String> COLUMN_NAMES = List.of("MetricsPV_01", "MetricsPV_02");

    private String providerId;

    @Before
    public void setUp() throws Exception {
        super.setUp();
        providerId = ingestionServiceWrapper.registerProvider("ingestionMetricsProvider", null);
        assertNotNull(providerId);
    }

    @After
    public void tearDown() {
        super.tearDown();
    }

    // ---- metric access -------------------------------------------------------------------------

    private MetricData metricNamed(String name) {
        final Collection<MetricData> metrics = metricReader.collectAllMetrics();
        return metrics.stream()
                .filter(metric -> metric.getName().equals(name))
                .findFirst()
                .orElse(null);
    }

    private long counterValue(String metricName, String outcome) {
        final MetricData metric = metricNamed(metricName);
        if (metric == null) {
            return 0;
        }
        return metric.getLongSumData().getPoints().stream()
                .filter(point -> outcome == null || outcome.equals(
                        point.getAttributes().get(DpMetrics.ATTR_OUTCOME)))
                .mapToLong(LongPointData::getValue)
                .sum();
    }

    /**
     * Waits until the handler has recorded {@code expectedRequests} requests with the given
     * outcome.
     *
     * <p>Ingestion is asynchronous by design: the response the client receives is sent when the
     * request is validated and enqueued, and everything this test asserts on is recorded later on
     * a worker thread. Asserting as soon as the stub returns therefore races the measurement —
     * which is the same property {@code dp.ingest.duration} exists to expose, so the test has to
     * respect it rather than assume it away.
     */
    private void awaitRequestsRecorded(String outcome, long expectedRequests) {
        final long deadline = System.currentTimeMillis() + 30_000;
        long seen = 0;
        while (System.currentTimeMillis() < deadline) {
            seen = counterValue(DpMetrics.METRIC_INGEST_REQUESTS, outcome);
            if (seen >= expectedRequests) {
                return;
            }
            try {
                Thread.sleep(50);
            } catch (InterruptedException ex) {
                Thread.currentThread().interrupt();
                break;
            }
        }
        throw new AssertionError(
                "timed out waiting for " + expectedRequests + " " + outcome + " ingestion "
                        + "request(s); dp.ingest.requests recorded " + seen);
    }

    // ---- requests ------------------------------------------------------------------------------

    private IngestDataRequest buildRequest(String requestId, List<String> columnNames) {
        return buildRequest(requestId, columnNames, Instant.now().getEpochSecond());
    }

    /**
     * Builds a request whose bucket starts at {@code epochSeconds}.
     *
     * <p>The second is a parameter because two requests for the same PV and the same start time are
     * the same bucket: the second insert fails on the unique index and is recorded as an error, not
     * a success. A streaming test therefore has to stagger its requests, or it is measuring a
     * duplicate-key failure rather than the throughput it means to.
     */
    private IngestDataRequest buildRequest(
            String requestId, List<String> columnNames, long epochSeconds) {
        // values is indexed by column, each inner list holding that column's NUM_ROWS samples
        final List<List<Object>> values = new ArrayList<>();
        for (int column = 0; column < columnNames.size(); column++) {
            final List<Object> columnValues = new ArrayList<>();
            for (int row = 0; row < NUM_ROWS; row++) {
                columnValues.add(1.0 + row + column);
            }
            values.add(columnValues);
        }

        final IngestionTestBase.IngestionRequestParams params =
                new IngestionTestBase.IngestionRequestParams(
                        providerId,
                        requestId,
                        null,
                        null,
                        epochSeconds,
                        0L,
                        1_000_000L,
                        NUM_ROWS,
                        columnNames,
                        IngestionTestBase.IngestionDataType.DOUBLE,
                        values,
                        null);
        return IngestionTestBase.buildIngestionRequest(params);
    }

    /**
     * A successful request is counted once, with a duration, and its payload counters report the
     * buckets written and the samples ingested.
     *
     * <p>The sample count is rows × columns, and it comes from the shared column-count method
     * rather than a private sum — a private copy would silently start undercounting the first time
     * a column type was added, and report a plausible number while doing it.
     */
    @Test
    public void testSuccessfulRequestRecordsItsOutcomeAndPayload() {

        final IngestDataRequest request = buildRequest("metrics-request-1", COLUMN_NAMES);
        final IngestDataResponse response = ingestionServiceWrapper.sendIngestData(request);
        assertFalse(response.hasExceptionalResult());

        awaitRequestsRecorded(DpMetrics.OUTCOME_SUCCESS, 1);

        assertEquals(
                "one request should be counted as a success",
                1L,
                counterValue(DpMetrics.METRIC_INGEST_REQUESTS, DpMetrics.OUTCOME_SUCCESS));

        // one bucket per column
        assertEquals(
                COLUMN_NAMES.size(),
                counterValue(DpMetrics.METRIC_INGEST_BUCKETS, null));

        // rows x columns
        assertEquals(
                (long) NUM_ROWS * COLUMN_NAMES.size(),
                counterValue(DpMetrics.METRIC_INGEST_SAMPLES, null));

        assertEquals(
                "request bytes must be the request's serialized size",
                request.getSerializedSize(),
                counterValue(DpMetrics.METRIC_INGEST_REQUEST_BYTES, null));
        assertEquals(
                DpMetrics.UNIT_BYTES, metricNamed(DpMetrics.METRIC_INGEST_REQUEST_BYTES).getUnit());

        final MetricData duration = metricNamed(DpMetrics.METRIC_INGEST_DURATION);
        assertNotNull("dp.ingest.duration was not recorded", duration);
        assertEquals(DpMetrics.UNIT_SECONDS, duration.getUnit());

        final HistogramPointData durationPoint = duration.getHistogramData().getPoints().stream()
                .filter(point -> DpMetrics.OUTCOME_SUCCESS.equals(
                        point.getAttributes().get(DpMetrics.ATTR_OUTCOME)))
                .findFirst()
                .orElseThrow(() -> new AssertionError("no success point in dp.ingest.duration"));
        assertEquals(1, durationPoint.getCount());
        assertTrue(
                "ingest duration " + durationPoint.getSum() + "s is not seconds-scaled",
                durationPoint.getSum() > 0.0 && durationPoint.getSum() < 60.0);
        assertEquals(
                "the D9 ladder is not in force",
                DpMetrics.DURATION_BUCKET_BOUNDARIES_SECONDS,
                durationPoint.getBoundaries());
    }

    /**
     * A rejected request is counted as {@code reject} and contributes no buckets, but still counts
     * its samples and bytes.
     *
     * <p>That asymmetry is deliberate: a rejected request is real offered load that stored nothing,
     * so counting samples on every outcome and buckets only on success keeps
     * {@code dp.ingest.buckets / dp.ingest.samples} readable as "what fraction of the offered load
     * was actually stored". Excluding failures from the denominator would make that ratio read as
     * 1.0 no matter how much was being dropped.
     */
    @Test
    public void testRejectedRequestIsCountedAsRejectWithNoBuckets() {

        // an empty PV name, which the service rejects
        final IngestDataRequest request = buildRequest("metrics-reject-1", Arrays.asList(""));
        final IngestDataResponse response = ingestionServiceWrapper.sendIngestData(request);

        assertTrue(response.hasExceptionalResult());
        assertEquals(
                ExceptionalResult.ExceptionalResultStatus.RESULT_STATUS_REJECT,
                response.getExceptionalResult().getExceptionalResultStatus());

        awaitRequestsRecorded(DpMetrics.OUTCOME_REJECT, 1);

        assertEquals(
                "the rejection used the wrong dp.outcome spelling",
                1L,
                counterValue(DpMetrics.METRIC_INGEST_REQUESTS, DpMetrics.OUTCOME_REJECT));
        assertEquals(
                "a rejected request must not be counted as a success",
                0L,
                counterValue(DpMetrics.METRIC_INGEST_REQUESTS, DpMetrics.OUTCOME_SUCCESS));
        assertEquals(
                "a rejected request wrote buckets",
                0L,
                counterValue(DpMetrics.METRIC_INGEST_BUCKETS, null));
        assertTrue(
                "a rejected request's offered load was not counted",
                counterValue(DpMetrics.METRIC_INGEST_SAMPLES, null) > 0);
        assertTrue(
                "a rejected request's bytes were not counted",
                counterValue(DpMetrics.METRIC_INGEST_REQUEST_BYTES, null) > 0);
    }

    /**
     * A streaming call records one request per message, not one per stream. A long-lived bidi
     * stream would otherwise report durations that grow with the age of the stream rather than
     * with how long each request took.
     */
    @Test
    public void testStreamRecordsOneRequestPerMessageNotPerStream() {

        final int numRequests = 3;
        final long firstSecond = Instant.now().getEpochSecond();
        final List<IngestDataRequest> requestList = new ArrayList<>();
        for (int i = 0; i < numRequests; i++) {
            requestList.add(buildRequest("metrics-stream-" + i, COLUMN_NAMES, firstSecond + i));
        }

        ingestionServiceWrapper.sendIngestDataStream(requestList);

        awaitRequestsRecorded(DpMetrics.OUTCOME_SUCCESS, numRequests);

        assertEquals(
                numRequests,
                counterValue(DpMetrics.METRIC_INGEST_REQUESTS, DpMetrics.OUTCOME_SUCCESS));
        assertEquals(
                (long) numRequests * COLUMN_NAMES.size(),
                counterValue(DpMetrics.METRIC_INGEST_BUCKETS, null));
        assertEquals(
                (long) numRequests * NUM_ROWS * COLUMN_NAMES.size(),
                counterValue(DpMetrics.METRIC_INGEST_SAMPLES, null));
    }

    /**
     * The D8 guard for the ingestion metrics: the only attribute is {@code dp.outcome}, and
     * neither the provider id nor the client request id nor a PV name reaches any attribute value.
     * Each of those three is per-request and unbounded, and any of them as an attribute would
     * multiply the series count by the size of the facility.
     */
    @Test
    public void testIngestionMetricsCarryOnlyTheOutcomeAttribute() {

        ingestionServiceWrapper.sendIngestData(buildRequest("metrics-d8-1", COLUMN_NAMES));
        awaitRequestsRecorded(DpMetrics.OUTCOME_SUCCESS, 1);

        final List<String> ingestionMetricNames = List.of(
                DpMetrics.METRIC_INGEST_REQUESTS,
                DpMetrics.METRIC_INGEST_DURATION,
                DpMetrics.METRIC_INGEST_BUCKETS,
                DpMetrics.METRIC_INGEST_SAMPLES,
                DpMetrics.METRIC_INGEST_REQUEST_BYTES);

        final Set<String> forbiddenValues =
                Set.of(providerId, "metrics-d8-1", COLUMN_NAMES.get(0));

        for (String metricName : ingestionMetricNames) {
            final MetricData metric = metricNamed(metricName);
            assertNotNull(metricName + " was not recorded", metric);
            metric.getData().getPoints().forEach(point ->
                    point.getAttributes().forEach((key, value) -> {
                        assertEquals(
                                metricName + " carries attribute outside the D8 vocabulary: "
                                        + key.getKey(),
                                DpMetrics.ATTR_OUTCOME.getKey(),
                                key.getKey());
                        assertFalse(
                                "per-request detail leaked into " + metricName + "." + key.getKey(),
                                forbiddenValues.contains(String.valueOf(value)));
                    }));
        }
    }
}
