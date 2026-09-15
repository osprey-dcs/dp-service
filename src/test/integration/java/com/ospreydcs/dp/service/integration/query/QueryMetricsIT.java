package com.ospreydcs.dp.service.integration.query;

import com.ospreydcs.dp.grpc.v1.common.ExceptionalResult;
import com.ospreydcs.dp.grpc.v1.query.QueryBucketsRequest;
import com.ospreydcs.dp.grpc.v1.query.QueryBucketsResponse;
import com.ospreydcs.dp.grpc.v1.query.QuerySamplesRequest;
import com.ospreydcs.dp.grpc.v1.query.QuerySamplesResponse;
import com.ospreydcs.dp.grpc.v1.query.QuerySpec;
import com.ospreydcs.dp.grpc.v1.query.QueryTableRequest;
import com.ospreydcs.dp.service.common.telemetry.DpMetrics;
import com.ospreydcs.dp.service.integration.GrpcIntegrationTestBase;
import com.ospreydcs.dp.service.integration.ingest.GrpcIntegrationIngestionServiceWrapper;
import com.ospreydcs.dp.service.query.QueryTestBase;
import io.opentelemetry.sdk.metrics.data.HistogramPointData;
import io.opentelemetry.sdk.metrics.data.LongPointData;
import io.opentelemetry.sdk.metrics.data.MetricData;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.LoggerContext;
import org.apache.logging.log4j.core.appender.AbstractAppender;
import org.apache.logging.log4j.core.config.Property;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/**
 * End-to-end coverage of the query telemetry (issue #212, D3 and D5): that a request over the gRPC
 * channel produces the full stage breakdown, that the counters report what the request actually
 * did, and that the slow-query log line carries the per-request detail the metrics deliberately do
 * not.
 *
 * <p>Asserts on the dp instruments only, never on {@code grpc.server.*}: the in-process transport
 * the integration tests use produces no gRPC series even though the server builder is configured
 * exactly as production's is (Task 7 finding 7).
 */
@RunWith(JUnit4.class)
public class QueryMetricsIT extends GrpcIntegrationTestBase {

    private static final String PV_1 = "S01-GCC01";
    private static final String PV_2 = "S01-BPM01";

    private ListAppender slowQueryAppender;
    private long startSeconds;

    /** Captures the {@code dp.slowquery} lines produced during one test. */
    private static class ListAppender extends AbstractAppender {

        private final List<String> messages = new ArrayList<>();

        ListAppender() {
            super("QueryMetricsITAppender", null, null, true, Property.EMPTY_ARRAY);
        }

        @Override
        public void append(LogEvent event) {
            synchronized (messages) {
                messages.add(event.getMessage().getFormattedMessage());
            }
        }

        List<String> messages() {
            synchronized (messages) {
                return List.copyOf(messages);
            }
        }
    }

    @Before
    public void setUp() throws Exception {
        super.setUp();

        // attach to the dedicated slow-query logger by its literal name, which is the name an
        // operator routes in log4j2.xml -- asserting through the same name the configuration uses
        slowQueryAppender = new ListAppender();
        slowQueryAppender.start();
        final LoggerContext context = (LoggerContext) LogManager.getContext(false);
        context.getConfiguration().addLoggerAppender(
                (org.apache.logging.log4j.core.Logger) LogManager.getLogger("dp.slowquery"),
                slowQueryAppender);

        startSeconds = Instant.now().getEpochSecond();
        final GrpcIntegrationIngestionServiceWrapper.IngestionScenarioResult scenario =
                ingestionServiceWrapper.simpleIngestionScenario(startSeconds, false);
        assertNotNull(scenario);
    }

    @After
    public void tearDown() {
        if (slowQueryAppender != null) {
            final LoggerContext context = (LoggerContext) LogManager.getContext(false);
            ((org.apache.logging.log4j.core.Logger) LogManager.getLogger("dp.slowquery"))
                    .removeAppender(slowQueryAppender);
            slowQueryAppender.stop();
            slowQueryAppender = null;
        }
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

    /**
     * Waits until the worker has completed the telemetry for {@code rpcMethod}.
     *
     * <p>Necessary because a query's measurements are recorded <em>after</em> the response the
     * client is waiting on. {@code QueryTelemetry.complete()} runs in the job's {@code finally},
     * which the worker reaches only once {@code executeAndDispatch} has returned — and for a unary
     * call the dispatcher has sent the response by then. So a test that asserts as soon as the
     * stub returns is racing the recording it is asserting on, and would fail intermittently
     * against entirely correct code.
     *
     * <p>Polls rather than sleeps a fixed interval so a slow machine does not need a longer
     * constant, and fails with the stages actually seen rather than a bare timeout.
     */
    /**
     * Waits for at least one captured slow-query line containing {@code match}.
     *
     * <p>Separate from {@link #awaitRequestRecorded} because the two signals are written at
     * different points of {@code QueryTelemetry.complete()} -- the counter first, the log line
     * afterwards -- so a test that awaits the counter and then reads the appender has a real race
     * against the recording thread.
     */
    private List<String> awaitSlowQueryLines(String match) {
        final long deadline = System.currentTimeMillis() + 30_000;
        while (System.currentTimeMillis() < deadline) {
            final List<String> lines = slowQueryAppender.messages().stream()
                    .filter(message -> message.contains(match))
                    .toList();
            if (!lines.isEmpty()) {
                return lines;
            }
            try {
                Thread.sleep(50);
            } catch (InterruptedException ex) {
                Thread.currentThread().interrupt();
                break;
            }
        }
        return List.of();
    }

    private void awaitRequestRecorded(String rpcMethod, long expectedRequests) {
        final long deadline = System.currentTimeMillis() + 30_000;
        long seen = 0;
        while (System.currentTimeMillis() < deadline) {
            final MetricData metric = metricNamed(DpMetrics.METRIC_QUERY_REQUESTS);
            if (metric != null) {
                seen = metric.getLongSumData().getPoints().stream()
                        .filter(point -> rpcMethod.equals(
                                point.getAttributes().get(DpMetrics.ATTR_RPC_METHOD)))
                        .mapToLong(LongPointData::getValue)
                        .sum();
                if (seen >= expectedRequests) {
                    return;
                }
            }
            try {
                Thread.sleep(50);
            } catch (InterruptedException ex) {
                Thread.currentThread().interrupt();
                break;
            }
        }
        throw new AssertionError(
                "timed out waiting for " + expectedRequests + " completed " + rpcMethod
                        + " request(s); dp.query.requests recorded " + seen);
    }

    private List<HistogramPointData> stagePoints(String rpcMethod) {
        final MetricData metric = metricNamed(DpMetrics.METRIC_QUERY_STAGE_DURATION);
        assertNotNull("dp.query.stage.duration was not recorded", metric);
        return metric.getHistogramData().getPoints().stream()
                .filter(point -> rpcMethod.equals(
                        point.getAttributes().get(DpMetrics.ATTR_RPC_METHOD)))
                .collect(Collectors.toList());
    }

    private Set<String> stagesRecordedFor(String rpcMethod) {
        return stagePoints(rpcMethod).stream()
                .map(point -> point.getAttributes().get(DpMetrics.ATTR_STAGE))
                .collect(Collectors.toSet());
    }

    private HistogramPointData stagePoint(String rpcMethod, String stage) {
        return stagePoints(rpcMethod).stream()
                .filter(point -> stage.equals(point.getAttributes().get(DpMetrics.ATTR_STAGE)))
                .findFirst()
                .orElseThrow(() -> new AssertionError(
                        "no " + stage + " stage for " + rpcMethod
                                + "; recorded: " + stagesRecordedFor(rpcMethod)));
    }

    private long counterValue(String metricName, String rpcMethod, String outcome) {
        final MetricData metric = metricNamed(metricName);
        assertNotNull(metricName + " was not recorded", metric);
        return metric.getLongSumData().getPoints().stream()
                .filter(point -> rpcMethod.equals(
                        point.getAttributes().get(DpMetrics.ATTR_RPC_METHOD)))
                .filter(point -> outcome == null || outcome.equals(
                        point.getAttributes().get(DpMetrics.ATTR_OUTCOME)))
                .mapToLong(LongPointData::getValue)
                .sum();
    }

    // ---- requests ------------------------------------------------------------------------------

    private QuerySpec spec(List<String> pvNames) {
        return QueryTestBase.buildV2QuerySpecPvNameList(
                pvNames, startSeconds, 0, startSeconds + 10, 0);
    }

    /**
     * Every query method records the full stage breakdown, and the counters report what the
     * request actually did.
     *
     * <p>The stage set is the contract a dashboard is built on: an operator answering "where did
     * the time go" needs every stage present for every method, because a missing stage reads as
     * zero time spent there rather than as no measurement.
     */
    @Test
    public void testEveryQueryMethodRecordsItsStagesAndCounters() {

        // ---- queryBuckets (unary, V2) ----
        {
            final QueryBucketsRequest request =
                    QueryTestBase.buildQueryBucketsRequest(spec(List.of(PV_1)), 0, null, false, false);
            final QueryBucketsResponse response = queryServiceWrapper.sendQueryBuckets(request);
            assertTrue(response.hasBucketQueryResult());
            assertEquals(10, response.getBucketQueryResult().getDataBucketsCount());
        }
        awaitRequestRecorded("queryBuckets", 1);

        assertEquals(
                "queryBuckets did not record every stage",
                Set.of(DpMetrics.STAGE_RESOLVE, DpMetrics.STAGE_QUEUE, DpMetrics.STAGE_DB,
                        DpMetrics.STAGE_PROCESS, DpMetrics.STAGE_TOTAL),
                stagesRecordedFor("queryBuckets"));

        // the 10 buckets the query actually read
        assertEquals(10L, counterValue(DpMetrics.METRIC_QUERY_BUCKETS, "queryBuckets", null));
        assertEquals(
                1L,
                counterValue(
                        DpMetrics.METRIC_QUERY_REQUESTS, "queryBuckets", DpMetrics.OUTCOME_SUCCESS));
        assertTrue(
                "no response messages counted",
                counterValue(DpMetrics.METRIC_QUERY_RESPONSE_MESSAGES, "queryBuckets", null) >= 1);
        assertTrue(
                "no response bytes counted",
                counterValue(DpMetrics.METRIC_QUERY_RESPONSE_BYTES, "queryBuckets", null) > 0);

        // total must be at least as large as any single stage it contains
        final HistogramPointData total = stagePoint("queryBuckets", DpMetrics.STAGE_TOTAL);
        final HistogramPointData db = stagePoint("queryBuckets", DpMetrics.STAGE_DB);
        assertTrue(
                "total (" + total.getSum() + "s) is smaller than the db stage (" + db.getSum() + "s)",
                total.getSum() >= db.getSum());
        assertTrue(
                "total of " + total.getSum() + "s is not seconds-scaled",
                total.getSum() > 0.0 && total.getSum() < 60.0);
        assertEquals(
                DpMetrics.UNIT_SECONDS,
                metricNamed(DpMetrics.METRIC_QUERY_STAGE_DURATION).getUnit());

        // ---- queryBucketsStream (V2) ----
        {
            final QueryBucketsRequest request =
                    QueryTestBase.buildQueryBucketsRequest(spec(List.of(PV_1)), 4, null, false, false);
            final List<QueryBucketsResponse> messages =
                    queryServiceWrapper.sendQueryBucketsStream(request);
            assertFalse(messages.isEmpty());
        }
        awaitRequestRecorded("queryBucketsStream", 1);
        assertEquals(
                Set.of(DpMetrics.STAGE_RESOLVE, DpMetrics.STAGE_QUEUE, DpMetrics.STAGE_DB,
                        DpMetrics.STAGE_PROCESS, DpMetrics.STAGE_TOTAL),
                stagesRecordedFor("queryBucketsStream"));
        assertEquals(10L, counterValue(DpMetrics.METRIC_QUERY_BUCKETS, "queryBucketsStream", null));
        assertTrue(
                "a chunked stream should count more than one response message",
                counterValue(DpMetrics.METRIC_QUERY_RESPONSE_MESSAGES, "queryBucketsStream", null) > 1);

        // ---- querySamples (V2) ----
        {
            final QuerySamplesRequest request =
                    QueryTestBase.buildQuerySamplesRequest(spec(List.of(PV_1, PV_2)), 0, null, false);
            final QuerySamplesResponse response = queryServiceWrapper.sendQuerySamples(request);
            assertTrue(response.hasSampleQueryResult());
        }
        awaitRequestRecorded("querySamples", 1);
        assertEquals(
                Set.of(DpMetrics.STAGE_RESOLVE, DpMetrics.STAGE_QUEUE, DpMetrics.STAGE_DB,
                        DpMetrics.STAGE_PROCESS, DpMetrics.STAGE_TOTAL),
                stagesRecordedFor("querySamples"));
        assertEquals(20L, counterValue(DpMetrics.METRIC_QUERY_BUCKETS, "querySamples", null));

        // ---- querySamplesStream (V2) ----
        {
            final QuerySamplesRequest request =
                    QueryTestBase.buildQuerySamplesRequest(spec(List.of(PV_1)), 30, null, false);
            final List<QuerySamplesResponse> messages =
                    queryServiceWrapper.sendQuerySamplesStream(request);
            assertFalse(messages.isEmpty());
        }
        awaitRequestRecorded("querySamplesStream", 1);
        assertEquals(
                Set.of(DpMetrics.STAGE_RESOLVE, DpMetrics.STAGE_QUEUE, DpMetrics.STAGE_DB,
                        DpMetrics.STAGE_PROCESS, DpMetrics.STAGE_TOTAL),
                stagesRecordedFor("querySamplesStream"));

        // ---- queryData (legacy V1) ----
        {
            final QueryTestBase.QueryDataRequestParams params =
                    new QueryTestBase.QueryDataRequestParams(
                            List.of(PV_1), startSeconds, 0L, startSeconds + 10, 0L);
            queryServiceWrapper.queryData(params, false, "");
        }
        awaitRequestRecorded("queryData", 1);
        assertEquals(
                "the legacy path must record the same stage set",
                Set.of(DpMetrics.STAGE_RESOLVE, DpMetrics.STAGE_QUEUE, DpMetrics.STAGE_DB,
                        DpMetrics.STAGE_PROCESS, DpMetrics.STAGE_TOTAL),
                stagesRecordedFor("queryData"));
        assertEquals(
                1L,
                counterValue(
                        DpMetrics.METRIC_QUERY_REQUESTS, "queryData", DpMetrics.OUTCOME_SUCCESS));

        // ---- queryTable (legacy V1, its own job and dispatcher) ----
        {
            final QueryTestBase.QueryTableRequestParams params =
                    new QueryTestBase.QueryTableRequestParams(
                            QueryTableRequest.TableResultFormat.TABLE_FORMAT_COLUMN,
                            List.of(PV_1),
                            null,
                            startSeconds, 0L, startSeconds + 10, 0L);
            assertNotNull(queryServiceWrapper.queryTable(params, false, ""));
        }
        awaitRequestRecorded("queryTable", 1);
        assertEquals(
                Set.of(DpMetrics.STAGE_RESOLVE, DpMetrics.STAGE_QUEUE, DpMetrics.STAGE_DB,
                        DpMetrics.STAGE_PROCESS, DpMetrics.STAGE_TOTAL),
                stagesRecordedFor("queryTable"));
        assertEquals(
                1L,
                counterValue(
                        DpMetrics.METRIC_QUERY_REQUESTS, "queryTable", DpMetrics.OUTCOME_SUCCESS));
    }

    /**
     * A rejected request is counted as {@code reject}, not {@code error} and not {@code success}.
     * The distinction is the repo's #235 split carried into the metrics: a rise in rejects means
     * clients are sending something wrong, a rise in errors means the service is failing, and an
     * alert that cannot tell them apart pages the wrong person.
     *
     * <p>It also records no database time, because the request never reached the database — which
     * is what keeps a burst of client mistakes from looking like a database slowdown.
     */
    @Test
    public void testRejectedRequestIsCountedAsRejectWithNoDatabaseWork() {

        // a streaming call with a page token, which the server rejects
        final QueryBucketsRequest request =
                QueryTestBase.buildQueryBucketsRequest(spec(List.of(PV_1)), 4, "bogus-token", false, false);
        final List<QueryBucketsResponse> messages =
                queryServiceWrapper.sendQueryBucketsStream(request);

        assertFalse(messages.isEmpty());
        final QueryBucketsResponse last = messages.get(messages.size() - 1);
        assertTrue(last.hasExceptionalResult());
        assertEquals(
                ExceptionalResult.ExceptionalResultStatus.RESULT_STATUS_REJECT,
                last.getExceptionalResult().getExceptionalResultStatus());
        awaitRequestRecorded("queryBucketsStream", 1);

        assertEquals(
                "the rejection was not counted as reject",
                1L,
                counterValue(
                        DpMetrics.METRIC_QUERY_REQUESTS, "queryBucketsStream",
                        DpMetrics.OUTCOME_REJECT));
        assertEquals(
                "a rejected request must not be counted as a success",
                0L,
                counterValue(
                        DpMetrics.METRIC_QUERY_REQUESTS, "queryBucketsStream",
                        DpMetrics.OUTCOME_SUCCESS));
        assertEquals(
                "a rejected request read buckets",
                0L,
                counterValue(DpMetrics.METRIC_QUERY_BUCKETS, "queryBucketsStream", null));
        assertEquals(
                "a rejected request spent time in the database",
                0.0,
                stagePoint("queryBucketsStream", DpMetrics.STAGE_DB).getSum(),
                0.0);
    }

    /**
     * The slow-query line carries the per-request detail the metrics deliberately omit — the PV
     * names above all. That split is the whole of D5: the histograms answer "is the service slow
     * and in which stage" across all requests without multiplying the series count by the size of
     * the facility, and this line answers "which request was slow and what did it ask for".
     *
     * <p>The threshold is 0 in {@code src/test/resources/application.yml}, so every query in the
     * suite qualifies.
     */
    @Test
    public void testSlowQueryLineCarriesTheStageBreakdownAndRequestShape() {

        final QueryBucketsRequest request =
                QueryTestBase.buildQueryBucketsRequest(spec(List.of(PV_1)), 0, null, false, false);
        queryServiceWrapper.sendQueryBuckets(request);

        // Wait for the log line itself, not for dp.query.requests. complete() increments the
        // counter before it writes the line, so awaiting the counter can return in the window
        // between the two -- an intermittent "no slow query line was produced" that reproduces
        // only under a loaded full-suite run.
        final List<String> lines = awaitSlowQueryLines("method: queryBuckets");
        assertFalse("no slow query line was produced", lines.isEmpty());

        final String line = lines.get(lines.size() - 1);
        for (String expected : new String[] {
                "outcome: " + DpMetrics.OUTCOME_SUCCESS,
                "totalMs:", "resolveMs:", "queueMs:", "dbMs:", "processMs:",
                "buckets: 10", "messages:", "bytes:"}) {
            assertTrue("slow query line is missing " + expected + ": " + line, line.contains(expected));
        }

        // the PV names, which are exactly what the metrics may not carry
        assertTrue("the PV name is not in the line: " + line, line.contains(PV_1));
        assertTrue("the PV count is not in the line: " + line, line.contains("pvCount=1"));
    }

    /**
     * The D8 guard for the query metrics: every point carries only the declared vocabulary, and no
     * PV name reaches any attribute value. A facility with 10^5 PVs would otherwise turn one
     * histogram into 10^5 time series.
     */
    @Test
    public void testQueryMetricsCarryOnlyTheDeclaredAttributes() {

        final QueryBucketsRequest request =
                QueryTestBase.buildQueryBucketsRequest(spec(List.of(PV_1, PV_2)), 0, null, false, false);
        queryServiceWrapper.sendQueryBuckets(request);
        awaitRequestRecorded("queryBuckets", 1);

        final Set<String> allowedKeys = Set.of(
                DpMetrics.ATTR_RPC_METHOD.getKey(),
                DpMetrics.ATTR_STAGE.getKey(),
                DpMetrics.ATTR_OUTCOME.getKey());

        final List<String> queryMetricNames = List.of(
                DpMetrics.METRIC_QUERY_STAGE_DURATION,
                DpMetrics.METRIC_QUERY_REQUESTS,
                DpMetrics.METRIC_QUERY_BUCKETS,
                DpMetrics.METRIC_QUERY_RESPONSE_MESSAGES,
                DpMetrics.METRIC_QUERY_RESPONSE_BYTES);

        for (String metricName : queryMetricNames) {
            final MetricData metric = metricNamed(metricName);
            assertNotNull(metricName + " was not recorded", metric);
            metric.getData().getPoints().forEach(point ->
                    point.getAttributes().forEach((key, value) -> {
                        assertTrue(
                                metricName + " carries attribute outside the D8 vocabulary: "
                                        + key.getKey(),
                                allowedKeys.contains(key.getKey()));
                        assertFalse(
                                "a PV name leaked into " + metricName + "." + key.getKey(),
                                String.valueOf(value).contains(PV_1));
                    }));
        }
    }
}
