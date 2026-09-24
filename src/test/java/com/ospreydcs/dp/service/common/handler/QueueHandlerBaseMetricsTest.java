package com.ospreydcs.dp.service.common.handler;

import com.ospreydcs.dp.service.common.telemetry.DpMetrics;
import com.ospreydcs.dp.service.common.telemetry.DpTelemetry;
import io.opentelemetry.sdk.OpenTelemetrySdk;
import io.opentelemetry.sdk.metrics.SdkMeterProvider;
import io.opentelemetry.sdk.metrics.data.HistogramPointData;
import io.opentelemetry.sdk.metrics.data.MetricData;
import io.opentelemetry.sdk.testing.exporter.InMemoryMetricReader;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.util.Collection;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/**
 * Covers the D2 handler instrumentation against a real {@link QueueHandlerBase} subclass driven by
 * its real worker pool (issue #212).
 *
 * <p>The measurements here are the ones every service shares, so a break is a break in all four.
 * The case worth the most is {@link #testWorkersActiveNetsToZeroWhenAJobThrows()}: the worker
 * deliberately swallows whatever {@code execute()} throws, and a decrement on the normal path only
 * would leak a permanent +1 per escape, until the gauge that exists to reveal saturation read as
 * saturated on an idle service.
 */
@RunWith(JUnit4.class)
public class QueueHandlerBaseMetricsTest {

    private static final int NUM_WORKERS = 1;
    private static final String SERVICE_NAME = DpMetrics.SERVICE_QUERY;

    private InMemoryMetricReader metricReader;
    private OpenTelemetrySdk telemetrySdk;
    private TestHandler handler;

    /** Minimal handler: one worker, no backing service to init. */
    private static class TestHandler extends QueueHandlerBase {

        @Override
        protected boolean init_() {
            return true;
        }

        @Override
        protected boolean fini_() {
            return true;
        }

        @Override
        protected int getNumWorkers_() {
            return NUM_WORKERS;
        }

        @Override
        protected String getServiceName_() {
            return SERVICE_NAME;
        }

        /** Exposes the shared enqueue path, which is protected on the base class. */
        void submit(HandlerJob job) {
            enqueueJob(job, 1);
        }
    }

    /** Runs the supplied body, counting down when it is finished either way. */
    private static class TestJob extends HandlerJob {

        private final Runnable body;
        private final CountDownLatch finished = new CountDownLatch(1);

        TestJob(Runnable body) {
            this.body = body;
        }

        @Override
        public void execute() {
            try {
                body.run();
            } finally {
                finished.countDown();
            }
        }

        /**
         * Waits until the worker has finished <em>recording</em> this job, not merely until
         * {@code execute()} returned. The latch counts down inside {@code execute()}, but
         * {@code job.duration} and the {@code workers.active} decrement are recorded afterwards in
         * the worker's own {@code finally} — so awaiting the latch alone races the measurement this
         * class exists to assert on, and the assertion would fail intermittently against correct
         * code. The barrier job that follows is enqueued behind this one on a single-worker
         * handler, so its start proves the worker left the previous job's finally block.
         */
        void awaitFinished() throws InterruptedException {
            assertTrue("job did not finish", finished.await(30, TimeUnit.SECONDS));
        }
    }

    /**
     * A job of its own class, enqueued to prove the worker has left the previous job's
     * {@code finally} block.
     *
     * <p>Needed because a {@code TestJob}'s latch counts down inside {@code execute()}, while
     * {@code dp.handler.job.duration} and the {@code workers.active} decrement are recorded
     * afterwards in the worker's {@code finally}. Awaiting the latch alone therefore races the
     * measurement these tests assert on, and the assertions would fail intermittently against
     * correct code. A distinct class rather than another {@code TestJob} so the barrier gets its
     * own {@code dp.job} point and cannot inflate the counts under test.
     *
     * <p>That separation holds only for the job-attributed histograms. {@code workers.active} is
     * attributed by service alone, and the barrier is itself inside {@code execute()} when its
     * latch releases, so it is still counted there; a gauge assertion has to poll past it.
     */
    private static class BarrierJob extends HandlerJob {

        private final CountDownLatch started = new CountDownLatch(1);

        @Override
        public void execute() {
            started.countDown();
        }
    }

    /**
     * Blocks until every measurement for the jobs submitted so far has been recorded. Safe only on
     * a single-worker handler, where a job enqueued behind them cannot start until they are done.
     */
    private void awaitRecorded() throws InterruptedException {
        final BarrierJob barrier = new BarrierJob();
        handler.submit(barrier);
        assertTrue("barrier job did not run", barrier.started.await(30, TimeUnit.SECONDS));
    }

    @Before
    public void setUp() {
        DpTelemetry.resetForTest();
        metricReader = InMemoryMetricReader.create();
        telemetrySdk = OpenTelemetrySdk.builder()
                .setMeterProvider(
                        SdkMeterProvider.builder().registerMetricReader(metricReader).build())
                .build();
        DpTelemetry.initForTest(telemetrySdk);

        handler = new TestHandler();
        assertTrue(handler.init());
    }

    @After
    public void tearDown() {
        if (handler != null) {
            handler.fini();
            handler = null;
        }
        DpTelemetry.resetForTest();
        if (telemetrySdk != null) {
            telemetrySdk.close();
            telemetrySdk = null;
        }
        metricReader = null;
    }

    private MetricData metricNamed(String name) {
        final Collection<MetricData> metrics = metricReader.collectAllMetrics();
        return metrics.stream()
                .filter(metric -> metric.getName().equals(name))
                .findFirst()
                .orElse(null);
    }

    private HistogramPointData histogramPoint(String name, String jobClassName) {
        final MetricData metric = metricNamed(name);
        assertNotNull(name + " was not recorded", metric);
        return metric.getHistogramData().getPoints().stream()
                .filter(point -> jobClassName.equals(point.getAttributes().get(DpMetrics.ATTR_JOB)))
                .findFirst()
                .orElseThrow(() -> new AssertionError(
                        name + " has no point for dp.job=" + jobClassName));
    }

    /**
     * A completed job records a queue wait and a duration, both attributed by service and job
     * class, in seconds. The duration is bounded on both sides against the deliberate 200 ms hold:
     * an upper bound alone would pass if nanos were recorded as if they were seconds, and a lower
     * bound alone would pass if they were recorded as millis.
     */
    @Test
    public void testCompletedJobRecordsQueueWaitAndDuration() throws Exception {

        final TestJob job = new TestJob(() -> sleep(200));
        handler.submit(job);
        job.awaitFinished();
        awaitRecorded();

        final String jobName = TestJob.class.getSimpleName();

        final HistogramPointData duration =
                histogramPoint(DpMetrics.METRIC_HANDLER_JOB_DURATION, jobName);
        assertEquals(1, duration.getCount());
        assertTrue(
                "job duration " + duration.getSum() + "s is not seconds-scaled",
                duration.getSum() >= 0.2 && duration.getSum() < 10.0);
        assertEquals(SERVICE_NAME, duration.getAttributes().get(DpMetrics.ATTR_SERVICE));

        final HistogramPointData queueWait =
                histogramPoint(DpMetrics.METRIC_HANDLER_QUEUE_WAIT, jobName);
        assertEquals(1, queueWait.getCount());
        assertEquals(SERVICE_NAME, queueWait.getAttributes().get(DpMetrics.ATTR_SERVICE));

        assertEquals(
                DpMetrics.UNIT_SECONDS, metricNamed(DpMetrics.METRIC_HANDLER_JOB_DURATION).getUnit());

        // the value the worker recorded and the value a job can read must be the same number, so a
        // dashboard comparing dp.handler.queue.wait against the query stage breakdown is not
        // comparing two slightly different measurements
        assertEquals(
                DpMetrics.nanosToSeconds(job.getQueueWaitNanos()),
                queueWait.getSum(),
                0.001);
    }

    /**
     * A job held in the queue behind a running one records a wait at least as long as the hold.
     * With one worker the second job cannot start until the first returns, so the hold time is a
     * genuine lower bound rather than a timing coincidence.
     */
    @Test
    public void testQueuedJobRecordsTheWaitItActuallyIncurred() throws Exception {

        final CountDownLatch release = new CountDownLatch(1);
        final CountDownLatch firstStarted = new CountDownLatch(1);

        final TestJob blocking = new TestJob(() -> {
            firstStarted.countDown();
            try {
                release.await(30, TimeUnit.SECONDS);
            } catch (InterruptedException ex) {
                Thread.currentThread().interrupt();
            }
        });
        handler.submit(blocking);
        assertTrue(firstStarted.await(30, TimeUnit.SECONDS));

        final TestJob queued = new TestJob(() -> { });
        // submitted while the single worker is occupied, so it waits for the release below
        handler.submit(queued);

        sleep(300);
        release.countDown();
        blocking.awaitFinished();
        queued.awaitFinished();
        awaitRecorded();

        // both jobs are the same class, so the two observations share one point
        final HistogramPointData queueWait =
                histogramPoint(DpMetrics.METRIC_HANDLER_QUEUE_WAIT, TestJob.class.getSimpleName());
        assertEquals(2, queueWait.getCount());
        assertTrue(
                "queued job's wait of at least 0.3s is not in the distribution: max="
                        + queueWait.getMax(),
                queueWait.getMax() >= 0.3);
    }

    /**
     * A job that throws still records its duration and still decrements {@code workers.active},
     * and the handler goes on serving. The escape itself is the documented hang failure mode — the
     * caller's stream is never answered — but the instrumentation must not compound it by leaving
     * the saturation gauge permanently wrong.
     */
    @Test
    public void testWorkersActiveNetsToZeroWhenAJobThrows() throws Exception {

        final TestJob throwing = new TestJob(() -> {
            throw new RuntimeException("deliberate test failure");
        });
        handler.submit(throwing);
        throwing.awaitFinished();

        // a normal job afterwards proves the worker survived the escape
        final TestJob afterwards = new TestJob(() -> { });
        handler.submit(afterwards);
        afterwards.awaitFinished();
        awaitRecorded();

        // Polled rather than read once. workers.active is attributed by service only, not by job,
        // so the barrier job awaitRecorded() just started holds the gauge at +1 until its own
        // finally runs, which races a single collection. A genuine leak is a permanent +1, so the
        // gauge never reaches 0 and the poll still fails.
        final long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
        long active;
        while (true) {
            final MetricData workersActive = metricNamed(DpMetrics.METRIC_HANDLER_WORKERS_ACTIVE);
            assertNotNull("workers.active was not recorded", workersActive);
            active = workersActive.getLongSumData().getPoints().stream()
                    .filter(point -> SERVICE_NAME.equals(
                            point.getAttributes().get(DpMetrics.ATTR_SERVICE)))
                    .mapToLong(point -> point.getValue())
                    .sum();
            if (active == 0L || System.nanoTime() > deadline) {
                break;
            }
            sleep(10);
        }
        assertEquals("workers.active leaked after an escaping job", 0L, active);

        final HistogramPointData duration =
                histogramPoint(DpMetrics.METRIC_HANDLER_JOB_DURATION, TestJob.class.getSimpleName());
        assertEquals("the throwing job's duration was not recorded", 2, duration.getCount());
    }

    /**
     * {@code workers.active} reads 1 while a job is inside {@code execute()}. Asserted from inside
     * the job so the reading is taken at the only instant it is non-zero on an otherwise idle
     * handler; a collection after the fact could only ever see the net zero above.
     */
    @Test
    public void testWorkersActiveIsOneDuringExecution() throws Exception {

        final long[] activeDuringExecution = new long[] {-1};

        final TestJob job = new TestJob(() -> {
            final MetricData metric = metricNamed(DpMetrics.METRIC_HANDLER_WORKERS_ACTIVE);
            if (metric != null) {
                activeDuringExecution[0] = metric.getLongSumData().getPoints().stream()
                        .filter(point -> SERVICE_NAME.equals(
                                point.getAttributes().get(DpMetrics.ATTR_SERVICE)))
                        .mapToLong(point -> point.getValue())
                        .sum();
            }
        });
        handler.submit(job);
        job.awaitFinished();

        assertEquals(1L, activeDuringExecution[0]);
    }

    /** The observable gauge reports the handler's configured worker count. */
    @Test
    public void testWorkersMaxGaugeReportsTheConfiguredCount() {

        final MetricData metric = metricNamed(DpMetrics.METRIC_HANDLER_WORKERS_MAX);
        assertNotNull("workers.max was not registered by init()", metric);
        final var point = metric.getLongGaugeData().getPoints().iterator().next();
        assertEquals(NUM_WORKERS, point.getValue());
        assertEquals(SERVICE_NAME, point.getAttributes().get(DpMetrics.ATTR_SERVICE));
    }

    /**
     * After {@code fini()} the gauge callback is closed, so it stops reporting for a handler that
     * no longer exists. An integration test builds many handlers in one JVM, and a leaked callback
     * would keep contributing a worker count for each of them.
     */
    @Test
    public void testWorkersMaxGaugeStopsReportingAfterFini() {

        assertNotNull(metricNamed(DpMetrics.METRIC_HANDLER_WORKERS_MAX));

        handler.fini();
        handler = null;

        org.junit.Assert.assertNull(
                "workers.max kept reporting after fini()",
                metricNamed(DpMetrics.METRIC_HANDLER_WORKERS_MAX));
    }

    /** The D8 guard: handler metrics carry only the declared attribute vocabulary. */
    @Test
    public void testHandlerMetricsCarryOnlyTheDeclaredAttributes() throws Exception {

        final TestJob job = new TestJob(() -> { });
        handler.submit(job);
        job.awaitFinished();
        awaitRecorded();

        for (String name : new String[] {
                DpMetrics.METRIC_HANDLER_QUEUE_WAIT, DpMetrics.METRIC_HANDLER_JOB_DURATION}) {
            final MetricData metric = metricNamed(name);
            assertNotNull(metric);
            for (HistogramPointData point : metric.getHistogramData().getPoints()) {
                for (var key : point.getAttributes().asMap().keySet()) {
                    assertTrue(
                            name + " carries unexpected attribute " + key.getKey(),
                            key.getKey().equals(DpMetrics.ATTR_SERVICE.getKey())
                                    || key.getKey().equals(DpMetrics.ATTR_JOB.getKey()));
                }
            }
        }
    }

    private static void sleep(long millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
        }
    }
}
