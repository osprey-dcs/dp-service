package com.ospreydcs.dp.service.common.handler;

import com.ospreydcs.dp.service.common.telemetry.DpMetrics;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertTrue;

/**
 * A job that throws must not cost the handler a worker (issue #314).
 *
 * <p>The workers are long-running {@code Runnable}s submitted once to a fixed pool. When one ends,
 * the pool does start a replacement <em>thread</em>, but that thread waits on the pool's own
 * (empty) task queue rather than on the handler's {@code requestQueue}, so it never runs a job. A
 * handler that loses every worker this way keeps accepting requests and executes none of them.
 */
@RunWith(JUnit4.class)
public class QueueHandlerBaseWorkerSurvivalTest {

    private static final int NUM_WORKERS = 1;

    private TestHandler handler;

    private static class TestHandler extends QueueHandlerBase {
        @Override protected boolean init_() { return true; }
        @Override protected boolean fini_() { return true; }
        @Override protected int getNumWorkers_() { return NUM_WORKERS; }
        @Override protected String getServiceName_() { return DpMetrics.SERVICE_QUERY; }

        void submit(HandlerJob job) {
            enqueueJob(job, 1);
        }
    }

    private static class ThrowingJob extends HandlerJob {
        private final Throwable toThrow;
        final CountDownLatch started = new CountDownLatch(1);

        ThrowingJob(Throwable toThrow) {
            this.toThrow = toThrow;
        }

        @Override
        public void execute() {
            started.countDown();
            if (toThrow instanceof Error error) {
                throw error;
            }
            throw (RuntimeException) toThrow;
        }
    }

    private static class MarkerJob extends HandlerJob {
        final CountDownLatch started = new CountDownLatch(1);

        @Override
        public void execute() {
            started.countDown();
        }
    }

    @Before
    public void setUp() {
        handler = new TestHandler();
        assertTrue(handler.init());
    }

    @After
    public void tearDown() {
        handler.fini();
    }

    /** With one worker, the next job runs only if that worker survived the throw. */
    private void assertNextJobRunsAfter(Throwable thrown) throws InterruptedException {
        final ThrowingJob throwing = new ThrowingJob(thrown);
        handler.submit(throwing);
        assertTrue("throwing job did not start", throwing.started.await(10, TimeUnit.SECONDS));

        final MarkerJob next = new MarkerJob();
        handler.submit(next);
        assertTrue("the handler's only worker died with " + thrown.getClass().getSimpleName()
                        + ": the next job was queued but never executed",
                next.started.await(5, TimeUnit.SECONDS));
    }

    @Test
    public void testWorkerSurvivesARuntimeException() throws InterruptedException {
        assertNextJobRunsAfter(new IllegalStateException("simulated job failure"));
    }

    /**
     * The #314 mechanism, with a plain {@code Error} standing in for the {@code OutOfMemoryError}
     * of the incident: {@code executeJob} caught only {@code Exception}, so this ended the worker.
     */
    @Test
    public void testWorkerSurvivesAnError() throws InterruptedException {
        assertNextJobRunsAfter(new Error("simulated non-fatal Error"));
    }
}
