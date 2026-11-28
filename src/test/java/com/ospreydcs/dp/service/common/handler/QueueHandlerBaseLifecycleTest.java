package com.ospreydcs.dp.service.common.handler;

import com.ospreydcs.dp.service.common.telemetry.DpMetrics;
import org.junit.Test;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Covers the two things about a handler's start and stop that the background initialization of
 * issue #293 depends on: {@code fini()} is safe on a handler that has not finished {@code init()},
 * and {@code init()} registers no JVM shutdown hook.
 */
public class QueueHandlerBaseLifecycleTest {

    private static final String INIT_DONE = "HANDLER_INIT_DONE";
    private static final String FINI_RAN = "HANDLER_FINI_RAN";

    private static class TestHandler extends QueueHandlerBase {

        final CountDownLatch initEntered = new CountDownLatch(1);
        final CountDownLatch releaseInit = new CountDownLatch(1);
        private final boolean blockInInit;

        TestHandler(boolean blockInInit) {
            this.blockInInit = blockInInit;
        }

        @Override
        protected boolean init_() {
            initEntered.countDown();
            if (blockInInit) {
                try {
                    releaseInit.await(60, TimeUnit.SECONDS);
                } catch (InterruptedException ex) {
                    Thread.currentThread().interrupt();
                    return false;
                }
            }
            return true;
        }

        @Override
        protected boolean fini_() {
            System.out.println(FINI_RAN);
            return true;
        }

        @Override
        protected int getNumWorkers_() {
            return 1;
        }

        @Override
        protected String getServiceName_() {
            return DpMetrics.SERVICE_QUERY;
        }
    }

    @Test
    public void testFiniOnAHandlerStillInsideInitDoesNotThrow() throws Exception {

        // init_() is where a handler with a Mongo client runs the schema migrations, and the
        // worker pool is created only after it returns. Before #293, fini() at this point threw
        // NullPointerException, which on the shutdown hook's thread skipped everything after it.
        final TestHandler handler = new TestHandler(true);
        final Thread initThread = new Thread(handler::init, "test-init");
        initThread.start();
        try {
            assertTrue(handler.initEntered.await(30, TimeUnit.SECONDS));
            assertTrue(handler.fini());
            assertTrue(handler.getShutdownRequested());
        } finally {
            handler.releaseInit.countDown();
            initThread.join(TimeUnit.SECONDS.toMillis(30));
        }
        assertFalse(initThread.isAlive());
    }

    /** Run in a JVM of its own: initializes a handler and exits without calling {@code fini()}. */
    public static class ExitWithoutFini {
        public static void main(String[] args) {
            final TestHandler handler = new TestHandler(false);
            if (!handler.init()) {
                System.exit(2);
            }
            System.out.println(INIT_DONE);
            System.exit(0);
        }
    }

    @Test
    public void testInitRegistersNoShutdownHook() throws Exception {

        // A hook cannot be listed from inside the JVM, so this watches what one would do: in a
        // JVM that exits without calling fini(), a handler with a hook of its own runs fini()
        // on the way out. The server stops the handler now, after it has drained (#293 D11).
        final String java = System.getProperty("java.home") + File.separator + "bin" + File.separator + "java";
        final Process process = new ProcessBuilder(
                java, "-cp", System.getProperty("java.class.path"), ExitWithoutFini.class.getName())
                .redirectErrorStream(true)
                .start();
        final String output = new String(process.getInputStream().readAllBytes(), StandardCharsets.UTF_8);
        assertTrue("the child JVM did not exit", process.waitFor(60, TimeUnit.SECONDS));

        assertEquals("child JVM output: " + output, 0, process.exitValue());
        assertTrue("child JVM output: " + output, output.contains(INIT_DONE));
        assertFalse(
                "fini() ran at JVM exit, so init() registered a shutdown hook: " + output,
                output.contains(FINI_RAN));
    }
}
