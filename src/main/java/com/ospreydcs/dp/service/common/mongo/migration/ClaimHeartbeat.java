package com.ospreydcs.dp.service.common.mongo.migration;

import com.mongodb.client.MongoDatabase;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/**
 * Keeps a held migration claim visibly alive by incrementing its heartbeat at an interval (issue
 * #292). A waiting process that stops seeing the heartbeat move takes the claim over.
 *
 * <p>Two failures of this class are both silent and both wrong, in opposite directions:
 *
 * <ul>
 *   <li><b>A heartbeat that outlives its migration keeps a dead claim fresh forever.</b> The thread
 *       is a daemon, so it cannot keep the JVM alive, and the runner stops it in a {@code finally}:
 *       if the migrating thread ends with an {@code Error} while some other non-daemon thread keeps
 *       the process up, a heartbeat stopped only on the normal paths would go on beating.</li>
 *   <li><b>A heartbeat that dies makes a live migrator look dead.</b> Each beat is therefore under
 *       its own {@code catch (Throwable)}: it is logged and the loop continues.</li>
 * </ul>
 *
 * <p>A beat that finds the claim under another token, or cleared, sets {@link #isClaimLost()} and
 * stops beating. It does not interrupt the migrating thread: the operation that thread is waiting
 * on would continue on the server regardless, and the runner checks the flag before starting the
 * next migration.
 */
class ClaimHeartbeat {

    private static final Logger logger = LogManager.getLogger();

    static final String THREAD_NAME = "dp-schema-migration-heartbeat";

    // How long stop() waits for a beat in flight before interrupting it.
    private static final long STOP_JOIN_MILLIS = 2_000L;

    private final MongoDatabase database;
    private final SchemaVersionMarker.Claim claim;
    private final long intervalMillis;

    private final CountDownLatch stopRequested = new CountDownLatch(1);
    private volatile boolean claimLost = false;
    private Thread thread;

    ClaimHeartbeat(MongoDatabase database, SchemaVersionMarker.Claim claim, long intervalMillis) {
        this.database = database;
        this.claim = claim;
        this.intervalMillis = intervalMillis;
    }

    synchronized void start() {
        if (thread != null) {
            return;
        }
        thread = new Thread(this::run, THREAD_NAME);
        thread.setDaemon(true);
        thread.start();
    }

    /**
     * Stops the heartbeat. Idempotent. Returns once the thread has ended, or after a bounded wait
     * if a beat is stuck in the driver; no beat is started after this is called.
     */
    synchronized void stop() {
        stopRequested.countDown();
        if (thread == null || thread == Thread.currentThread()) {
            return;
        }
        try {
            thread.join(STOP_JOIN_MILLIS);
            if (thread.isAlive()) {
                // a beat blocked on a write that cannot be acknowledged; the latch already stops
                // the loop, the interrupt only frees the thread
                thread.interrupt();
                thread.join(STOP_JOIN_MILLIS);
            }
        } catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
        }
    }

    /** True once a beat has found that the claim no longer belongs to this process. */
    boolean isClaimLost() {
        return claimLost;
    }

    private void run() {
        while (true) {
            try {
                if (stopRequested.await(intervalMillis, TimeUnit.MILLISECONDS)) {
                    return;
                }
                beat();
            } catch (ClaimLostException ex) {
                if (stopRequested.getCount() == 0) {
                    return;
                }
                claimLost = true;
                logger.error(
                        "schema migration heartbeat stopped, the claim is no longer held by this "
                                + "process: {}",
                        ex.getMessage(), ex);
                return;
            } catch (Throwable t) {
                // Anything else, including an interrupt, a driver error and an Error: keep
                // beating. Reporting is itself guarded, since a throw from here would end the
                // thread and the claim would read as abandoned.
                if (stopRequested.getCount() == 0) {
                    return;
                }
                try {
                    logger.error("schema migration heartbeat failed, will retry: {}", t.getMessage(), t);
                } catch (Throwable ignored) {
                    // nothing more can be done here
                }
            }
        }
    }

    /** One beat. Overridable so a test can make a beat fail. */
    void beat() throws Exception {
        SchemaVersionMarker.heartbeat(database, claim);
    }
}
