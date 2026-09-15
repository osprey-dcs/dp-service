package com.ospreydcs.dp.service.common.grpc;

import io.grpc.stub.ServerCallStreamObserver;
import io.grpc.stub.StreamObserver;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Outbound flow control for a server-streaming response written from a worker thread (issue #274,
 * plan D8).
 *
 * <p>gRPC never blocks {@code onNext}: every message a slow client has not yet consumed is
 * buffered in the transport, so a dispatcher that iterates a large cursor and calls
 * {@code onNext} per chunk holds the whole result on the heap for as long as the client takes to
 * read it. {@link ServerCallStreamObserver#isReady()} is the transport's signal that the buffer has
 * room, and {@link ServerCallStreamObserver#setOnReadyHandler} the callback when it regains room.
 * This gate turns the two into one blocking call, {@link #awaitReady()}, that a dispatcher makes
 * before each streamed send.
 *
 * <p><b>Construct on the gRPC thread.</b> gRPC requires the ready handler to be registered before
 * the service method returns. The query handler constructs each stream dispatcher synchronously
 * inside the service method (the job that runs it is enqueued afterwards), so a dispatcher that
 * builds its gate in its constructor satisfies that without any interface change.
 *
 * <p><b>A plain {@link StreamObserver} yields a no-op gate.</b> The unit tests drive dispatchers
 * with hand-rolled observers; those have no readiness signal, and {@link #awaitReady()} returns
 * true immediately, as it did before flow control existed.
 *
 * <p>The trade: a slow reader now occupies the worker thread that serves it (bounded by
 * {@code timeoutSeconds}) instead of the heap. Seven workers blocked on seven slow clients is a
 * visible, bounded condition; a heap exhausted by one of them takes every query down.
 */
public class OutboundReadinessGate {

    private static final Logger logger = LogManager.getLogger();

    private static final OutboundReadinessGate NO_OP = new OutboundReadinessGate(null, 0);

    private final ServerCallStreamObserver<?> observer;
    private final long timeoutNanos;
    private final ReentrantLock lock = new ReentrantLock();
    private final Condition readyChanged = lock.newCondition();

    private OutboundReadinessGate(ServerCallStreamObserver<?> observer, long timeoutSeconds) {
        this.observer = observer;
        this.timeoutNanos = TimeUnit.SECONDS.toNanos(timeoutSeconds);
    }

    /**
     * A gate for {@code responseObserver}: a gating instance when it is a
     * {@link ServerCallStreamObserver} (registering the ready handler now, so call this on the
     * gRPC thread), otherwise a shared no-op.
     *
     * @param timeoutSeconds longest a single {@link #awaitReady()} waits before giving up
     */
    public static OutboundReadinessGate forObserver(StreamObserver<?> responseObserver, long timeoutSeconds) {
        if (!(responseObserver instanceof ServerCallStreamObserver<?> serverCallStreamObserver)) {
            return NO_OP;
        }
        final OutboundReadinessGate gate = new OutboundReadinessGate(serverCallStreamObserver, timeoutSeconds);
        serverCallStreamObserver.setOnReadyHandler(gate::signal);
        return gate;
    }

    /** True for the no-op gate returned for a plain {@link StreamObserver}. */
    public boolean isNoOp() {
        return observer == null;
    }

    private void signal() {
        lock.lock();
        try {
            readyChanged.signalAll();
        } finally {
            lock.unlock();
        }
    }

    /**
     * Blocks until the transport can accept another message. Returns true when it can; false when
     * the call was cancelled by the client or the timeout elapsed, in which case the caller should
     * stop sending, close its cursor, and return -- a cancelled call has no reader, and a timed-out
     * one is better left incomplete than given another buffered message.
     *
     * <p>Re-checks {@code isReady()} after every wake-up rather than trusting the handler: the
     * handler can fire before this thread reaches the wait (the signal is not lost, because the
     * check precedes each wait under the same lock), and it can also fire spuriously.
     */
    public boolean awaitReady() {
        if (observer == null) {
            return true;
        }
        long remainingNanos = timeoutNanos;
        lock.lock();
        try {
            while (true) {
                if (observer.isCancelled()) {
                    logger.debug("stream cancelled by the client while awaiting readiness");
                    return false;
                }
                if (observer.isReady()) {
                    return true;
                }
                if (remainingNanos <= 0) {
                    logger.warn("stream not ready for {} s; abandoning the response",
                            TimeUnit.NANOSECONDS.toSeconds(timeoutNanos));
                    return false;
                }
                try {
                    remainingNanos = readyChanged.awaitNanos(remainingNanos);
                } catch (InterruptedException ex) {
                    Thread.currentThread().interrupt();
                    return false;
                }
            }
        } finally {
            lock.unlock();
        }
    }
}
