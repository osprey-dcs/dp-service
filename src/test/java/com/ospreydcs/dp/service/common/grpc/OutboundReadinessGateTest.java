package com.ospreydcs.dp.service.common.grpc;

import io.grpc.stub.StreamObserver;
import org.junit.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/**
 * Pins {@link OutboundReadinessGate} (issue #274, plan D8) against a hand-rolled
 * {@link ServerCallStreamObserver}: a plain observer yields a no-op gate; a ready observer passes
 * at once; a not-ready one blocks until the ready handler fires; cancellation and the timeout
 * return false.
 */
public class OutboundReadinessGateTest {

    @Test
    public void testPlainObserverIsNoOp() {
        final StreamObserver<Object> plain = new StreamObserver<>() {
            @Override public void onNext(Object value) { }
            @Override public void onError(Throwable t) { }
            @Override public void onCompleted() { }
        };
        final OutboundReadinessGate gate = OutboundReadinessGate.forObserver(plain, 1, "test");
        assertTrue(gate.isNoOp());
        assertTrue(gate.awaitReady());
    }

    @Test
    public void testReadyObserverPassesImmediately() {
        final FakeServerCallStreamObserver<Object> observer = new FakeServerCallStreamObserver<>();
        final OutboundReadinessGate gate = OutboundReadinessGate.forObserver(observer, 1, "test");
        assertFalse(gate.isNoOp());
        assertNotNull("the ready handler must be registered at construction", observer.onReady.get());
        assertTrue(gate.awaitReady());
    }

    @Test
    public void testNotReadyBlocksUntilTheHandlerFires() throws Exception {
        final FakeServerCallStreamObserver<Object> observer = new FakeServerCallStreamObserver<>();
        observer.ready.set(false);
        final OutboundReadinessGate gate = OutboundReadinessGate.forObserver(observer, 10, "test");

        final CountDownLatch entered = new CountDownLatch(1);
        final AtomicBoolean result = new AtomicBoolean(false);
        final Thread waiter = new Thread(() -> {
            entered.countDown();
            result.set(gate.awaitReady());
        });
        waiter.start();
        assertTrue(entered.await(5, TimeUnit.SECONDS));
        Thread.sleep(100);
        assertTrue("waiter must still be blocked while not ready", waiter.isAlive());

        observer.becomeReady();
        waiter.join(5_000);
        assertFalse("waiter must return once ready", waiter.isAlive());
        assertTrue(result.get());
    }

    @Test
    public void testCancelledReturnsFalse() {
        final FakeServerCallStreamObserver<Object> observer = new FakeServerCallStreamObserver<>();
        observer.ready.set(false);
        observer.cancelled.set(true);
        final OutboundReadinessGate gate = OutboundReadinessGate.forObserver(observer, 10, "test");
        assertFalse(gate.awaitReady());
    }

    /**
     * A cancellation arriving while the worker is already blocked must wake it, not be discovered
     * at the timeout. gRPC dispatches cancel and ready separately, so before the gate registered a
     * cancel handler this case slept out the whole bound (measured at the full timeout), holding a
     * worker for a call with no reader. The timeout here is 30 s, so a regression fails by taking
     * ~30 s rather than by hanging the suite.
     */
    @Test
    public void testCancelWhileBlockedWakesTheWaiter() throws Exception {
        final FakeServerCallStreamObserver<Object> observer = new FakeServerCallStreamObserver<>();
        observer.ready.set(false);
        final OutboundReadinessGate gate = OutboundReadinessGate.forObserver(observer, 30, "test");
        assertNotNull("the cancel handler must be registered at construction", observer.onCancel.get());

        final CountDownLatch entered = new CountDownLatch(1);
        final AtomicBoolean result = new AtomicBoolean(true);
        final Thread waiter = new Thread(() -> {
            entered.countDown();
            result.set(gate.awaitReady());
        });
        waiter.start();
        assertTrue(entered.await(5, TimeUnit.SECONDS));
        Thread.sleep(100);
        assertTrue("waiter must be blocked while not ready and not cancelled", waiter.isAlive());

        final long start = System.nanoTime();
        observer.cancel();
        waiter.join(5_000);

        assertFalse("cancellation must wake the waiter", waiter.isAlive());
        assertFalse("awaitReady must report refusal", result.get());
        assertTrue("must not have waited for the timeout",
                System.nanoTime() - start < TimeUnit.SECONDS.toNanos(10));
    }

    @Test
    public void testTimeoutReturnsFalse() {
        final FakeServerCallStreamObserver<Object> observer = new FakeServerCallStreamObserver<>();
        observer.ready.set(false);
        final OutboundReadinessGate gate = OutboundReadinessGate.forObserver(observer, 1, "test");
        final long start = System.nanoTime();
        assertFalse(gate.awaitReady());
        assertTrue("must have waited for the timeout", System.nanoTime() - start >= TimeUnit.MILLISECONDS.toNanos(900));
    }
}
