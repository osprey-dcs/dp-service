package com.ospreydcs.dp.service.common.grpc;

import io.grpc.stub.ServerCallStreamObserver;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

/**
 * A {@link ServerCallStreamObserver} whose readiness and cancellation are test-controlled, for
 * driving the streaming dispatchers through {@link OutboundReadinessGate} without a gRPC server.
 * Records every message, whether each arrived while the observer reported not-ready (a flow
 * control violation), and completion.
 */
public class FakeServerCallStreamObserver<T> extends ServerCallStreamObserver<T> {

    public final AtomicBoolean ready = new AtomicBoolean(true);
    public final AtomicBoolean cancelled = new AtomicBoolean(false);
    public final AtomicReference<Runnable> onReady = new AtomicReference<>();
    public final AtomicReference<Runnable> onCancel = new AtomicReference<>();
    public final List<T> messages = new ArrayList<>();
    public int sentWhileNotReady = 0;
    public boolean completed = false;
    public Throwable error = null;

    /** Called on every onNext, after the message is recorded; a hook for tests to flip readiness. */
    private Runnable afterNext = () -> { };

    public void setAfterNext(Runnable afterNext) {
        this.afterNext = afterNext;
    }

    @Override public boolean isCancelled() { return cancelled.get(); }
    @Override public void setOnCancelHandler(Runnable onCancelHandler) { onCancel.set(onCancelHandler); }
    @Override public void setCompression(String compression) { }
    @Override public boolean isReady() { return ready.get(); }
    @Override public void setOnReadyHandler(Runnable onReadyHandler) { onReady.set(onReadyHandler); }
    @Override public void disableAutoInboundFlowControl() { }
    @Override public void request(int count) { }
    @Override public void setMessageCompression(boolean enable) { }

    @Override
    public synchronized void onNext(T value) {
        if (!ready.get()) {
            sentWhileNotReady++;
        }
        messages.add(value);
        afterNext.run();
    }

    @Override public void onError(Throwable t) { error = t; }
    @Override public void onCompleted() { completed = true; }

    /** Cancels the call and fires the registered cancel handler, as gRPC would. */
    public void cancel() {
        cancelled.set(true);
        final Runnable handler = onCancel.get();
        if (handler != null) {
            handler.run();
        }
    }

    /** Marks the transport ready again and fires the registered ready handler, as gRPC would. */
    public void becomeReady() {
        ready.set(true);
        final Runnable handler = onReady.get();
        if (handler != null) {
            handler.run();
        }
    }
}
