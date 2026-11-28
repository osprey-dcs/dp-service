package com.ospreydcs.dp.service.common.grpc;

import io.grpc.Metadata;
import io.grpc.ServerCall;
import io.grpc.ServerCallHandler;
import io.grpc.ServerInterceptor;
import io.grpc.Status;

import java.util.function.Supplier;

/**
 * Rejects every call to the service it wraps until the service has initialized (issue #293).
 *
 * <p>The gRPC port is bound before initialization, so that a probe has something to talk to during
 * a long schema migration. Until initialization succeeds the service implementation has no
 * handler, and a call that reached it would fail on a null field. While closed, this interceptor
 * ends each call with {@code UNAVAILABLE} and a description of what the process is doing, and the
 * call never reaches the service.
 *
 * <p>{@code UNAVAILABLE}, not the OK response carrying an {@code ExceptionalResult} that the
 * services use for their own rejections: it is the status clients and retry policies read as
 * "retry later", and it applies to every method, streaming ones included, with no per-method
 * response builder.
 *
 * <p>The health service is added to the server unwrapped; it must answer while this is closed.
 * The gate opens once and stays installed for the life of the process.
 */
public final class ReadinessGate implements ServerInterceptor {

    static final String DEFAULT_DESCRIPTION = "service is initializing";

    private final Supplier<String> description;
    private volatile boolean open = false;

    /**
     * @param description read at each rejected call, so that the reason follows what the process
     *     is doing at that moment
     */
    public ReadinessGate(Supplier<String> description) {
        this.description = description;
    }

    public void open() {
        open = true;
    }

    public boolean isOpen() {
        return open;
    }

    @Override
    public <ReqT, RespT> ServerCall.Listener<ReqT> interceptCall(
            ServerCall<ReqT, RespT> call, Metadata headers, ServerCallHandler<ReqT, RespT> next) {

        if (open) {
            return next.startCall(call, headers);
        }

        String reason;
        try {
            reason = description.get();
        } catch (RuntimeException ex) {
            // the rejection matters more than its wording
            reason = DEFAULT_DESCRIPTION;
        }
        call.close(Status.UNAVAILABLE.withDescription(reason), new Metadata());
        return new ServerCall.Listener<>() {
        };
    }
}
