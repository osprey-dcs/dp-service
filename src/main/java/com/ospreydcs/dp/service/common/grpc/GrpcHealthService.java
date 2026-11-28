package com.ospreydcs.dp.service.common.grpc;

import com.google.protobuf.CodedInputStream;
import com.google.protobuf.WireFormat;
import io.grpc.MethodDescriptor;
import io.grpc.ServerServiceDefinition;
import io.grpc.ServiceDescriptor;
import io.grpc.Status;
import io.grpc.stub.ServerCallStreamObserver;
import io.grpc.stub.ServerCalls;
import io.grpc.stub.StreamObserver;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * The standard gRPC health service, {@code grpc.health.v1.Health}, written by hand (issue #293).
 *
 * <p><b>Why this is not {@code io.grpc.protobuf.services.HealthStatusManager}.</b> That class is
 * on the classpath, in {@code grpc-services}, and it does not work here. Its request and response
 * messages are code generated for protobuf 3.x and extend {@code GeneratedMessageV3.Builder},
 * which protobuf-java 4.27.2 (the version dp-grpc pins) does not contain. Measured on this
 * classpath with grpc-services 1.65.1: {@code setStatus()} throws {@code NoClassDefFoundError},
 * and a {@code Check} ends {@code UNKNOWN}. <b>Do not replace this class with the stock one</b>
 * without running {@code GrpcHealthServiceTest} against it; the failure is at first use, not at
 * compile time, and from a probe's side it reads as an unhealthy service.
 *
 * <p>The two messages are small enough to encode directly, so nothing here depends on generated
 * code: a request is one string field, a response is one enum field. The service name, method
 * names and encoding are those of {@code health.proto}, so a Kubernetes gRPC probe,
 * {@code grpc_health_probe} and a gRPC client's health checking all work against it.
 * {@code GrpcHealthServiceTest} pins the bytes.
 *
 * <p>Semantics follow the stock manager where a caller can see them:
 * <ul>
 *   <li>{@code Check} of a name that was never set ends {@code NOT_FOUND}.</li>
 *   <li>{@code Watch} sends the current status at once ({@code SERVICE_UNKNOWN} for a name never
 *       set), then each change, and stays open until the client cancels.</li>
 *   <li>{@link #enterTerminalState()} sets every registered name to {@code NOT_SERVING} and
 *       ignores every later change. That includes a name meant to be always {@code SERVING}.</li>
 * </ul>
 * Unlike the stock manager, a new instance has <b>no</b> status for {@code ""}: the stock one
 * starts it at {@code SERVING}, which is the wrong answer for a server that has not initialized.
 */
public final class GrpcHealthService {

    public static final String SERVICE_NAME = "grpc.health.v1.Health";

    /** The health service name that stands for the server as a whole. */
    public static final String SERVICE_NAME_ALL_SERVICES = "";

    /** {@code grpc.health.v1.HealthCheckResponse.ServingStatus}, with its wire numbers. */
    public enum ServingStatus {
        UNKNOWN(0),
        SERVING(1),
        NOT_SERVING(2),
        SERVICE_UNKNOWN(3);

        private final int number;

        ServingStatus(int number) {
            this.number = number;
        }

        public int number() {
            return number;
        }

        static ServingStatus forNumber(int number) {
            for (ServingStatus status : values()) {
                if (status.number == number) {
                    return status;
                }
            }
            return UNKNOWN;
        }
    }

    // HealthCheckRequest { string service = 1; }
    private static final int REQUEST_SERVICE_TAG = (1 << 3) | WireFormat.WIRETYPE_LENGTH_DELIMITED;
    // HealthCheckResponse { ServingStatus status = 1; }
    private static final int RESPONSE_STATUS_TAG = (1 << 3) | WireFormat.WIRETYPE_VARINT;

    /** Marshals a {@code HealthCheckRequest}, as the service name it carries. */
    static final MethodDescriptor.Marshaller<String> REQUEST_MARSHALLER =
            new MethodDescriptor.Marshaller<>() {
                @Override
                public InputStream stream(String service) {
                    if (service.isEmpty()) {
                        return new ByteArrayInputStream(new byte[0]); // proto3 omits a default
                    }
                    final byte[] utf8 = service.getBytes(java.nio.charset.StandardCharsets.UTF_8);
                    final java.io.ByteArrayOutputStream out = new java.io.ByteArrayOutputStream();
                    final com.google.protobuf.CodedOutputStream coded =
                            com.google.protobuf.CodedOutputStream.newInstance(out);
                    try {
                        coded.writeByteArray(1, utf8);
                        coded.flush();
                    } catch (IOException ex) {
                        throw new IllegalStateException(ex);
                    }
                    return new ByteArrayInputStream(out.toByteArray());
                }

                @Override
                public String parse(InputStream stream) {
                    try {
                        final CodedInputStream coded = CodedInputStream.newInstance(stream);
                        String service = "";
                        int tag;
                        while ((tag = coded.readTag()) != 0) {
                            if (tag == REQUEST_SERVICE_TAG) {
                                service = coded.readStringRequireUtf8();
                            } else if (!coded.skipField(tag)) {
                                break;
                            }
                        }
                        return service;
                    } catch (IOException ex) {
                        throw Status.INVALID_ARGUMENT
                                .withDescription("malformed HealthCheckRequest")
                                .withCause(ex)
                                .asRuntimeException();
                    }
                }
            };

    /** Marshals a {@code HealthCheckResponse}, as the status it carries. */
    static final MethodDescriptor.Marshaller<ServingStatus> RESPONSE_MARSHALLER =
            new MethodDescriptor.Marshaller<>() {
                @Override
                public InputStream stream(ServingStatus status) {
                    // proto3 omits a default value, and UNKNOWN is 0
                    return new ByteArrayInputStream(status == ServingStatus.UNKNOWN
                            ? new byte[0]
                            : new byte[] {(byte) RESPONSE_STATUS_TAG, (byte) status.number()});
                }

                @Override
                public ServingStatus parse(InputStream stream) {
                    try {
                        final CodedInputStream coded = CodedInputStream.newInstance(stream);
                        ServingStatus status = ServingStatus.UNKNOWN;
                        int tag;
                        while ((tag = coded.readTag()) != 0) {
                            if (tag == RESPONSE_STATUS_TAG) {
                                status = ServingStatus.forNumber(coded.readEnum());
                            } else if (!coded.skipField(tag)) {
                                break;
                            }
                        }
                        return status;
                    } catch (IOException ex) {
                        throw Status.INTERNAL
                                .withDescription("malformed HealthCheckResponse")
                                .withCause(ex)
                                .asRuntimeException();
                    }
                }
            };

    /** {@code grpc.health.v1.Health/Check}. Public so a client, or a test, can call it. */
    public static final MethodDescriptor<String, ServingStatus> CHECK_METHOD =
            MethodDescriptor.<String, ServingStatus>newBuilder()
                    .setType(MethodDescriptor.MethodType.UNARY)
                    .setFullMethodName(MethodDescriptor.generateFullMethodName(SERVICE_NAME, "Check"))
                    .setRequestMarshaller(REQUEST_MARSHALLER)
                    .setResponseMarshaller(RESPONSE_MARSHALLER)
                    .build();

    /** {@code grpc.health.v1.Health/Watch}. */
    public static final MethodDescriptor<String, ServingStatus> WATCH_METHOD =
            MethodDescriptor.<String, ServingStatus>newBuilder()
                    .setType(MethodDescriptor.MethodType.SERVER_STREAMING)
                    .setFullMethodName(MethodDescriptor.generateFullMethodName(SERVICE_NAME, "Watch"))
                    .setRequestMarshaller(REQUEST_MARSHALLER)
                    .setResponseMarshaller(RESPONSE_MARSHALLER)
                    .build();

    private static final ServiceDescriptor SERVICE_DESCRIPTOR =
            ServiceDescriptor.newBuilder(SERVICE_NAME)
                    .addMethod(CHECK_METHOD)
                    .addMethod(WATCH_METHOD)
                    .build();

    private record Watcher(String service, StreamObserver<ServingStatus> observer) {
    }

    // Everything below is guarded by `lock`, the sends to watchers included: a StreamObserver
    // is not thread-safe, and gRPC's onNext() does not block.
    private final Object lock = new Object();
    private final Map<String, ServingStatus> statuses = new HashMap<>();
    private final List<Watcher> watchers = new ArrayList<>();
    private boolean terminal = false;

    /** Sets the status of one name. Ignored once {@link #enterTerminalState()} has been called. */
    public void setStatus(String service, ServingStatus status) {
        synchronized (lock) {
            if (terminal) {
                return;
            }
            setStatusLocked(service, status);
        }
    }

    /**
     * Sets every registered name to {@code NOT_SERVING} for good, so that a process that is
     * shutting down leaves a load balancer's endpoints and cannot be put back by a late change.
     */
    public void enterTerminalState() {
        synchronized (lock) {
            if (terminal) {
                return;
            }
            terminal = true;
            for (String service : List.copyOf(statuses.keySet())) {
                setStatusLocked(service, ServingStatus.NOT_SERVING);
            }
        }
    }

    private void setStatusLocked(String service, ServingStatus status) {
        final ServingStatus previous = statuses.put(service, status);
        if (previous == status) {
            return;
        }
        for (Watcher watcher : watchers) {
            if (watcher.service().equals(service)) {
                try {
                    watcher.observer().onNext(status);
                } catch (RuntimeException ex) {
                    // a watcher that has gone away must not stop the status from changing; its
                    // cancel handler removes it
                }
            }
        }
    }

    /** The status of one name, or null if it was never set. */
    public ServingStatus getStatus(String service) {
        synchronized (lock) {
            return statuses.get(service);
        }
    }

    private void check(String service, StreamObserver<ServingStatus> observer) {
        final ServingStatus status = getStatus(service);
        if (status == null) {
            observer.onError(Status.NOT_FOUND
                    .withDescription("unknown service " + service)
                    .asRuntimeException());
            return;
        }
        observer.onNext(status);
        observer.onCompleted();
    }

    private void watch(String service, StreamObserver<ServingStatus> observer) {
        final Watcher watcher = new Watcher(service, observer);
        synchronized (lock) {
            observer.onNext(statuses.getOrDefault(service, ServingStatus.SERVICE_UNKNOWN));
            watchers.add(watcher);
        }
        if (observer instanceof ServerCallStreamObserver<ServingStatus> serverObserver) {
            serverObserver.setOnCancelHandler(() -> {
                synchronized (lock) {
                    watchers.remove(watcher);
                }
            });
        }
    }

    /** The service to add to a server. It is added unwrapped: it must answer behind no gate. */
    public ServerServiceDefinition bindService() {
        return ServerServiceDefinition.builder(SERVICE_DESCRIPTOR)
                .addMethod(CHECK_METHOD, ServerCalls.asyncUnaryCall(this::check))
                .addMethod(WATCH_METHOD, ServerCalls.asyncServerStreamingCall(this::watch))
                .build();
    }
}
