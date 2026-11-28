package com.ospreydcs.dp.service.common.grpc;

import com.ospreydcs.dp.service.common.grpc.GrpcHealthService.ServingStatus;
import io.grpc.CallOptions;
import io.grpc.ManagedChannel;
import io.grpc.MethodDescriptor;
import io.grpc.Server;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import io.grpc.stub.ClientCalls;
import io.grpc.stub.StreamObserver;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.fail;

/**
 * Covers the hand-written {@code grpc.health.v1.Health} service (issue #293).
 *
 * <p>The service exists because the stock one cannot run on this classpath (see
 * {@link GrpcHealthService}), and it is what a Kubernetes probe decides a pod's fate on. So the
 * tests that matter most are the ones that pin the <b>bytes</b>: the marshallers here are used by
 * both ends of every other test in the repo, and two ends that agree with each other prove
 * nothing about agreeing with {@code health.proto}.
 */
public class GrpcHealthServiceTest {

    private GrpcHealthService health;
    private Server server;
    private ManagedChannel channel;

    /** Calls with raw bytes, so that the server's marshallers are checked against the wire. */
    private static final MethodDescriptor.Marshaller<byte[]> RAW = new MethodDescriptor.Marshaller<>() {
        @Override
        public InputStream stream(byte[] value) {
            return new ByteArrayInputStream(value);
        }

        @Override
        public byte[] parse(InputStream stream) {
            try {
                return stream.readAllBytes();
            } catch (IOException ex) {
                throw new IllegalStateException(ex);
            }
        }
    };

    private static final MethodDescriptor<byte[], byte[]> RAW_CHECK =
            MethodDescriptor.<byte[], byte[]>newBuilder()
                    .setType(MethodDescriptor.MethodType.UNARY)
                    // spelled out, not taken from the class under test
                    .setFullMethodName("grpc.health.v1.Health/Check")
                    .setRequestMarshaller(RAW)
                    .setResponseMarshaller(RAW)
                    .build();

    @Before
    public void setUp() throws Exception {
        health = new GrpcHealthService();
        final String name = InProcessServerBuilder.generateName();
        server = InProcessServerBuilder.forName(name).directExecutor()
                .addService(health.bindService()).build().start();
        channel = InProcessChannelBuilder.forName(name).directExecutor().build();
    }

    @After
    public void tearDown() throws Exception {
        channel.shutdownNow().awaitTermination(10, TimeUnit.SECONDS);
        server.shutdownNow().awaitTermination(10, TimeUnit.SECONDS);
    }

    private byte[] rawCheck(byte[] request) {
        return ClientCalls.blockingUnaryCall(channel, RAW_CHECK, CallOptions.DEFAULT, request);
    }

    private ServingStatus check(String service) {
        return ClientCalls.blockingUnaryCall(
                channel, GrpcHealthService.CHECK_METHOD, CallOptions.DEFAULT, service);
    }

    // ------------------------------------------------------------------
    // the wire format of health.proto
    // ------------------------------------------------------------------

    @Test
    public void testCheckSpeaksTheWireFormatOfHealthProto() {

        health.setStatus("", ServingStatus.SERVING);
        health.setStatus("liveness", ServingStatus.NOT_SERVING);

        // HealthCheckRequest{} is zero bytes: a proto3 string at its default is not written.
        // HealthCheckResponse{status: SERVING} is field 1, varint, value 1.
        assertArrayEquals(new byte[] {0x08, 0x01}, rawCheck(new byte[0]));

        // HealthCheckRequest{service: "liveness"} is field 1, length-delimited, 8 bytes.
        final byte[] livenessRequest =
                {0x0A, 0x08, 'l', 'i', 'v', 'e', 'n', 'e', 's', 's'};
        assertArrayEquals(
                "NOT_SERVING is 2", new byte[] {0x08, 0x02}, rawCheck(livenessRequest));

        // An unknown field, as a later health.proto might add, is skipped.
        final byte[] withUnknownField =
                {0x0A, 0x08, 'l', 'i', 'v', 'e', 'n', 'e', 's', 's', 0x10, 0x07};
        assertArrayEquals(new byte[] {0x08, 0x02}, rawCheck(withUnknownField));
    }

    @Test
    public void testMarshallersWriteWhatAStockClientReads() throws IOException {

        assertArrayEquals(
                new byte[] {0x0A, 0x08, 'l', 'i', 'v', 'e', 'n', 'e', 's', 's'},
                GrpcHealthService.REQUEST_MARSHALLER.stream("liveness").readAllBytes());
        assertArrayEquals(
                new byte[0], GrpcHealthService.REQUEST_MARSHALLER.stream("").readAllBytes());

        assertArrayEquals(
                new byte[] {0x08, 0x01},
                GrpcHealthService.RESPONSE_MARSHALLER.stream(ServingStatus.SERVING).readAllBytes());
        assertArrayEquals(
                new byte[] {0x08, 0x03},
                GrpcHealthService.RESPONSE_MARSHALLER
                        .stream(ServingStatus.SERVICE_UNKNOWN).readAllBytes());
        assertArrayEquals(
                "UNKNOWN is the default and is not written",
                new byte[0],
                GrpcHealthService.RESPONSE_MARSHALLER.stream(ServingStatus.UNKNOWN).readAllBytes());

        assertEquals(
                ServingStatus.NOT_SERVING,
                GrpcHealthService.RESPONSE_MARSHALLER.parse(
                        new ByteArrayInputStream(new byte[] {0x08, 0x02})));
        assertEquals(
                ServingStatus.UNKNOWN,
                GrpcHealthService.RESPONSE_MARSHALLER.parse(new ByteArrayInputStream(new byte[0])));
    }

    @Test
    public void testServiceAndMethodNamesAreTheStandardOnes() {
        assertEquals("grpc.health.v1.Health", GrpcHealthService.SERVICE_NAME);
        assertEquals("grpc.health.v1.Health/Check", GrpcHealthService.CHECK_METHOD.getFullMethodName());
        assertEquals("grpc.health.v1.Health/Watch", GrpcHealthService.WATCH_METHOD.getFullMethodName());
        assertEquals(1, ServingStatus.SERVING.number());
        assertEquals(2, ServingStatus.NOT_SERVING.number());
        assertEquals(3, ServingStatus.SERVICE_UNKNOWN.number());
    }

    // ------------------------------------------------------------------
    // semantics
    // ------------------------------------------------------------------

    @Test
    public void testNewServiceHasNoStatusForTheServerName() {
        // The stock manager starts "" at SERVING, which would pass a readiness probe on a
        // server that has set nothing yet.
        assertNull(health.getStatus(""));
        try {
            check("");
            fail("a name that was never set is NOT_FOUND");
        } catch (StatusRuntimeException ex) {
            assertEquals(Status.Code.NOT_FOUND, ex.getStatus().getCode());
        }
    }

    @Test
    public void testCheckReportsEachNamesOwnStatus() {
        health.setStatus("", ServingStatus.NOT_SERVING);
        health.setStatus("liveness", ServingStatus.SERVING);
        assertEquals(ServingStatus.NOT_SERVING, check(""));
        assertEquals(ServingStatus.SERVING, check("liveness"));

        health.setStatus("", ServingStatus.SERVING);
        assertEquals(ServingStatus.SERVING, check(""));
    }

    @Test
    public void testTerminalStateSetsEveryNameNotServingAndIgnoresLaterChanges() {
        health.setStatus("", ServingStatus.SERVING);
        health.setStatus("liveness", ServingStatus.SERVING);

        health.enterTerminalState();
        assertEquals(ServingStatus.NOT_SERVING, check(""));
        assertEquals(ServingStatus.NOT_SERVING, check("liveness"));

        // an initialization that finishes during shutdown must not put the process back
        health.setStatus("", ServingStatus.SERVING);
        assertEquals(ServingStatus.NOT_SERVING, check(""));
    }

    @Test
    public void testWatchSendsTheCurrentStatusAndThenEachChange() throws Exception {

        health.setStatus("", ServingStatus.NOT_SERVING);

        final BlockingQueue<Object> received = new LinkedBlockingQueue<>();
        final StreamObserver<ServingStatus> observer = new StreamObserver<>() {
            @Override
            public void onNext(ServingStatus status) {
                received.add(status);
            }

            @Override
            public void onError(Throwable throwable) {
                received.add(throwable);
            }

            @Override
            public void onCompleted() {
                received.add("completed");
            }
        };
        ClientCalls.asyncServerStreamingCall(
                channel.newCall(GrpcHealthService.WATCH_METHOD, CallOptions.DEFAULT), "", observer);

        assertEquals(ServingStatus.NOT_SERVING, received.poll(10, TimeUnit.SECONDS));

        health.setStatus("", ServingStatus.SERVING);
        assertEquals(ServingStatus.SERVING, received.poll(10, TimeUnit.SECONDS));

        // the same status again is not a change, and another name is not this watcher's
        health.setStatus("", ServingStatus.SERVING);
        health.setStatus("liveness", ServingStatus.SERVING);
        health.enterTerminalState();
        assertEquals(ServingStatus.NOT_SERVING, received.poll(10, TimeUnit.SECONDS));
        assertNull("the stream stays open and sends nothing else", received.poll(200, TimeUnit.MILLISECONDS));
    }

    @Test
    public void testWatchOfAnUnknownNameReportsServiceUnknownUntilItIsSet() throws Exception {

        final BlockingQueue<Object> received = new LinkedBlockingQueue<>();
        ClientCalls.asyncServerStreamingCall(
                channel.newCall(GrpcHealthService.WATCH_METHOD, CallOptions.DEFAULT), "later",
                new StreamObserver<ServingStatus>() {
                    @Override
                    public void onNext(ServingStatus status) {
                        received.add(status);
                    }

                    @Override
                    public void onError(Throwable throwable) {
                        received.add(throwable);
                    }

                    @Override
                    public void onCompleted() {
                        received.add("completed");
                    }
                });

        assertEquals(ServingStatus.SERVICE_UNKNOWN, received.poll(10, TimeUnit.SECONDS));
        health.setStatus("later", ServingStatus.SERVING);
        assertEquals(ServingStatus.SERVING, received.poll(10, TimeUnit.SECONDS));
    }
}
