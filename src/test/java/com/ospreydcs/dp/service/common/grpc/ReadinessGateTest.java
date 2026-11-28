package com.ospreydcs.dp.service.common.grpc;

import com.ospreydcs.dp.grpc.v1.query.DpQueryServiceGrpc;
import com.ospreydcs.dp.grpc.v1.query.QueryDataRequest;
import com.ospreydcs.dp.grpc.v1.query.QueryDataResponse;
import io.grpc.ManagedChannel;
import io.grpc.Server;
import io.grpc.ServerInterceptors;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import io.grpc.stub.StreamObserver;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.Iterator;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Covers the gate that stands in front of a service while it initializes (issue #293): closed,
 * no call reaches the service and each ends {@code UNAVAILABLE} with the reason; open, calls pass.
 *
 * <p>The service behind the gate is a counting implementation of two query methods, one unary
 * and one server-streaming.
 */
public class ReadinessGateTest {

    private final AtomicInteger callsReachingService = new AtomicInteger();
    private volatile String reason = "service is initializing: testing";

    private ReadinessGate gate;
    private Server server;
    private ManagedChannel channel;

    private class CountingService extends DpQueryServiceGrpc.DpQueryServiceImplBase {

        @Override
        public void queryData(QueryDataRequest request, StreamObserver<QueryDataResponse> observer) {
            callsReachingService.incrementAndGet();
            observer.onNext(QueryDataResponse.getDefaultInstance());
            observer.onCompleted();
        }

        @Override
        public void queryDataStream(
                QueryDataRequest request, StreamObserver<QueryDataResponse> observer) {
            callsReachingService.incrementAndGet();
            observer.onNext(QueryDataResponse.getDefaultInstance());
            observer.onCompleted();
        }
    }

    @Before
    public void setUp() throws Exception {
        gate = new ReadinessGate(() -> reason);
        final String name = InProcessServerBuilder.generateName();
        server = InProcessServerBuilder.forName(name)
                .directExecutor()
                .addService(ServerInterceptors.intercept(new CountingService(), gate))
                .build()
                .start();
        channel = InProcessChannelBuilder.forName(name).directExecutor().build();
    }

    @After
    public void tearDown() throws Exception {
        channel.shutdownNow().awaitTermination(10, TimeUnit.SECONDS);
        server.shutdownNow().awaitTermination(10, TimeUnit.SECONDS);
    }

    private static void assertUnavailable(StatusRuntimeException ex, String expectedReason) {
        assertEquals(Status.Code.UNAVAILABLE, ex.getStatus().getCode());
        assertEquals(expectedReason, ex.getStatus().getDescription());
    }

    @Test
    public void testClosedGateEndsAUnaryCallUnavailableWithoutReachingTheService() {
        assertFalse(gate.isOpen());
        try {
            DpQueryServiceGrpc.newBlockingStub(channel).queryData(QueryDataRequest.getDefaultInstance());
            fail("a closed gate must reject the call");
        } catch (StatusRuntimeException ex) {
            assertUnavailable(ex, "service is initializing: testing");
        }
        assertEquals("the call must not reach the service", 0, callsReachingService.get());
    }

    @Test
    public void testClosedGateEndsAStreamingCallUnavailableWithoutReachingTheService() {
        try {
            final Iterator<QueryDataResponse> responses = DpQueryServiceGrpc.newBlockingStub(channel)
                    .queryDataStream(QueryDataRequest.getDefaultInstance());
            responses.hasNext();
            fail("a closed gate must reject the call");
        } catch (StatusRuntimeException ex) {
            assertUnavailable(ex, "service is initializing: testing");
        }
        assertEquals("the call must not reach the service", 0, callsReachingService.get());
    }

    @Test
    public void testTheReasonIsReadAtEachRejectedCall() {
        reason = "service is initializing: applying schema migration version 4 (target 5)";
        try {
            DpQueryServiceGrpc.newBlockingStub(channel).queryData(QueryDataRequest.getDefaultInstance());
            fail("a closed gate must reject the call");
        } catch (StatusRuntimeException ex) {
            assertUnavailable(
                    ex, "service is initializing: applying schema migration version 4 (target 5)");
        }
    }

    @Test
    public void testAReasonThatCannotBeProducedStillRejects() {
        final ReadinessGate failing = new ReadinessGate(() -> {
            throw new IllegalStateException("deliberate");
        });
        // the shared server's gate has a working supplier, so this one gets a server of its own
        final String name = InProcessServerBuilder.generateName();
        Server second = null;
        ManagedChannel secondChannel = null;
        try {
            second = InProcessServerBuilder.forName(name).directExecutor()
                    .addService(ServerInterceptors.intercept(new CountingService(), failing))
                    .build().start();
            secondChannel = InProcessChannelBuilder.forName(name).directExecutor().build();
            DpQueryServiceGrpc.newBlockingStub(secondChannel)
                    .queryData(QueryDataRequest.getDefaultInstance());
            fail("a closed gate must reject the call");
        } catch (StatusRuntimeException ex) {
            assertUnavailable(ex, ReadinessGate.DEFAULT_DESCRIPTION);
        } catch (java.io.IOException ex) {
            throw new AssertionError(ex);
        } finally {
            if (secondChannel != null) {
                secondChannel.shutdownNow();
            }
            if (second != null) {
                second.shutdownNow();
            }
        }
        assertEquals(0, callsReachingService.get());
    }

    @Test
    public void testOpenGatePassesUnaryAndStreamingCalls() {
        gate.open();
        assertTrue(gate.isOpen());

        DpQueryServiceGrpc.newBlockingStub(channel).queryData(QueryDataRequest.getDefaultInstance());
        final Iterator<QueryDataResponse> responses = DpQueryServiceGrpc.newBlockingStub(channel)
                    .queryDataStream(QueryDataRequest.getDefaultInstance());
        assertTrue(responses.hasNext());

        assertEquals(2, callsReachingService.get());
    }
}
