package com.ospreydcs.dp.client;

import com.ospreydcs.dp.client.result.ApiResultStatus;
import com.ospreydcs.dp.grpc.v1.ingestion.IngestDataResponse;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import org.junit.Test;

import java.time.Duration;
import java.time.Instant;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Provides test coverage for the onError() handling of the client response observers, which must
 * release the latch their send method awaits.
 *
 * <p>An observer that records the failure but leaves its latch held does not fail any test that
 * only inspects the result: await() eventually expires and the call is still reported as a
 * failure, just a minute later and with the timeout message in place of the transport status.
 * These tests therefore assert on <em>elapsed time</em> as well as content, since the wait is the
 * defect.  The observers are plain StreamObservers, so no server, channel or database is involved.
 */
public class ResponseObserverErrorTest {

    // an await that returns within this bound cannot have waited out the one minute timeout the
    // observers use, while leaving ample headroom for a slow CI machine
    private static final Duration AWAIT_BOUND = Duration.ofSeconds(10);

    private static final StatusRuntimeException TRANSPORT_FAILURE =
            new StatusRuntimeException(Status.UNAVAILABLE.withDescription("io exception"));

    /*
     * Asserts that the given wait returns promptly rather than expiring, and returns how long it
     * took so the caller can report it.
     */
    private static Duration assertReturnsPromptly(String description, Runnable wait) {

        final Instant start = Instant.now();
        wait.run();
        final Duration elapsed = Duration.between(start, Instant.now());

        assertTrue(
                description + " did not return promptly, took " + elapsed.toMillis()
                        + "ms, which means onError left the latch held",
                elapsed.compareTo(AWAIT_BOUND) < 0);

        return elapsed;
    }

    /*
     * QueryTableResponseObserver.await() must return as soon as onError fires, carrying the gRPC
     * status rather than a timeout message.
     */
    @Test
    public void testQueryTableObserverReleasesLatchOnError() {

        final QueryClient.QueryTableResponseObserver observer =
                new QueryClient.QueryTableResponseObserver();

        observer.onError(TRANSPORT_FAILURE);
        assertReturnsPromptly("QueryTableResponseObserver.await()", observer::await);

        assertTrue(observer.isError());
        assertTrue(
                "expected the gRPC status, got: " + observer.getErrorMessage(),
                observer.getErrorMessage().contains("UNAVAILABLE"));
        assertEquals(ApiResultStatus.NONE, observer.getApiResultStatus());
    }

    /*
     * IngestDataResponseObserver's latch is initialized to the expected response count, so onError
     * must release every outstanding count, not just one.  Fail the stream after a single response
     * of three so that two counts remain.
     */
    @Test
    public void testIngestDataObserverReleasesAllOutstandingCountsOnError() {

        final IngestionClient.IngestDataResponseObserver observer =
                new IngestionClient.IngestDataResponseObserver(3);

        observer.onNext(IngestDataResponse.newBuilder().build());
        observer.onError(TRANSPORT_FAILURE);

        assertReturnsPromptly("IngestDataResponseObserver.await()", observer::await);

        assertTrue(observer.isError());
        assertTrue(
                "expected the gRPC status, got: " + observer.getErrorMessage(),
                observer.getErrorMessage().contains("UNAVAILABLE"));
    }

    /*
     * sendSubscribeDataEvent blocks on awaitAckLatch before returning, and onCompleted is not
     * reached after a transport failure, so onError must release both the ack and close latches.
     */
    @Test
    public void testSubscribeDataEventObserverReleasesBothLatchesOnError() {

        final IngestionStreamClient.SubscribeDataEventResponseObserver observer =
                new IngestionStreamClient.SubscribeDataEventResponseObserver(10);

        observer.onError(TRANSPORT_FAILURE);

        assertReturnsPromptly("awaitAckLatch()", observer::awaitAckLatch);
        assertReturnsPromptly("awaitCloseLatch()", observer::awaitCloseLatch);

        assertTrue(observer.isError());
        assertTrue(
                "expected the gRPC status, got: " + observer.getErrorMessage(),
                observer.getErrorMessage().contains("UNAVAILABLE"));
    }
}
