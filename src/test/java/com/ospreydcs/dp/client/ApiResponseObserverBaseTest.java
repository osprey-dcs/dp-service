package com.ospreydcs.dp.client;

import com.ospreydcs.dp.client.result.ApiResultStatus;
import com.ospreydcs.dp.grpc.v1.common.ExceptionalResult;
import com.ospreydcs.dp.grpc.v1.query.QueryProvidersResponse;
import com.ospreydcs.dp.grpc.v1.query.QueryTableResponse;
import org.junit.Test;

import java.time.Duration;
import java.time.Instant;
import java.util.function.BooleanSupplier;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

/**
 * Provides test coverage for the response-sequence handling shared by the client API response
 * observers via {@link ApiResponseObserverBase}.
 *
 * <p>These cover the two defects that motivated lifting the machinery into a base class:
 *
 * <ul>
 *   <li>A duplicate response was detected but left the latch held, so the caller blocked for the
 *       full await timeout instead of failing fast.  As with {@link ResponseObserverErrorTest},
 *       the wait <em>is</em> the defect, so these assert on elapsed time as well as content.
 *   <li>{@code QueryTableResponseObserver} counted the latch down before checking for a duplicate,
 *       so the second response raced the caller and its error flag was usually read too late.
 * </ul>
 *
 * <p>The observers are plain StreamObservers, so no server, channel or database is involved.  A
 * short await timeout is used where the timeout path itself is under test, so that the case is
 * covered in milliseconds rather than the production minute.
 */
public class ApiResponseObserverBaseTest {

    // an await that returns within this bound cannot have waited out the one minute timeout the
    // observers use by default, while leaving ample headroom for a slow CI machine
    private static final Duration AWAIT_BOUND = Duration.ofSeconds(10);

    /*
     * Asserts that the given wait returns promptly rather than expiring.
     */
    private static void assertReturnsPromptly(String description, Runnable wait) {

        final Instant start = Instant.now();
        wait.run();
        final Duration elapsed = Duration.between(start, Instant.now());

        assertTrue(
                description + " did not return promptly, took " + elapsed.toMillis()
                        + "ms, which means the duplicate response left the latch held",
                elapsed.compareTo(AWAIT_BOUND) < 0);
    }

    /*
     * onNext dispatches its work to a new thread, so a response delivered after the latch has
     * already been released -- a duplicate -- is not necessarily visible by the time onNext
     * returns.  await() cannot be used to wait for it, since the latch is already at zero.  Poll
     * for the error flag instead, which is what the duplicate sets, rather than sleeping a fixed
     * interval that would be either flaky or slow.
     */
    private static void awaitCondition(String description, BooleanSupplier condition) {

        final Instant deadline = Instant.now().plus(AWAIT_BOUND);

        while (!condition.getAsBoolean() && Instant.now().isBefore(deadline)) {
            Thread.onSpinWait();
        }

        assertTrue(
                description + " did not happen within " + AWAIT_BOUND.toSeconds() + " seconds",
                condition.getAsBoolean());
    }

    private static void awaitErrorRecorded(ApiResponseObserverBase<?> observer) {
        awaitCondition("the observer recording an error", observer::isError);
    }

    /*
     * A second response must be reported as an error and must release the caller immediately.
     * Before the fix the duplicate branch set the error flag but omitted the countDown, so await()
     * blocked for the full timeout and then overwrote nothing -- the caller waited a minute to
     * learn about a failure that was already known.
     */
    @Test
    public void testDuplicateResponseFailsFast() {

        final QueryClient.QueryTableResponseObserver observer =
                new QueryClient.QueryTableResponseObserver();

        // deliver the first response and let it settle before sending the second.  onNext
        // dispatches to a new thread, so firing both back to back leaves which one wins the
        // sequence check undefined -- and await() can then return on the winner's countDown
        // before the loser's thread has recorded anything.
        observer.onNext(QueryTableResponse.newBuilder().build());
        assertReturnsPromptly("QueryTableResponseObserver.await()", observer::await);
        assertFalse("the first response must not be an error", observer.isError());

        // the duplicate must fail fast rather than leaving a caller to wait out the timeout.
        // The latch is already released, so timing the duplicate's own await() is what would
        // have caught the missing countDown: measure that it returns without expiring.
        observer.onNext(QueryTableResponse.newBuilder().build());
        awaitErrorRecorded(observer);
        assertReturnsPromptly("await() after a duplicate", observer::await);

        assertTrue("a duplicate response must be reported as an error", observer.isError());
        assertTrue(
                "expected the duplicate-response message, got: " + observer.getErrorMessage(),
                observer.getErrorMessage().contains("more than one response"));
    }

    /*
     * The duplicate must not displace the payload or the status recorded from the first response.
     * QueryTableResponseObserver previously appended every response to its list and counted down
     * before checking the sequence, so the check raced the caller reading isError().
     */
    @Test
    public void testDuplicateResponseDoesNotDisplaceFirstResult() {

        final QueryClient.QueryProvidersResponseObserver observer =
                new QueryClient.QueryProvidersResponseObserver();

        final QueryProvidersResponse first = QueryProvidersResponse.newBuilder()
                .setProvidersResult(QueryProvidersResponse.ProvidersResult.newBuilder()
                        .addProviderInfos(QueryProvidersResponse.ProvidersResult.ProviderInfo
                                .newBuilder().setId("provider-one").build())
                        .build())
                .build();

        final QueryProvidersResponse second = QueryProvidersResponse.newBuilder()
                .setProvidersResult(QueryProvidersResponse.ProvidersResult.newBuilder()
                        .addProviderInfos(QueryProvidersResponse.ProvidersResult.ProviderInfo
                                .newBuilder().setId("provider-two").build())
                        .build())
                .build();

        observer.onNext(first);
        observer.await();
        observer.onNext(second);
        awaitErrorRecorded(observer);

        assertTrue(observer.isError());
        assertEquals(
                "the duplicate must not be appended to the payload",
                1,
                observer.getProviderInfoList().size());
        assertEquals("provider-one", observer.getProviderInfoList().get(0).getId());
    }

    /*
     * An exceptional result carries the service's status through to the caller, and a duplicate
     * arriving afterward must not overwrite it -- getErrorMessage() and getApiResultStatus() are
     * required to describe the same failure.
     */
    @Test
    public void testExceptionalResultStatusSurvivesDuplicate() {

        final QueryClient.QueryTableResponseObserver observer =
                new QueryClient.QueryTableResponseObserver();

        observer.onNext(QueryTableResponse.newBuilder()
                .setExceptionalResult(ExceptionalResult.newBuilder()
                        .setExceptionalResultStatus(
                                ExceptionalResult.ExceptionalResultStatus.RESULT_STATUS_REJECT)
                        .setMessage("rejected by service")
                        .build())
                .build());

        assertReturnsPromptly("QueryTableResponseObserver.await()", observer::await);

        // isError is already set by the exceptional result, so wait for the duplicate's own
        // message to land -- that is what proves the duplicate was processed and that the
        // assertions below are checking the first failure survived rather than a race
        observer.onNext(QueryTableResponse.newBuilder().build());
        awaitCondition(
                "the duplicate response being recorded",
                () -> observer.getErrorMessageList().size() > 1);

        assertTrue(observer.isError());
        assertEquals(ApiResultStatus.REJECT, observer.getApiResultStatus());
        assertTrue(
                "expected the service message, got: " + observer.getErrorMessage(),
                observer.getErrorMessage().contains("rejected by service"));
        assertNull("an exceptional result carries no payload", observer.getQueryResponse());
    }

    /*
     * A response missing the result field it is required to carry is rejected rather than stored,
     * and must release the caller.  Previously QueryProvidersResponseObserver called
     * Objects.requireNonNull here, on the onNext thread, where the resulting NPE was swallowed and
     * the latch was never counted down.
     */
    @Test
    public void testResponseMissingResultIsRejected() {

        final QueryClient.QueryProvidersResponseObserver observer =
                new QueryClient.QueryProvidersResponseObserver();

        observer.onNext(QueryProvidersResponse.newBuilder().build());

        assertReturnsPromptly("QueryProvidersResponseObserver.await()", observer::await);

        assertTrue(observer.isError());
        assertTrue(
                "expected the missing-result message, got: " + observer.getErrorMessage(),
                observer.getErrorMessage().contains("does not contain ProvidersResult"));
        assertTrue(observer.getProviderInfoList().isEmpty());
    }

    /*
     * A normal single response must not be reported as an error, so that the sequence check does
     * not turn a successful call into a failure.
     */
    @Test
    public void testSingleResponseSucceeds() {

        final QueryClient.QueryTableResponseObserver observer =
                new QueryClient.QueryTableResponseObserver();

        final QueryTableResponse response = QueryTableResponse.newBuilder().build();
        observer.onNext(response);

        assertReturnsPromptly("QueryTableResponseObserver.await()", observer::await);

        assertFalse(observer.isError());
        assertEquals("", observer.getErrorMessage());
        assertEquals(ApiResultStatus.NONE, observer.getApiResultStatus());
        assertEquals(response, observer.getQueryResponse());
    }

    /*
     * An await that expires must be reported as an error, so that the caller cannot mistake a hung
     * RPC for a success carrying a null payload.  Uses a short timeout so the case is covered
     * without waiting out the production value.
     */
    @Test
    public void testAwaitTimeoutIsReportedAsError() {

        final ApiResponseObserverBase<QueryTableResponse> observer =
                new ApiResponseObserverBase<>(1) {

                    @Override
                    protected boolean hasExceptionalResult(QueryTableResponse response) {
                        return response.hasExceptionalResult();
                    }

                    @Override
                    protected ExceptionalResult getExceptionalResult(QueryTableResponse response) {
                        return response.getExceptionalResult();
                    }

                    @Override
                    protected boolean handleResult(QueryTableResponse response) {
                        return true;
                    }
                };

        // never deliver a response, so the await expires
        observer.await();

        assertTrue("an expired await must be reported as an error", observer.isError());
        assertTrue(
                "expected the timeout message, got: " + observer.getErrorMessage(),
                observer.getErrorMessage().contains("timed out"));
    }
}
