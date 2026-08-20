package com.ospreydcs.dp.client;

import com.ospreydcs.dp.client.result.ApiResultStatus;
import com.google.protobuf.Message;
import com.ospreydcs.dp.grpc.v1.common.ExceptionalResult;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Base class for the client API response observers that handle a single-response RPC.
 *
 * <p>Every such observer awaits one response, records a failure so the corresponding {@code
 * sendXxx()} method can build an error {@code ApiResult}, and releases a latch so that {@code
 * sendXxx()} does not block past the point where the outcome is known.  That machinery was
 * previously copied into each observer, and the copies drifted: the duplicate-response branch
 * omitted the {@code countDown()} in every one of them, so a second response stalled the caller
 * for the full await timeout instead of failing fast.  Fixing that class of defect in one place
 * is the reason this class exists.
 *
 * <p>Subclasses supply only the parts that genuinely differ between RPCs, via {@link
 * #hasExceptionalResult}, {@link #getExceptionalResult} and {@link #handleResult}.  Everything
 * else -- the latch, the error state, the await, {@code onError}, and the response-sequence
 * checks -- is final here.
 *
 * <p><strong>Latch discipline.</strong> The latch is released exactly once, by {@link
 * #recordFailure} or by a successful {@link #handleResult}, and {@link #onNext} routes every path
 * through one of those.  A path that records a failure without releasing the latch is the
 * original defect, so subclasses must report failures through {@code recordFailure} rather than
 * touching the error state directly.
 *
 * <p><strong>Threading.</strong> {@code onNext} and {@code onError} dispatch their work onto a new
 * thread, preserving the behavior of the observers this class replaces: it keeps response handling
 * off the service handler's thread, so in-process tests exercise the same concurrency an
 * out-of-process gRPC client would.
 *
 * @param <T> the response message type of the RPC
 */
public abstract class ApiResponseObserverBase<T extends Message> implements StreamObserver<T> {

    /**
     * How long {@link #await} waits for a response before reporting a timeout.  Exposed so tests
     * can drive the timeout path without waiting out the production value.
     */
    public static final long DEFAULT_AWAIT_TIMEOUT_SECONDS = 60;

    // instance variables
    private final CountDownLatch finishLatch = new CountDownLatch(1);
    private final AtomicBoolean isError = new AtomicBoolean(false);
    private final List<String> errorMessageList = Collections.synchronizedList(new ArrayList<>());
    // categorizes a failure so callers can distinguish a service rejection from a service
    // error.  Defaults to NONE, which ApiResultBase maps to LOCAL_FAILURE when a failure was
    // recorded without a status-bearing response -- a transport error, an await timeout, a
    // malformed response sequence.  Defaulting to NONE rather than LOCAL_FAILURE keeps this
    // getter honest for a call that succeeded.  Only the first status recorded is kept, so
    // that the status and the message returned by getErrorMessage() describe the same failure.
    private final AtomicReference<ApiResultStatus> apiResultStatus =
            new AtomicReference<>(ApiResultStatus.NONE);
    // guards the response-sequence check.  A separate flag rather than an isEmpty() test on the
    // subclass payload list, because an observer whose response carries an empty repeated field
    // stores nothing on the first response and would otherwise fail to recognize the second.
    private final AtomicBoolean responseReceived = new AtomicBoolean(false);

    private final long awaitTimeoutSeconds;

    protected ApiResponseObserverBase() {
        this(DEFAULT_AWAIT_TIMEOUT_SECONDS);
    }

    /**
     * @param awaitTimeoutSeconds how long {@link #await} waits before reporting a timeout; intended
     *                            for tests that exercise the timeout path.
     */
    protected ApiResponseObserverBase(long awaitTimeoutSeconds) {
        this.awaitTimeoutSeconds = awaitTimeoutSeconds;
    }

    /**
     * Names this observer in its error messages.  Defaults to the concrete class's simple name so
     * that a message identifies the RPC that produced it without each subclass repeating a
     * literal.
     *
     * <p>{@code getSimpleName()} is empty for an anonymous subclass, which would leave a message
     * with no identity at all, so fall back to the nearest named ancestor in that case.
     */
    protected String observerName() {

        for (Class<?> c = getClass(); c != null; c = c.getSuperclass()) {
            final String simpleName = c.getSimpleName();
            if (!simpleName.isEmpty()) {
                return simpleName;
            }
        }

        return "ApiResponseObserver";
    }

    /**
     * Whether the given response carries an {@code ExceptionalResult} in place of a payload.
     */
    protected abstract boolean hasExceptionalResult(T response);

    /**
     * Returns the {@code ExceptionalResult} from a response for which {@link #hasExceptionalResult}
     * returned true.
     */
    protected abstract ExceptionalResult getExceptionalResult(T response);

    /**
     * Extracts and stores the payload of a successful response.
     *
     * <p>Called at most once per observer, and only for a response that carries no {@code
     * ExceptionalResult}.  Return false, after calling {@link #recordFailure}, to reject a response
     * that is well-formed on the wire but unusable -- a response missing the result field it is
     * required to carry, for instance.  Returning true releases the awaiting caller.
     *
     * @return true if the payload was accepted, false if the response was rejected
     */
    protected abstract boolean handleResult(T response);

    /**
     * Records a failure and releases the awaiting caller.
     *
     * <p>Every message recorded is retained, in order, and is available from {@link
     * #getErrorMessageList}.  It is {@link #getErrorMessage} that selects the first one, so that
     * the message it returns and the status from {@link #getApiResultStatus} describe the same
     * failure -- the earliest one, which is the one that caused the call to fail.
     */
    protected final void recordFailure(String errorMsg) {
        recordFailure(errorMsg, null);
    }

    /**
     * Records a failure carrying a service-supplied status, and releases the awaiting caller.
     *
     * @param apiResultStatus the status to report, or null to leave it at its current value
     */
    protected final void recordFailure(String errorMsg, ApiResultStatus apiResultStatus) {

        System.err.println(errorMsg);

        if (apiResultStatus != null) {
            this.apiResultStatus.compareAndSet(ApiResultStatus.NONE, apiResultStatus);
        }

        isError.set(true);
        errorMessageList.add(errorMsg);
        finishLatch.countDown();
    }

    /**
     * Releases the awaiting caller after a payload has been accepted.
     */
    private void recordSuccess() {
        finishLatch.countDown();
    }

    /**
     * Waits for the RPC to produce a response, a transport error, or a timeout.
     *
     * <p>A timeout is reported as an error: the latch is only counted down by a response or an
     * onError, so an expired await means no result was received and the caller would otherwise see
     * a success carrying a null payload.
     */
    public final void await() {
        try {
            if (!finishLatch.await(awaitTimeoutSeconds, TimeUnit.SECONDS)) {
                recordFailure(observerName() + " timed out waiting for finishLatch after "
                        + awaitTimeoutSeconds + " seconds");
            }
        } catch (InterruptedException e) {
            recordFailure(observerName() + " InterruptedException waiting for finishLatch");
            // restore the interrupt flag so callers up the stack can still observe it
            Thread.currentThread().interrupt();
        }
    }

    public final boolean isError() {
        return isError.get();
    }

    public final String getErrorMessage() {
        if (!errorMessageList.isEmpty()) {
            return errorMessageList.get(0);
        } else {
            return "";
        }
    }

    public final ApiResultStatus getApiResultStatus() {
        return apiResultStatus.get();
    }

    /**
     * Returns every failure recorded, in the order recorded, for callers that need more than the
     * first one -- a test asserting that a later failure was processed without displacing the
     * earlier one, for instance.
     */
    public final List<String> getErrorMessageList() {
        return Collections.unmodifiableList(errorMessageList);
    }

    @Override
    public final void onNext(T response) {

        // handle response in separate thread to better simulate out of process grpc,
        // otherwise response is handled in same thread as service handler that sent it
        new Thread(() -> {

            // check the response sequence before doing anything else, so that a second response
            // cannot overwrite the payload or the status recorded from the first.  The duplicate
            // must release the latch as well: it is a failure the caller needs to see promptly,
            // and leaving the latch held here was the original defect.
            if (!responseReceived.compareAndSet(false, true)) {
                recordFailure(observerName() + " onNext received more than one response");
                return;
            }

            if (hasExceptionalResult(response)) {
                final ExceptionalResult exceptionalResult = getExceptionalResult(response);
                recordFailure(
                        observerName() + " onNext received exceptional response: "
                                + exceptionalResult.getMessage(),
                        ApiResultStatus.fromProto(exceptionalResult.getExceptionalResultStatus()));
                return;
            }

            // handleResult calls recordFailure itself when it rejects the response, so the latch
            // is released on both branches
            if (handleResult(response)) {
                recordSuccess();
            }

        }).start();
    }

    @Override
    public final void onError(Throwable t) {

        // handle response in separate thread to better simulate out of process grpc,
        // otherwise response is handled in same thread as service handler that sent it
        new Thread(() -> {
            final Status status = Status.fromThrowable(t);
            // the latch must be counted down here, otherwise a transport failure leaves await()
            // to expire and the caller sees a timeout message instead of the actual gRPC status
            recordFailure(observerName() + " error: " + status);
        }).start();
    }

    @Override
    public void onCompleted() {
    }
}
