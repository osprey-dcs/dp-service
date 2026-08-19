package com.ospreydcs.dp.client.result;

import com.ospreydcs.dp.grpc.v1.common.ExceptionalResult;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Provides test coverage for the API result status added to ApiResultBase, including the mapping
 * from the protobuf status enum and the invariants tying apiResultStatus to isError.
 */
public class ApiResultBaseTest {

    /*
     * Minimal concrete subclass, since ApiResultBase is abstract.
     */
    private static class TestApiResult extends ApiResultBase {

        public TestApiResult(boolean isError, String errorMessage) {
            super(isError, errorMessage);
        }

        public TestApiResult(boolean isError, String errorMessage, ApiResultStatus apiResultStatus) {
            super(isError, errorMessage, apiResultStatus);
        }
    }

    @Test
    public void testFromProtoMapsEachWireValue() {
        assertEquals(
                ApiResultStatus.REJECT,
                ApiResultStatus.fromProto(ExceptionalResult.ExceptionalResultStatus.RESULT_STATUS_REJECT));
        assertEquals(
                ApiResultStatus.ERROR,
                ApiResultStatus.fromProto(ExceptionalResult.ExceptionalResultStatus.RESULT_STATUS_ERROR));
        assertEquals(
                ApiResultStatus.NOT_READY,
                ApiResultStatus.fromProto(ExceptionalResult.ExceptionalResultStatus.RESULT_STATUS_NOT_READY));
    }

    /*
     * An unrecognized wire value must still be reported as a failure rather than throwing, so that
     * a client built against an older protobuf revision degrades gracefully against a newer service.
     */
    @Test
    public void testFromProtoMapsUnrecognizedValueToError() {
        assertEquals(
                ApiResultStatus.ERROR,
                ApiResultStatus.fromProto(ExceptionalResult.ExceptionalResultStatus.UNRECOGNIZED));
        assertEquals(ApiResultStatus.ERROR, ApiResultStatus.fromProto(null));
    }

    /*
     * The two argument constructor is the pre-existing signature used by every locally generated
     * failure, so its failures must categorize as LOCAL_FAILURE.
     */
    @Test
    public void testLegacyConstructorCategorizesFailureAsLocal() {

        final TestApiResult result = new TestApiResult(true, "timed out waiting for finishLatch");

        assertTrue(result.isError());
        assertTrue(result.resultStatus.isError);
        assertEquals(ApiResultStatus.LOCAL_FAILURE, result.apiResultStatus);
        assertFalse(result.isReject());
    }

    @Test
    public void testSuccessCategorizesAsNone() {

        final TestApiResult result = new TestApiResult(false, "");

        assertFalse(result.isError());
        assertEquals(ApiResultStatus.NONE, result.apiResultStatus);
        assertFalse(result.isReject());
    }

    @Test
    public void testRejectStatusIsPreservedAndPredicated() {

        final TestApiResult result =
                new TestApiResult(true, "no Configuration record found", ApiResultStatus.REJECT);

        assertTrue(result.isError());
        assertEquals(ApiResultStatus.REJECT, result.apiResultStatus);
        assertTrue(result.isReject());
    }

    @Test
    public void testErrorStatusIsNotAReject() {

        final TestApiResult result =
                new TestApiResult(true, "database error", ApiResultStatus.ERROR);

        assertEquals(ApiResultStatus.ERROR, result.apiResultStatus);
        assertFalse(result.isReject());
    }

    /*
     * A success must never carry a failure category, even if a caller passes one, so that isReject()
     * cannot report true for a result whose payload is valid.
     */
    @Test
    public void testSuccessOverridesSuppliedFailureStatus() {

        final TestApiResult result = new TestApiResult(false, "", ApiResultStatus.REJECT);

        assertEquals(ApiResultStatus.NONE, result.apiResultStatus);
        assertFalse(result.isReject());
    }

    /*
     * Conversely a failure must always carry some failure category, so callers can switch on
     * apiResultStatus without handling NONE as a failure case.
     */
    @Test
    public void testFailureWithoutStatusFallsBackToLocalFailure() {

        assertEquals(
                ApiResultStatus.LOCAL_FAILURE,
                new TestApiResult(true, "boom", ApiResultStatus.NONE).apiResultStatus);
        assertEquals(
                ApiResultStatus.LOCAL_FAILURE,
                new TestApiResult(true, "boom", null).apiResultStatus);
    }
}
