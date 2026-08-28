package com.ospreydcs.dp.client;

import com.ospreydcs.dp.client.result.ApiResultStatus;
import com.ospreydcs.dp.grpc.v1.annotation.GetConfigurationActivationResponse;
import com.ospreydcs.dp.grpc.v1.annotation.GetPvMetadataResponse;
import com.ospreydcs.dp.grpc.v1.annotation.QueryConfigurationActivationsResponse;
import com.ospreydcs.dp.grpc.v1.annotation.QueryConfigurationsResponse;
import com.ospreydcs.dp.grpc.v1.annotation.QueryPvMetadataResponse;
import com.ospreydcs.dp.grpc.v1.common.ConfigurationActivation;
import com.ospreydcs.dp.grpc.v1.common.ExceptionalResult;
import com.ospreydcs.dp.grpc.v1.common.PvMetadata;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

/**
 * Provides test coverage for the five response observers added by #243, for the two behaviors that
 * an integration test against a live service cannot reach.
 *
 * <p><strong>The ERROR/REJECT distinction.</strong> The plan for #243 calls for a guard that a
 * backend failure surfaces as ApiResultStatus.ERROR, distinct from the validation rejections the
 * integration tests cover.  Server-side that status comes from a MongoException, which cannot be
 * induced from an integration test running against a healthy database — there is no fault-injection
 * hook in the test harness.  The client-layer half of the contract is what actually matters to a
 * caller, though, and it is testable here: a response carrying RESULT_STATUS_ERROR must reach the
 * caller as ERROR and NOT as REJECT.  Conflating the two is the #235 inversion in client-layer
 * form: a caller reading isReject() as "the record does not exist" would silently mistake a
 * database outage for a benign absence.
 *
 * <p><strong>The missing-result-field guard.</strong> Each observer must call recordFailure() and
 * return false when a response carries neither an ExceptionalResult nor its result message.  A
 * live service never sends that, so only a synthetic response exercises it; without the guard the
 * caller would receive a success carrying a null or empty payload.
 *
 * <p>These are plain StreamObservers, so no server, channel or database is involved.
 */
public class AnnotationQueryObserverTest {

    private static ExceptionalResult exceptionalResult(
            ExceptionalResult.ExceptionalResultStatus status, String message
    ) {
        return ExceptionalResult.newBuilder()
                .setExceptionalResultStatus(status)
                .setMessage(message)
                .build();
    }

    // =========================================================================
    // service-reported ERROR is surfaced as ERROR, not REJECT
    // =========================================================================

    @Test
    public void testQueryPvMetadataObserverSurfacesServiceError() {

        final AnnotationClient.QueryPvMetadataResponseObserver observer =
                new AnnotationClient.QueryPvMetadataResponseObserver();

        observer.onNext(QueryPvMetadataResponse.newBuilder()
                .setExceptionalResult(exceptionalResult(
                        ExceptionalResult.ExceptionalResultStatus.RESULT_STATUS_ERROR,
                        "error executing pvMetadata query"))
                .build());
        observer.await();

        assertTrue(observer.isError());
        assertEquals(ApiResultStatus.ERROR, observer.getApiResultStatus());

        // the service message is recorded verbatim, not prefixed
        assertEquals("error executing pvMetadata query", observer.getErrorMessage());

        // an empty payload accompanying an ERROR must not read as "nothing matched"
        assertTrue(observer.getPvMetadata().isEmpty());
    }

    @Test
    public void testQueryConfigurationsObserverSurfacesServiceError() {

        final AnnotationClient.QueryConfigurationsResponseObserver observer =
                new AnnotationClient.QueryConfigurationsResponseObserver();

        observer.onNext(QueryConfigurationsResponse.newBuilder()
                .setExceptionalResult(exceptionalResult(
                        ExceptionalResult.ExceptionalResultStatus.RESULT_STATUS_ERROR,
                        "error executing configurations query"))
                .build());
        observer.await();

        assertTrue(observer.isError());
        assertEquals(ApiResultStatus.ERROR, observer.getApiResultStatus());
        assertTrue(observer.getConfigurations().isEmpty());
    }

    @Test
    public void testQueryConfigurationActivationsObserverSurfacesServiceError() {

        final AnnotationClient.QueryConfigurationActivationsResponseObserver observer =
                new AnnotationClient.QueryConfigurationActivationsResponseObserver();

        observer.onNext(QueryConfigurationActivationsResponse.newBuilder()
                .setExceptionalResult(exceptionalResult(
                        ExceptionalResult.ExceptionalResultStatus.RESULT_STATUS_ERROR,
                        "error executing activations query"))
                .build());
        observer.await();

        assertTrue(observer.isError());
        assertEquals(ApiResultStatus.ERROR, observer.getApiResultStatus());
        assertTrue(observer.getConfigurationActivations().isEmpty());
    }

    /**
     * The getters are where conflating the two statuses does the most damage, since a caller uses
     * them as an existence check.  A backend failure must be ERROR so that isReject() stays false.
     */
    @Test
    public void testGetPvMetadataObserverSurfacesServiceErrorNotReject() {

        final AnnotationClient.GetPvMetadataResponseObserver observer =
                new AnnotationClient.GetPvMetadataResponseObserver();

        observer.onNext(GetPvMetadataResponse.newBuilder()
                .setExceptionalResult(exceptionalResult(
                        ExceptionalResult.ExceptionalResultStatus.RESULT_STATUS_ERROR,
                        "error querying pvMetadata"))
                .build());
        observer.await();

        assertTrue(observer.isError());
        assertEquals(ApiResultStatus.ERROR, observer.getApiResultStatus());
        assertNull(observer.getPvMetadata());
    }

    @Test
    public void testGetConfigurationActivationObserverSurfacesServiceErrorNotReject() {

        final AnnotationClient.GetConfigurationActivationResponseObserver observer =
                new AnnotationClient.GetConfigurationActivationResponseObserver();

        observer.onNext(GetConfigurationActivationResponse.newBuilder()
                .setExceptionalResult(exceptionalResult(
                        ExceptionalResult.ExceptionalResultStatus.RESULT_STATUS_ERROR,
                        "error querying configurationActivation"))
                .build());
        observer.await();

        assertTrue(observer.isError());
        assertEquals(ApiResultStatus.ERROR, observer.getApiResultStatus());
        assertNull(observer.getConfigurationActivation());
    }

    /**
     * Confirms the two statuses are actually distinguished, rather than every failure arriving as
     * one of them.  Paired with the ERROR tests above, this is what makes isReject() meaningful.
     */
    @Test
    public void testGetPvMetadataObserverDistinguishesRejectFromError() {

        final AnnotationClient.GetPvMetadataResponseObserver observer =
                new AnnotationClient.GetPvMetadataResponseObserver();

        observer.onNext(GetPvMetadataResponse.newBuilder()
                .setExceptionalResult(exceptionalResult(
                        ExceptionalResult.ExceptionalResultStatus.RESULT_STATUS_REJECT,
                        "no PvMetadata record found for: NO:SUCH:PV"))
                .build());
        observer.await();

        assertTrue(observer.isError());
        assertEquals(ApiResultStatus.REJECT, observer.getApiResultStatus());
    }

    // =========================================================================
    // a response carrying neither an exceptional result nor its payload fails
    // =========================================================================

    @Test
    public void testQueryPvMetadataObserverRejectsResponseMissingResult() {

        final AnnotationClient.QueryPvMetadataResponseObserver observer =
                new AnnotationClient.QueryPvMetadataResponseObserver();

        observer.onNext(QueryPvMetadataResponse.getDefaultInstance());
        observer.await();

        assertTrue(observer.isError());
        assertTrue(
                observer.getErrorMessage(),
                observer.getErrorMessage().contains("does not contain PvMetadataResult"));

        // the failure was detected client-side, so no service status accompanies it
        assertEquals(ApiResultStatus.NONE, observer.getApiResultStatus());
    }

    @Test
    public void testGetPvMetadataObserverRejectsResponseMissingResult() {

        final AnnotationClient.GetPvMetadataResponseObserver observer =
                new AnnotationClient.GetPvMetadataResponseObserver();

        observer.onNext(GetPvMetadataResponse.getDefaultInstance());
        observer.await();

        assertTrue(observer.isError());
        assertTrue(
                observer.getErrorMessage(),
                observer.getErrorMessage().contains("does not contain GetPvMetadataResult"));
        assertNull(observer.getPvMetadata());
    }

    @Test
    public void testQueryConfigurationsObserverRejectsResponseMissingResult() {

        final AnnotationClient.QueryConfigurationsResponseObserver observer =
                new AnnotationClient.QueryConfigurationsResponseObserver();

        observer.onNext(QueryConfigurationsResponse.getDefaultInstance());
        observer.await();

        assertTrue(observer.isError());
        assertTrue(
                observer.getErrorMessage(),
                observer.getErrorMessage().contains("does not contain QueryConfigurationsResult"));
    }

    @Test
    public void testQueryConfigurationActivationsObserverRejectsResponseMissingResult() {

        final AnnotationClient.QueryConfigurationActivationsResponseObserver observer =
                new AnnotationClient.QueryConfigurationActivationsResponseObserver();

        observer.onNext(QueryConfigurationActivationsResponse.getDefaultInstance());
        observer.await();

        assertTrue(observer.isError());
        assertTrue(
                observer.getErrorMessage(),
                observer.getErrorMessage().contains(
                        "does not contain QueryConfigurationActivationsResult"));
    }

    @Test
    public void testGetConfigurationActivationObserverRejectsResponseMissingResult() {

        final AnnotationClient.GetConfigurationActivationResponseObserver observer =
                new AnnotationClient.GetConfigurationActivationResponseObserver();

        observer.onNext(GetConfigurationActivationResponse.getDefaultInstance());
        observer.await();

        assertTrue(observer.isError());
        assertTrue(
                observer.getErrorMessage(),
                observer.getErrorMessage().contains(
                        "does not contain GetConfigurationActivationResult"));
        assertNull(observer.getConfigurationActivation());
    }

    // =========================================================================
    // successful responses populate the payload and the page token
    // =========================================================================

    @Test
    public void testQueryPvMetadataObserverAcceptsResult() {

        final AnnotationClient.QueryPvMetadataResponseObserver observer =
                new AnnotationClient.QueryPvMetadataResponseObserver();

        observer.onNext(QueryPvMetadataResponse.newBuilder()
                .setPvMetadataResult(QueryPvMetadataResponse.PvMetadataResult.newBuilder()
                        .addPvMetadata(PvMetadata.newBuilder().setPvName("TEST:PV:1"))
                        .setNextPageToken("next"))
                .build());
        observer.await();

        assertFalse(observer.getErrorMessage(), observer.isError());
        assertEquals(ApiResultStatus.NONE, observer.getApiResultStatus());
        assertEquals(1, observer.getPvMetadata().size());
        assertEquals("TEST:PV:1", observer.getPvMetadata().get(0).getPvName());
        assertEquals("next", observer.getNextPageToken());
    }

    /**
     * An empty page is a valid success: the observer must accept it, leaving isError false, rather
     * than treating the absent records as a missing result field.
     */
    @Test
    public void testQueryConfigurationsObserverAcceptsEmptyResult() {

        final AnnotationClient.QueryConfigurationsResponseObserver observer =
                new AnnotationClient.QueryConfigurationsResponseObserver();

        observer.onNext(QueryConfigurationsResponse.newBuilder()
                .setQueryConfigurationsResult(
                        QueryConfigurationsResponse.QueryConfigurationsResult.getDefaultInstance())
                .build());
        observer.await();

        assertFalse(observer.getErrorMessage(), observer.isError());
        assertEquals(ApiResultStatus.NONE, observer.getApiResultStatus());
        assertTrue(observer.getConfigurations().isEmpty());
        assertEquals("", observer.getNextPageToken());
    }

    @Test
    public void testQueryConfigurationActivationsObserverAcceptsResult() {

        final AnnotationClient.QueryConfigurationActivationsResponseObserver observer =
                new AnnotationClient.QueryConfigurationActivationsResponseObserver();

        observer.onNext(QueryConfigurationActivationsResponse.newBuilder()
                .setQueryConfigurationActivationsResult(
                        QueryConfigurationActivationsResponse.QueryConfigurationActivationsResult
                                .newBuilder()
                                .addConfigurationActivations(ConfigurationActivation.newBuilder()
                                        .setClientActivationId("activation-1"))
                                .setNextPageToken("next"))
                .build());
        observer.await();

        assertFalse(observer.getErrorMessage(), observer.isError());
        assertEquals(1, observer.getConfigurationActivations().size());
        assertEquals(
                "activation-1",
                observer.getConfigurationActivations().get(0).getClientActivationId());
        assertEquals("next", observer.getNextPageToken());
    }

    @Test
    public void testGetConfigurationActivationObserverAcceptsResult() {

        final AnnotationClient.GetConfigurationActivationResponseObserver observer =
                new AnnotationClient.GetConfigurationActivationResponseObserver();

        observer.onNext(GetConfigurationActivationResponse.newBuilder()
                .setGetConfigurationActivationResult(
                        GetConfigurationActivationResponse.GetConfigurationActivationResult
                                .newBuilder()
                                .setConfigurationActivation(ConfigurationActivation.newBuilder()
                                        .setClientActivationId("activation-1")))
                .build());
        observer.await();

        assertFalse(observer.getErrorMessage(), observer.isError());
        assertNotNull(observer.getConfigurationActivation());
        assertEquals("activation-1", observer.getConfigurationActivation().getClientActivationId());
    }

    @Test
    public void testGetPvMetadataObserverAcceptsResult() {

        final AnnotationClient.GetPvMetadataResponseObserver observer =
                new AnnotationClient.GetPvMetadataResponseObserver();

        observer.onNext(GetPvMetadataResponse.newBuilder()
                .setGetPvMetadataResult(GetPvMetadataResponse.GetPvMetadataResult.newBuilder()
                        .setPvMetadata(PvMetadata.newBuilder().setPvName("TEST:PV:1")))
                .build());
        observer.await();

        assertFalse(observer.getErrorMessage(), observer.isError());
        assertNotNull(observer.getPvMetadata());
        assertEquals("TEST:PV:1", observer.getPvMetadata().getPvName());
    }
}
