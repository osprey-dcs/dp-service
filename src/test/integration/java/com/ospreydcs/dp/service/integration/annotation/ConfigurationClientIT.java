package com.ospreydcs.dp.service.integration.annotation;

import com.ospreydcs.dp.client.AnnotationClient;
import com.ospreydcs.dp.client.result.ApiResultStatus;
import com.ospreydcs.dp.client.result.GetConfigurationApiResult;
import com.ospreydcs.dp.client.result.SaveConfigurationActivationApiResult;
import com.ospreydcs.dp.client.result.SaveConfigurationApiResult;
import com.ospreydcs.dp.grpc.v1.annotation.GetConfigurationRequest;
import com.ospreydcs.dp.grpc.v1.annotation.SaveConfigurationActivationRequest;
import com.ospreydcs.dp.grpc.v1.annotation.SaveConfigurationRequest;
import com.ospreydcs.dp.grpc.v1.common.Timestamp;
import com.ospreydcs.dp.service.common.bson.configuration.ConfigurationActivationDocument;
import com.ospreydcs.dp.service.common.bson.configuration.ConfigurationDocument;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

/**
 * Provides integration test coverage for the saveConfiguration(), saveConfigurationActivation() and
 * getConfiguration() support in the com.ospreydcs.dp.client convenience layer, exercising
 * AnnotationClient against a running annotation service.
 *
 * Server-side behavior is covered separately and extensively by ConfigurationIT; these tests cover
 * the client wrapper — request building from the params records, the success payloads, and the
 * surfacing of server rejections through ApiResultBase.resultStatus.  Where a rejection is
 * exercised here, the assertion is about the surfacing mechanism (that the rejection arrives as
 * resultStatus.isError rather than a thrown exception, carrying the server's message verbatim in
 * resultStatus.msg) and not about the server-side rule that produced it.
 */
public class ConfigurationClientIT extends AnnotationIntegrationTestIntermediate {

    private AnnotationClient annotationClient;

    @Before
    public void setUp() throws Exception {
        super.setUp();
        annotationClient = new AnnotationClient(annotationServiceWrapper.getAnnotationChannel());
    }

    @After
    public void tearDown() {
        annotationClient = null;
        super.tearDown();
    }

    private static Timestamp timestamp(long seconds) {
        return Timestamp.newBuilder().setEpochSeconds(seconds).setNanoseconds(0).build();
    }

    // =========================================================================
    // buildSaveConfigurationRequest tests
    // =========================================================================

    /**
     * Verifies that optional fields are omitted from the request when null, and that only
     * configurationName and category are required.
     */
    @Test
    public void testBuildConfigurationRequestOmitsUnsuppliedOptionalFields() {

        final AnnotationClient.SaveConfigurationParams params =
                new AnnotationClient.SaveConfigurationParams(
                        "test-config-100", "beamline", null, null, null, null, null);

        final SaveConfigurationRequest request = AnnotationClient.buildSaveConfigurationRequest(params);

        assertEquals("test-config-100", request.getConfigurationName());
        assertEquals("beamline", request.getCategory());
        assertEquals("", request.getDescription());
        assertEquals("", request.getParentConfigurationName());
        assertTrue(request.getTagsList().isEmpty());
        assertTrue(request.getAttributesList().isEmpty());
        assertEquals("", request.getModifiedBy());
    }

    /**
     * Verifies that every supplied field reaches the request, and that the attribute map is
     * converted to the repeated Attribute field.
     */
    @Test
    public void testBuildConfigurationRequestPopulatesSuppliedFields() {

        // LinkedHashMap so the converted attribute order is predictable for assertion
        final Map<String, String> attributeMap = new LinkedHashMap<>();
        attributeMap.put("facility", "lcls");
        attributeMap.put("sector", "01");

        final AnnotationClient.SaveConfigurationParams params =
                new AnnotationClient.SaveConfigurationParams(
                        "test-config-101",
                        "beamline",
                        "a test configuration",
                        "test-parent",
                        List.of("TEST", "Unit"),
                        attributeMap,
                        "craigmcc");

        final SaveConfigurationRequest request = AnnotationClient.buildSaveConfigurationRequest(params);

        assertEquals("test-config-101", request.getConfigurationName());
        assertEquals("beamline", request.getCategory());
        assertEquals("a test configuration", request.getDescription());
        assertEquals("test-parent", request.getParentConfigurationName());

        // client does not normalize tags; the server lowercases, dedupes and sorts them
        assertEquals(List.of("TEST", "Unit"), request.getTagsList());

        assertEquals(2, request.getAttributesCount());
        assertEquals("facility", request.getAttributes(0).getName());
        assertEquals("lcls", request.getAttributes(0).getValue());
        assertEquals("sector", request.getAttributes(1).getName());
        assertEquals("01", request.getAttributes(1).getValue());

        assertEquals("craigmcc", request.getModifiedBy());
    }

    // =========================================================================
    // buildSaveConfigurationActivationRequest tests
    // =========================================================================

    /**
     * Verifies that optional activation fields are omitted when null.
     *
     * The endTime assertion is the substantive one: endTime is a message field with real protobuf
     * field presence, so an open-ended activation requires the field to be left entirely unset.  A
     * builder that instead set a zero-valued Timestamp would mark the field present and describe an
     * activation ending at the epoch, which the server would reject as not after startTime.
     */
    @Test
    public void testBuildActivationRequestOmitsUnsuppliedOptionalFields() {

        final AnnotationClient.SaveConfigurationActivationParams params =
                new AnnotationClient.SaveConfigurationActivationParams(
                        null, "test-config-100", timestamp(1000L), null, null, null, null, null);

        final SaveConfigurationActivationRequest request =
                AnnotationClient.buildSaveConfigurationActivationRequest(params);

        assertEquals("test-config-100", request.getConfigurationName());
        assertTrue(request.hasStartTime());
        assertEquals(1000L, request.getStartTime().getEpochSeconds());

        // an omitted endTime must leave the message field unset, not set to a zero Timestamp
        assertFalse(request.hasEndTime());

        // clientActivationId is a plain string with no field presence, so an omitted one is empty
        assertEquals("", request.getClientActivationId());

        assertEquals("", request.getDescription());
        assertTrue(request.getTagsList().isEmpty());
        assertTrue(request.getAttributesList().isEmpty());
        assertEquals("", request.getModifiedBy());
    }

    /**
     * Verifies that every supplied activation field reaches the request.
     */
    @Test
    public void testBuildActivationRequestPopulatesSuppliedFields() {

        final Map<String, String> attributeMap = new LinkedHashMap<>();
        attributeMap.put("operator", "night-shift");

        final AnnotationClient.SaveConfigurationActivationParams params =
                new AnnotationClient.SaveConfigurationActivationParams(
                        "client-id-101",
                        "test-config-101",
                        timestamp(2000L),
                        timestamp(3000L),
                        "a test activation",
                        List.of("TEST"),
                        attributeMap,
                        "craigmcc");

        final SaveConfigurationActivationRequest request =
                AnnotationClient.buildSaveConfigurationActivationRequest(params);

        assertEquals("client-id-101", request.getClientActivationId());
        assertEquals("test-config-101", request.getConfigurationName());
        assertEquals(2000L, request.getStartTime().getEpochSeconds());
        assertTrue(request.hasEndTime());
        assertEquals(3000L, request.getEndTime().getEpochSeconds());
        assertEquals("a test activation", request.getDescription());
        assertEquals(List.of("TEST"), request.getTagsList());
        assertEquals(1, request.getAttributesCount());
        assertEquals("operator", request.getAttributes(0).getName());
        assertEquals("night-shift", request.getAttributes(0).getValue());
        assertEquals("craigmcc", request.getModifiedBy());
    }

    /**
     * Verifies that buildGetConfigurationRequest carries the configuration name.
     */
    @Test
    public void testBuildGetConfigurationRequest() {
        final GetConfigurationRequest request =
                AnnotationClient.buildGetConfigurationRequest("test-config-102");
        assertEquals("test-config-102", request.getConfigurationName());
    }

    // =========================================================================
    // saveConfiguration tests
    // =========================================================================

    /**
     * Verifies that a successful save returns the canonical configuration name with no error, and
     * that the record reaches the database with tags normalized server-side.
     */
    @Test
    public void testSaveConfigurationSuccess() {

        final Map<String, String> attributeMap = new LinkedHashMap<>();
        attributeMap.put("facility", "lcls");

        final AnnotationClient.SaveConfigurationParams params =
                new AnnotationClient.SaveConfigurationParams(
                        "test-config-001",
                        "beamline",
                        "A test configuration",
                        null,
                        List.of("TEST", "Unit", "test"),
                        attributeMap,
                        "craigmcc");

        final SaveConfigurationApiResult result = annotationClient.saveConfiguration(params);

        assertFalse(result.resultStatus.msg, result.resultStatus.isError);
        assertEquals("", result.resultStatus.msg);

        // success payload is the canonical configurationName, not an ObjectId
        assertEquals("test-config-001", result.configurationName);

        final ConfigurationDocument document = mongoClient.findConfiguration("test-config-001");
        assertNotNull(document);
        assertEquals("test-config-001", document.getConfigurationName());
        assertEquals("beamline", document.getCategory());
        assertEquals("A test configuration", document.getDescription());
        assertEquals("craigmcc", document.getModifiedBy());
        assertEquals(List.of("test", "unit"), document.getTags());
    }

    /**
     * Verifies the full-replace upsert semantics through the client: a second save omitting fields
     * that the first save supplied clears them rather than preserving them.
     */
    @Test
    public void testSaveConfigurationFullReplaceUpsert() {

        final Map<String, String> attributeMap = new LinkedHashMap<>();
        attributeMap.put("facility", "lcls");

        final SaveConfigurationApiResult firstResult = annotationClient.saveConfiguration(
                new AnnotationClient.SaveConfigurationParams(
                        "test-config-002",
                        "beamline",
                        "first description",
                        "test-parent",
                        List.of("first"),
                        attributeMap,
                        "craigmcc"));
        assertFalse(firstResult.resultStatus.msg, firstResult.resultStatus.isError);

        final ConfigurationDocument firstDocument = mongoClient.findConfiguration("test-config-002");
        assertNotNull(firstDocument);
        assertEquals("first description", firstDocument.getDescription());
        assertNotNull(firstDocument.getCreatedAt());

        // save again with the optional fields omitted
        final SaveConfigurationApiResult secondResult = annotationClient.saveConfiguration(
                new AnnotationClient.SaveConfigurationParams(
                        "test-config-002", "beamline", null, null, null, null, null));
        assertFalse(secondResult.resultStatus.msg, secondResult.resultStatus.isError);
        assertEquals("test-config-002", secondResult.configurationName);

        final ConfigurationDocument secondDocument = mongoClient.findConfiguration("test-config-002");
        assertNotNull(secondDocument);

        // omitted fields are replaced, not preserved
        assertTrue(secondDocument.getDescription() == null || secondDocument.getDescription().isEmpty());
        assertTrue(secondDocument.getParentConfigurationName() == null
                || secondDocument.getParentConfigurationName().isEmpty());
        assertTrue(secondDocument.getTags() == null || secondDocument.getTags().isEmpty());

        // createdAt is preserved across the update and updatedAt is set
        assertEquals(firstDocument.getCreatedAt(), secondDocument.getCreatedAt());
        assertNotNull(secondDocument.getUpdatedAt());
    }

    /**
     * Verifies that a server rejection for a blank configurationName is surfaced via resultStatus
     * rather than thrown.
     */
    @Test
    public void testSaveConfigurationRejectBlankName() {

        final SaveConfigurationApiResult result = annotationClient.saveConfiguration(
                new AnnotationClient.SaveConfigurationParams(
                        null, "beamline", null, null, null, null, null));

        assertTrue(result.resultStatus.isError);
        assertTrue(result.resultStatus.msg,
                result.resultStatus.msg.contains("configurationName must be specified"));
        assertNull(result.configurationName);
    }

    // =========================================================================
    // saveConfigurationActivation tests
    // =========================================================================

    /**
     * Verifies that an activation saved without a clientActivationId returns the identifier the
     * server generated, which is the caller's only handle on the new record.
     */
    @Test
    public void testSaveActivationGeneratesClientActivationId() {

        final SaveConfigurationApiResult configResult = annotationClient.saveConfiguration(
                new AnnotationClient.SaveConfigurationParams(
                        "test-config-003", "category-003", null, null, null, null, null));
        assertFalse(configResult.resultStatus.msg, configResult.resultStatus.isError);

        final SaveConfigurationActivationApiResult result =
                annotationClient.saveConfigurationActivation(
                        new AnnotationClient.SaveConfigurationActivationParams(
                                null,
                                "test-config-003",
                                timestamp(1000L),
                                timestamp(2000L),
                                "generated id activation",
                                null,
                                null,
                                "craigmcc"));

        assertFalse(result.resultStatus.msg, result.resultStatus.isError);

        // the server generated an identifier and returned it to the caller
        assertNotNull(result.clientActivationId);
        assertFalse(result.clientActivationId.isBlank());

        final ConfigurationActivationDocument document =
                mongoClient.findConfigurationActivationById(result.clientActivationId);
        assertNotNull(document);
        assertEquals("test-config-003", document.getConfigurationName());
        assertEquals("generated id activation", document.getDescription());
    }

    /**
     * Verifies that a client-supplied clientActivationId is preserved and returned unchanged.
     */
    @Test
    public void testSaveActivationPreservesSuppliedClientActivationId() {

        final SaveConfigurationApiResult configResult = annotationClient.saveConfiguration(
                new AnnotationClient.SaveConfigurationParams(
                        "test-config-004", "category-004", null, null, null, null, null));
        assertFalse(configResult.resultStatus.msg, configResult.resultStatus.isError);

        final SaveConfigurationActivationApiResult result =
                annotationClient.saveConfigurationActivation(
                        new AnnotationClient.SaveConfigurationActivationParams(
                                "supplied-id-004",
                                "test-config-004",
                                timestamp(1000L),
                                timestamp(2000L),
                                null,
                                null,
                                null,
                                null));

        assertFalse(result.resultStatus.msg, result.resultStatus.isError);
        assertEquals("supplied-id-004", result.clientActivationId);

        final ConfigurationActivationDocument document =
                mongoClient.findConfigurationActivationById("supplied-id-004");
        assertNotNull(document);
        assertEquals("test-config-004", document.getConfigurationName());
    }

    /**
     * Verifies that an activation saved with a null endTime is accepted and stored as an open-ended
     * interval.  This is the end-to-end counterpart of the field-presence assertion in
     * testBuildActivationRequestOmitsUnsuppliedOptionalFields.
     */
    @Test
    public void testSaveActivationOpenEndedInterval() {

        final SaveConfigurationApiResult configResult = annotationClient.saveConfiguration(
                new AnnotationClient.SaveConfigurationParams(
                        "test-config-005", "category-005", null, null, null, null, null));
        assertFalse(configResult.resultStatus.msg, configResult.resultStatus.isError);

        final SaveConfigurationActivationApiResult result =
                annotationClient.saveConfigurationActivation(
                        new AnnotationClient.SaveConfigurationActivationParams(
                                "open-ended-005",
                                "test-config-005",
                                timestamp(1000L),
                                null,
                                null,
                                null,
                                null,
                                null));

        assertFalse(result.resultStatus.msg, result.resultStatus.isError);
        assertEquals("open-ended-005", result.clientActivationId);

        final ConfigurationActivationDocument document =
                mongoClient.findConfigurationActivationById("open-ended-005");
        assertNotNull(document);
        assertNotNull(document.getStartTime());

        // an omitted endTime is stored as an open-ended interval
        assertNull(document.getEndTime());
    }

    /**
     * Verifies that the activation overlap rejection is surfaced via resultStatus rather than
     * thrown, with the server's message reaching the caller intact.  This is the rejection the
     * desktop-app UI is most likely to encounter.  The overlap rule itself is covered by
     * ConfigurationIT; what matters here is the surfacing mechanism.
     */
    @Test
    public void testSaveActivationRejectOverlapSurfacedViaResultStatus() {

        final SaveConfigurationApiResult configResult = annotationClient.saveConfiguration(
                new AnnotationClient.SaveConfigurationParams(
                        "test-config-006", "category-006", null, null, null, null, null));
        assertFalse(configResult.resultStatus.msg, configResult.resultStatus.isError);

        final SaveConfigurationActivationApiResult firstResult =
                annotationClient.saveConfigurationActivation(
                        new AnnotationClient.SaveConfigurationActivationParams(
                                "overlap-first-006",
                                "test-config-006",
                                timestamp(1000L),
                                timestamp(2000L),
                                null, null, null, null));
        assertFalse(firstResult.resultStatus.msg, firstResult.resultStatus.isError);

        // second activation overlapping the first
        final SaveConfigurationActivationApiResult secondResult =
                annotationClient.saveConfigurationActivation(
                        new AnnotationClient.SaveConfigurationActivationParams(
                                "overlap-second-006",
                                "test-config-006",
                                timestamp(1500L),
                                timestamp(2500L),
                                null, null, null, null));

        assertTrue(secondResult.resultStatus.isError);
        assertTrue(secondResult.resultStatus.msg,
                secondResult.resultStatus.msg.contains("overlapping activation exists"));
        assertNull(secondResult.clientActivationId);
    }

    /**
     * Verifies that an activation referencing a configuration that does not exist is rejected via
     * resultStatus rather than thrown.
     */
    @Test
    public void testSaveActivationRejectUnresolvedConfigurationName() {

        final SaveConfigurationActivationApiResult result =
                annotationClient.saveConfigurationActivation(
                        new AnnotationClient.SaveConfigurationActivationParams(
                                null,
                                "no-such-config",
                                timestamp(1000L),
                                timestamp(2000L),
                                null, null, null, null));

        assertTrue(result.resultStatus.isError);
        // the referenced configuration not existing is a business rule, so this must be a REJECT
        // rather than an ERROR: the test's name and the wire status have to agree (issue #235)
        assertEquals(ApiResultStatus.REJECT, result.apiResultStatus);
        assertTrue(result.isReject());
        assertTrue(result.resultStatus.msg,
                result.resultStatus.msg.contains("no Configuration found for configurationName"));
        assertNull(result.clientActivationId);
    }

    // =========================================================================
    // getConfiguration tests
    // =========================================================================

    /**
     * Verifies that getConfiguration returns the saved record.
     */
    @Test
    public void testGetConfigurationSuccess() {

        final Map<String, String> attributeMap = new LinkedHashMap<>();
        attributeMap.put("facility", "lcls");

        final SaveConfigurationApiResult saveResult = annotationClient.saveConfiguration(
                new AnnotationClient.SaveConfigurationParams(
                        "test-config-007",
                        "beamline",
                        "a retrievable configuration",
                        "test-parent-007",
                        List.of("TEST", "Unit"),
                        attributeMap,
                        "craigmcc"));
        assertFalse(saveResult.resultStatus.msg, saveResult.resultStatus.isError);

        final GetConfigurationApiResult result = annotationClient.getConfiguration("test-config-007");

        assertFalse(result.resultStatus.msg, result.resultStatus.isError);
        assertEquals("", result.resultStatus.msg);
        assertNotNull(result.configuration);

        assertEquals("test-config-007", result.configuration.getConfigurationName());
        assertEquals("beamline", result.configuration.getCategory());
        assertEquals("a retrievable configuration", result.configuration.getDescription());
        assertEquals("test-parent-007", result.configuration.getParentConfigurationName());
        assertEquals("craigmcc", result.configuration.getModifiedBy());

        // tags are normalized server-side
        assertEquals(List.of("test", "unit"), result.configuration.getTagsList());

        assertEquals(1, result.configuration.getAttributesCount());
        assertEquals("facility", result.configuration.getAttributes(0).getName());
        assertEquals("lcls", result.configuration.getAttributes(0).getValue());
    }

    /**
     * Verifies that a missing record is surfaced as an error rather than an empty successful
     * result.  The server signals not-found as a rejection for all of the single-record getters, so
     * a caller using this method as an existence check must currently inspect resultStatus.msg.
     */
    @Test
    public void testGetConfigurationRejectNotFound() {

        final GetConfigurationApiResult result = annotationClient.getConfiguration("no-such-config");

        assertTrue(result.resultStatus.isError);
        assertTrue(result.resultStatus.msg,
                result.resultStatus.msg.contains("no Configuration record found for: no-such-config"));
        assertNull(result.configuration);

        // the rejection is categorized without matching on the message
        assertEquals(ApiResultStatus.REJECT, result.apiResultStatus);
        assertTrue(result.isReject());
    }

    // =========================================================================
    // apiResultStatus tests
    // =========================================================================

    /**
     * Verifies that a successful call carries ApiResultStatus.NONE and is not reported as a
     * rejection.
     */
    @Test
    public void testApiResultStatusNoneOnSuccess() {

        final SaveConfigurationApiResult saveResult = annotationClient.saveConfiguration(
                new AnnotationClient.SaveConfigurationParams(
                        "test-config-status-001", "category-status-001", null, null, null, null, null));

        assertFalse(saveResult.resultStatus.msg, saveResult.resultStatus.isError);
        assertEquals(ApiResultStatus.NONE, saveResult.apiResultStatus);
        assertFalse(saveResult.isReject());

        final GetConfigurationApiResult getResult =
                annotationClient.getConfiguration("test-config-status-001");

        assertFalse(getResult.resultStatus.msg, getResult.resultStatus.isError);
        assertEquals(ApiResultStatus.NONE, getResult.apiResultStatus);
        assertFalse(getResult.isReject());
    }

    /**
     * Verifies that a server-side validation rejection reaches the caller as REJECT, the same
     * status as a not-found rejection.
     *
     * This equivalence is the reason the client exposes isReject() rather than an isNotFound()
     * predicate: the services report a malformed request and an absent record with the same wire
     * status, so a not-found predicate built on the status alone would report a rejected bad
     * request as "the record does not exist".  A caller using isReject() to decide whether a save
     * would overwrite an existing record must validate its request first, since a rejection here
     * does not establish that the record is absent.
     */
    @Test
    public void testApiResultStatusRejectOnValidationFailure() {

        final SaveConfigurationApiResult result = annotationClient.saveConfiguration(
                new AnnotationClient.SaveConfigurationParams(
                        null, "beamline", null, null, null, null, null));

        assertTrue(result.resultStatus.isError);
        assertEquals(ApiResultStatus.REJECT, result.apiResultStatus);
        assertTrue(result.isReject());

        // a validation reject and a not-found reject are indistinguishable by status
        final GetConfigurationApiResult notFoundResult =
                annotationClient.getConfiguration("no-such-config-status");
        assertEquals(notFoundResult.apiResultStatus, result.apiResultStatus);
    }

    /**
     * Verifies that the overlap constraint failure arrives categorized as REJECT, not ERROR.
     *
     * The overlap check runs in MongoSyncAnnotationClient after the request has already passed
     * validation.  It used to report through the MongoSaveResult error path, so "an overlapping
     * activation already exists" — which reads like a rejection of the request — reached the caller
     * as RESULT_STATUS_ERROR and a caller branching on isReject() would not catch it.  Issue #235
     * gave the result wrappers an isReject flag and classified this site as a business rule, so the
     * constraint now arrives as a rejection: retrying the identical request is pointless, and the
     * condition is a correctable mistake rather than a service failure.
     */
    @Test
    public void testApiResultStatusRejectOnOverlapConstraint() {

        final SaveConfigurationApiResult configResult = annotationClient.saveConfiguration(
                new AnnotationClient.SaveConfigurationParams(
                        "test-config-status-002", "category-status-002", null, null, null, null, null));
        assertFalse(configResult.resultStatus.msg, configResult.resultStatus.isError);

        final SaveConfigurationActivationApiResult firstResult =
                annotationClient.saveConfigurationActivation(
                        new AnnotationClient.SaveConfigurationActivationParams(
                                "overlap-status-first",
                                "test-config-status-002",
                                timestamp(1000L),
                                timestamp(2000L),
                                null, null, null, null));
        assertFalse(firstResult.resultStatus.msg, firstResult.resultStatus.isError);
        assertEquals(ApiResultStatus.NONE, firstResult.apiResultStatus);

        final SaveConfigurationActivationApiResult secondResult =
                annotationClient.saveConfigurationActivation(
                        new AnnotationClient.SaveConfigurationActivationParams(
                                "overlap-status-second",
                                "test-config-status-002",
                                timestamp(1500L),
                                timestamp(2500L),
                                null, null, null, null));

        assertTrue(secondResult.resultStatus.isError);
        assertEquals(ApiResultStatus.REJECT, secondResult.apiResultStatus);
        assertTrue(secondResult.isReject());
        assertTrue(secondResult.resultStatus.msg.contains("overlapping activation exists"));
    }

}
