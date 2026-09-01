package com.ospreydcs.dp.service.integration.annotation;

import com.ospreydcs.dp.client.AnnotationClient;
import com.ospreydcs.dp.client.result.ApiResultStatus;
import com.ospreydcs.dp.client.result.GetConfigurationActivationApiResult;
import com.ospreydcs.dp.client.result.GetConfigurationApiResult;
import com.ospreydcs.dp.client.result.QueryConfigurationActivationsApiResult;
import com.ospreydcs.dp.client.result.QueryConfigurationsApiResult;
import com.ospreydcs.dp.client.result.SaveConfigurationActivationApiResult;
import com.ospreydcs.dp.client.result.SaveConfigurationApiResult;
import com.ospreydcs.dp.grpc.v1.annotation.GetConfigurationActivationRequest;
import com.ospreydcs.dp.grpc.v1.annotation.GetConfigurationRequest;
import com.ospreydcs.dp.grpc.v1.annotation.QueryConfigurationActivationsRequest;
import com.ospreydcs.dp.grpc.v1.annotation.QueryConfigurationsRequest;
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

    // =========================================================================
    // buildQueryConfigurationsRequest tests
    // =========================================================================

    /**
     * Verifies that a params record with nothing supplied emits NO criteria, rather than empty
     * ones.  The server rejects an empty TagsCriterion.values and an empty NameCriterion, so an
     * empty criterion for an omitted filter would turn an unfiltered query into a rejected request.
     */
    @Test
    public void testBuildQueryConfigurationsRequestOmitsUnsuppliedCriteria() {

        final QueryConfigurationsRequest request = AnnotationClient.buildQueryConfigurationsRequest(
                new AnnotationClient.QueryConfigurationsParams(
                        null, null, null, null, null, 0, null));

        assertEquals(0, request.getCriteriaCount());
        assertEquals(0, request.getLimit());
        assertEquals("", request.getPageToken());
    }

    /**
     * Verifies that empty collections are treated as unsupplied, and specifically that an empty
     * tagsAnyOf does not emit an empty TagsCriterion.
     */
    @Test
    public void testBuildQueryConfigurationsRequestOmitsEmptyCollections() {

        final QueryConfigurationsRequest request = AnnotationClient.buildQueryConfigurationsRequest(
                new AnnotationClient.QueryConfigurationsParams(
                        new AnnotationClient.TextMatch(List.of(), List.of(), List.of()),
                        List.of(),
                        List.of(),
                        List.of(),
                        List.of(),
                        -1,
                        "  "));

        assertEquals(0, request.getCriteriaCount());
        assertEquals(0, request.getLimit());
        assertEquals("", request.getPageToken());
    }

    /**
     * Verifies that collections CONTAINING blank entries are treated as unsupplied, a distinct and
     * more dangerous case than the empty collections covered above.
     *
     * A blank prefix or contains value survives protobuf serialization as a zero-length repeated
     * entry, so the NameCriterion passes the server's "at least one of exact/prefix/contains"
     * check, and MongoQueryFilterBuilder.nameMatchFilter() turns it into a match-all regex.  A
     * caller binding an unfilled optional UI field would silently retrieve every configuration
     * rather than applying no filter.
     */
    @Test
    public void testBuildQueryConfigurationsRequestOmitsBlankEntries() {

        final QueryConfigurationsRequest request = AnnotationClient.buildQueryConfigurationsRequest(
                new AnnotationClient.QueryConfigurationsParams(
                        new AnnotationClient.TextMatch(List.of(""), List.of("  "), List.of("\t")),
                        List.of("", "  "),
                        List.of(" "),
                        List.of(new AnnotationClient.AttributeCriterion("  ", List.of("v"))),
                        List.of(""),
                        0,
                        null));

        assertEquals(
                "criteria built entirely from blank entries must not be emitted",
                0,
                request.getCriteriaCount());
    }

    /**
     * Verifies that a blank prefix alone does not produce a match-all query — the single most
     * dangerous input, asserted separately from the aggregate test above.
     */
    @Test
    public void testBuildQueryConfigurationsRequestBlankPrefixEmitsNoCriterion() {

        final QueryConfigurationsRequest request = AnnotationClient.buildQueryConfigurationsRequest(
                new AnnotationClient.QueryConfigurationsParams(
                        new AnnotationClient.TextMatch(null, List.of(""), null),
                        null, null, null, null, 0, null));

        assertEquals(
                "a blank prefix must emit no criterion rather than a match-all regex",
                0,
                request.getCriteriaCount());
    }

    /**
     * Verifies that blank entries are dropped individually while real values in the same list
     * survive.
     */
    @Test
    public void testBuildQueryConfigurationsRequestDropsBlanksKeepsRealValues() {

        final QueryConfigurationsRequest request = AnnotationClient.buildQueryConfigurationsRequest(
                new AnnotationClient.QueryConfigurationsParams(
                        new AnnotationClient.TextMatch(null, List.of("cfg-", "", "  "), null),
                        List.of("beamline", ""),
                        null, null, null, 0, null));

        assertEquals(2, request.getCriteriaCount());

        final QueryConfigurationsRequest.QueryConfigurationsCriterion.NameCriterion nameCriterion =
                request.getCriteria(0).getNameCriterion();
        assertEquals(1, nameCriterion.getPrefixCount());
        assertEquals("cfg-", nameCriterion.getPrefix(0));

        assertEquals(
                List.of("beamline"),
                request.getCriteria(1).getCategoryCriterion().getValuesList());
    }

    /**
     * Verifies that a blank attribute key emits no criterion.  QueryConfigurationsJob validates
     * AttributesCriterion.key with isBlank(), so a whitespace key would otherwise produce an
     * avoidable REJECT instead of the omitted filter the caller intended.
     */
    @Test
    public void testBuildQueryConfigurationsRequestOmitsBlankAttributeKey() {

        final QueryConfigurationsRequest request = AnnotationClient.buildQueryConfigurationsRequest(
                new AnnotationClient.QueryConfigurationsParams(
                        null,
                        null,
                        null,
                        List.of(
                                new AnnotationClient.AttributeCriterion("  ", List.of("v")),
                                new AnnotationClient.AttributeCriterion("realkey", List.of("v"))),
                        null,
                        0,
                        null));

        assertEquals(1, request.getCriteriaCount());
        assertEquals("realkey", request.getCriteria(0).getAttributesCriterion().getKey());
    }

    /**
     * Verifies the same blank-entry handling for queryConfigurationActivations, whose criteria are
     * all exact-match value lists plus an attribute key validated with isBlank() by
     * QueryConfigurationActivationsJob.
     */
    @Test
    public void testBuildQueryActivationsRequestOmitsBlankEntries() {

        final QueryConfigurationActivationsRequest request =
                AnnotationClient.buildQueryConfigurationActivationsRequest(
                        new AnnotationClient.QueryConfigurationActivationsParams(
                                null,
                                null,
                                null,
                                List.of("", "  "),
                                List.of(" "),
                                List.of(""),
                                List.of("  "),
                                List.of(new AnnotationClient.AttributeCriterion(" ", null)),
                                0,
                                null));

        assertEquals(
                "criteria built entirely from blank entries must not be emitted",
                0,
                request.getCriteriaCount());
    }

    /**
     * Verifies that queryConfigurationActivations drops blank entries individually while retaining
     * real values in the same list.
     */
    @Test
    public void testBuildQueryActivationsRequestDropsBlanksKeepsRealValues() {

        final QueryConfigurationActivationsRequest request =
                AnnotationClient.buildQueryConfigurationActivationsRequest(
                        new AnnotationClient.QueryConfigurationActivationsParams(
                                null,
                                null,
                                null,
                                List.of("cfg-real", "", "  "),
                                null,
                                null,
                                null,
                                null,
                                0,
                                null));

        assertEquals(1, request.getCriteriaCount());
        assertEquals(
                List.of("cfg-real"),
                request.getCriteria(0).getConfigurationNameCriterion().getValuesList());
    }

    /**
     * Verifies that every supplied field maps to the right criterion type with the right values.
     */
    @Test
    public void testBuildQueryConfigurationsRequestPopulatesSuppliedFields() {

        final QueryConfigurationsRequest request = AnnotationClient.buildQueryConfigurationsRequest(
                new AnnotationClient.QueryConfigurationsParams(
                        new AnnotationClient.TextMatch(
                                List.of("cfg-exact"), List.of("cfg-"), List.of("fg")),
                        List.of("beamline"),
                        List.of("tag1", "tag2"),
                        List.of(new AnnotationClient.AttributeCriterion("facility", List.of("lcls"))),
                        List.of("parent-1"),
                        50,
                        "token-xyz"));

        assertEquals(5, request.getCriteriaCount());

        assertTrue(request.getCriteria(0).hasNameCriterion());
        assertEquals(List.of("cfg-exact"), request.getCriteria(0).getNameCriterion().getExactList());
        assertEquals(List.of("cfg-"), request.getCriteria(0).getNameCriterion().getPrefixList());
        assertEquals(List.of("fg"), request.getCriteria(0).getNameCriterion().getContainsList());

        assertTrue(request.getCriteria(1).hasCategoryCriterion());
        assertEquals(
                List.of("beamline"), request.getCriteria(1).getCategoryCriterion().getValuesList());

        assertTrue(request.getCriteria(2).hasTagsCriterion());
        assertEquals(
                List.of("tag1", "tag2"), request.getCriteria(2).getTagsCriterion().getValuesList());

        assertTrue(request.getCriteria(3).hasAttributesCriterion());
        assertEquals("facility", request.getCriteria(3).getAttributesCriterion().getKey());
        assertEquals(
                List.of("lcls"), request.getCriteria(3).getAttributesCriterion().getValuesList());

        assertTrue(request.getCriteria(4).hasParentCriterion());
        assertEquals(
                List.of("parent-1"), request.getCriteria(4).getParentCriterion().getValuesList());

        assertEquals(50, request.getLimit());
        assertEquals("token-xyz", request.getPageToken());
    }

    /**
     * Verifies that an AttributeCriterion with no values produces a key-only existence criterion.
     */
    @Test
    public void testBuildQueryConfigurationsRequestAttributeKeyOnly() {

        final QueryConfigurationsRequest request = AnnotationClient.buildQueryConfigurationsRequest(
                new AnnotationClient.QueryConfigurationsParams(
                        null,
                        null,
                        null,
                        List.of(new AnnotationClient.AttributeCriterion("facility", null)),
                        null,
                        0,
                        null));

        assertEquals(1, request.getCriteriaCount());
        assertEquals("facility", request.getCriteria(0).getAttributesCriterion().getKey());
        assertTrue(request.getCriteria(0).getAttributesCriterion().getValuesList().isEmpty());
    }

    // =========================================================================
    // buildQueryConfigurationActivationsRequest tests
    // =========================================================================

    /**
     * Verifies that a params record with nothing supplied emits no criteria.
     */
    @Test
    public void testBuildQueryActivationsRequestOmitsUnsuppliedCriteria() {

        final QueryConfigurationActivationsRequest request =
                AnnotationClient.buildQueryConfigurationActivationsRequest(
                        new AnnotationClient.QueryConfigurationActivationsParams(
                                null, null, null, null, null, null, null, null, 0, null));

        assertEquals(0, request.getCriteriaCount());
        assertEquals(0, request.getLimit());
        assertEquals("", request.getPageToken());
    }

    /**
     * Verifies that every supplied field maps to the right criterion type with the right values.
     */
    @Test
    public void testBuildQueryActivationsRequestPopulatesSuppliedFields() {

        final QueryConfigurationActivationsRequest request =
                AnnotationClient.buildQueryConfigurationActivationsRequest(
                        new AnnotationClient.QueryConfigurationActivationsParams(
                                timestamp(1500L),
                                timestamp(1000L),
                                timestamp(2000L),
                                List.of("cfg-1"),
                                List.of("activation-1"),
                                List.of("beamline"),
                                List.of("tag1"),
                                List.of(new AnnotationClient.AttributeCriterion(
                                        "facility", List.of("lcls"))),
                                40,
                                "token-act"));

        assertEquals(7, request.getCriteriaCount());

        assertTrue(request.getCriteria(0).hasTimestampCriterion());
        assertEquals(
                1500L,
                request.getCriteria(0).getTimestampCriterion().getTimestamp().getEpochSeconds());

        assertTrue(request.getCriteria(1).hasTimeRangeCriterion());
        assertEquals(
                1000L,
                request.getCriteria(1).getTimeRangeCriterion().getStartTime().getEpochSeconds());
        assertEquals(
                2000L,
                request.getCriteria(1).getTimeRangeCriterion().getEndTime().getEpochSeconds());

        assertTrue(request.getCriteria(2).hasConfigurationNameCriterion());
        assertEquals(
                List.of("cfg-1"),
                request.getCriteria(2).getConfigurationNameCriterion().getValuesList());

        assertTrue(request.getCriteria(3).hasClientActivationIdCriterion());
        assertEquals(
                List.of("activation-1"),
                request.getCriteria(3).getClientActivationIdCriterion().getValuesList());

        assertTrue(request.getCriteria(4).hasCategoryCriterion());
        assertEquals(
                List.of("beamline"), request.getCriteria(4).getCategoryCriterion().getValuesList());

        assertTrue(request.getCriteria(5).hasTagsCriterion());
        assertEquals(List.of("tag1"), request.getCriteria(5).getTagsCriterion().getValuesList());

        assertTrue(request.getCriteria(6).hasAttributesCriterion());
        assertEquals("facility", request.getCriteria(6).getAttributesCriterion().getKey());

        assertEquals(40, request.getLimit());
        assertEquals("token-act", request.getPageToken());
    }

    /**
     * Verifies that a TimeRangeCriterion is emitted only when BOTH bounds are supplied.  A partial
     * criterion would be rejected by the server, so the builder emits nothing rather than half of
     * one — supplying only one bound is a caller mistake that must not become a rejected request.
     */
    @Test
    public void testBuildQueryActivationsRequestTimeRangeRequiresBothBounds() {

        // start only
        final QueryConfigurationActivationsRequest startOnly =
                AnnotationClient.buildQueryConfigurationActivationsRequest(
                        new AnnotationClient.QueryConfigurationActivationsParams(
                                null, timestamp(1000L), null, null, null, null, null, null, 0, null));
        assertEquals(0, startOnly.getCriteriaCount());

        // end only
        final QueryConfigurationActivationsRequest endOnly =
                AnnotationClient.buildQueryConfigurationActivationsRequest(
                        new AnnotationClient.QueryConfigurationActivationsParams(
                                null, null, timestamp(2000L), null, null, null, null, null, 0, null));
        assertEquals(0, endOnly.getCriteriaCount());

        // both
        final QueryConfigurationActivationsRequest both =
                AnnotationClient.buildQueryConfigurationActivationsRequest(
                        new AnnotationClient.QueryConfigurationActivationsParams(
                                null, timestamp(1000L), timestamp(2000L), null, null, null, null,
                                null, 0, null));
        assertEquals(1, both.getCriteriaCount());
        assertTrue(both.getCriteria(0).hasTimeRangeCriterion());
    }

    // =========================================================================
    // buildGetConfigurationActivation request tests
    // =========================================================================

    /**
     * Verifies that each of the two activation get-request builders sets its own arm of the key
     * oneof, and only that arm.  Two named builders rather than one nullable-argument builder is
     * what keeps "both supplied" and "neither supplied" from arising client-side.
     */
    @Test
    public void testBuildGetActivationRequestSetsCorrectOneofArm() {

        final GetConfigurationActivationRequest byId =
                AnnotationClient.buildGetConfigurationActivationByIdRequest("activation-1");
        assertEquals(
                GetConfigurationActivationRequest.KeyCase.CLIENTACTIVATIONID, byId.getKeyCase());
        assertEquals("activation-1", byId.getClientActivationId());

        final GetConfigurationActivationRequest byCompositeKey =
                AnnotationClient.buildGetConfigurationActivationByCompositeKeyRequest(
                        "cfg-1", timestamp(1000L));
        assertEquals(
                GetConfigurationActivationRequest.KeyCase.COMPOSITEKEY,
                byCompositeKey.getKeyCase());
        assertEquals("cfg-1", byCompositeKey.getCompositeKey().getConfigurationName());
        assertEquals(1000L, byCompositeKey.getCompositeKey().getStartTime().getEpochSeconds());
    }

    // =========================================================================
    // queryConfigurations tests
    // =========================================================================

    /*
     * Saves a configuration with the given name and category, asserting success.
     */
    private void saveConfiguration(String configurationName, String category) {
        final SaveConfigurationApiResult result = annotationClient.saveConfiguration(
                new AnnotationClient.SaveConfigurationParams(
                        configurationName, category, null, null, null, null, "craigmcc"));
        assertFalse(result.resultStatus.msg, result.resultStatus.isError);
    }

    /**
     * Verifies that a query matching records returns them through the wrapper.
     */
    @Test
    public void testQueryConfigurationsSuccess() {

        saveConfiguration("query-cfg-001", "beamline");
        saveConfiguration("query-cfg-002", "beamline");

        final QueryConfigurationsApiResult result = annotationClient.queryConfigurations(
                new AnnotationClient.QueryConfigurationsParams(
                        new AnnotationClient.TextMatch(null, List.of("query-cfg-"), null),
                        null, null, null, null, 100, null));

        assertFalse(result.resultStatus.msg, result.resultStatus.isError);
        assertEquals(ApiResultStatus.NONE, result.apiResultStatus);
        assertNotNull(result.configurations);
        assertEquals(2, result.configurations.size());

        final List<String> names = result.configurations.stream()
                .map(c -> c.getConfigurationName()).sorted().toList();
        assertEquals(List.of("query-cfg-001", "query-cfg-002"), names);
    }

    /**
     * Verifies that a query matching nothing is a normal SUCCESS with an empty list, not a
     * rejection.
     */
    @Test
    public void testQueryConfigurationsEmptyResultIsSuccess() {

        final QueryConfigurationsApiResult result = annotationClient.queryConfigurations(
                new AnnotationClient.QueryConfigurationsParams(
                        new AnnotationClient.TextMatch(List.of("no-such-config"), null, null),
                        null, null, null, null, 100, null));

        assertFalse(result.resultStatus.msg, result.resultStatus.isError);
        assertEquals(ApiResultStatus.NONE, result.apiResultStatus);
        assertFalse(result.isReject());
        assertNotNull(result.configurations);
        assertTrue(result.configurations.isEmpty());
        assertEquals("", result.nextPageToken);
    }

    /**
     * Verifies a full paging round-trip through the wrapper, including the BLANK final-page token.
     * The wrapper returns nextPageToken directly, so this test needs none of the raw-stub
     * workaround that ConfigurationIT.testQueryConfigurationsPagination resorts to — and unlike
     * that test, it asserts end-of-pagination rather than leaving it unverified.
     */
    @Test
    public void testQueryConfigurationsPagingRoundTrip() {

        saveConfiguration("page-cfg-001", "beamline");
        saveConfiguration("page-cfg-002", "beamline");
        saveConfiguration("page-cfg-003", "beamline");

        final QueryConfigurationsApiResult firstPage = annotationClient.queryConfigurations(
                new AnnotationClient.QueryConfigurationsParams(
                        new AnnotationClient.TextMatch(null, List.of("page-cfg-"), null),
                        null, null, null, null, 2, null));

        assertFalse(firstPage.resultStatus.msg, firstPage.resultStatus.isError);
        assertEquals(2, firstPage.configurations.size());
        assertFalse(
                "expected a non-empty nextPageToken on a truncated page",
                firstPage.nextPageToken.isEmpty());

        final QueryConfigurationsApiResult secondPage = annotationClient.queryConfigurations(
                new AnnotationClient.QueryConfigurationsParams(
                        new AnnotationClient.TextMatch(null, List.of("page-cfg-"), null),
                        null, null, null, null, 2, firstPage.nextPageToken));

        assertFalse(secondPage.resultStatus.msg, secondPage.resultStatus.isError);
        assertEquals(1, secondPage.configurations.size());
        assertEquals(
                "the final page must carry a blank nextPageToken", "", secondPage.nextPageToken);

        final List<String> allNames = new java.util.ArrayList<String>();
        firstPage.configurations.forEach(c -> allNames.add(c.getConfigurationName()));
        secondPage.configurations.forEach(c -> allNames.add(c.getConfigurationName()));
        allNames.sort(null);
        assertEquals(List.of("page-cfg-001", "page-cfg-002", "page-cfg-003"), allNames);
    }

    /**
     * An empty criteria list is match-all, not a rejection (#245).  This is the browse-all entry
     * point the dp-desktop-app configuration explore view opens with.
     *
     * <p>Replaces testQueryConfigurationsRejectsEmptyCriteria, which pinned the pre-#245 rejection.
     */
    @Test
    public void testQueryConfigurationsEmptyCriteriaMatchesAll() {

        saveConfiguration("matchall-cfg-001", "beamline");
        saveConfiguration("matchall-cfg-002", "beamline");
        saveConfiguration("matchall-cfg-003", "vacuum");

        final QueryConfigurationsApiResult result = annotationClient.queryConfigurations(
                new AnnotationClient.QueryConfigurationsParams(
                        null, null, null, null, null, 100, null));

        assertFalse(result.resultStatus.msg, result.resultStatus.isError);
        assertEquals(ApiResultStatus.NONE, result.apiResultStatus);
        assertFalse(result.isReject());
        assertNotNull(result.configurations);
        assertEquals(3, result.configurations.size());

        final List<String> names = result.configurations.stream()
                .map(c -> c.getConfigurationName()).sorted().toList();
        assertEquals(
                List.of("matchall-cfg-001", "matchall-cfg-002", "matchall-cfg-003"), names);
    }

    /**
     * Match-all plus paging enumerates every configuration exactly once, with a blank token on the
     * final page.
     */
    @Test
    public void testQueryConfigurationsEmptyCriteriaPagesThroughAll() {

        saveConfiguration("matchallpage-cfg-001", "beamline");
        saveConfiguration("matchallpage-cfg-002", "beamline");
        saveConfiguration("matchallpage-cfg-003", "vacuum");
        saveConfiguration("matchallpage-cfg-004", "vacuum");
        saveConfiguration("matchallpage-cfg-005", "cryo");

        final List<String> seen = new java.util.ArrayList<String>();
        String pageToken = null;
        int pageCount = 0;

        while (true) {
            final QueryConfigurationsApiResult page = annotationClient.queryConfigurations(
                    new AnnotationClient.QueryConfigurationsParams(
                            null, null, null, null, null, 2, pageToken));

            assertFalse(page.resultStatus.msg, page.resultStatus.isError);
            assertNotNull(page.configurations);
            page.configurations.forEach(c -> seen.add(c.getConfigurationName()));

            pageCount++;
            assertTrue("paging did not terminate", pageCount <= 10);

            if (page.nextPageToken.isEmpty()) {
                assertEquals("the final page should hold the remainder", 1, page.configurations.size());
                break;
            }
            assertEquals("a non-final page should be full", 2, page.configurations.size());
            pageToken = page.nextPageToken;
        }

        assertEquals(3, pageCount);
        seen.sort(null);
        assertEquals(
                List.of("matchallpage-cfg-001", "matchallpage-cfg-002", "matchallpage-cfg-003",
                        "matchallpage-cfg-004", "matchallpage-cfg-005"),
                seen);
    }

    // =========================================================================
    // queryConfigurationActivations tests
    // =========================================================================

    /**
     * Verifies that an activation query matching records returns them through the wrapper.
     */
    @Test
    public void testQueryConfigurationActivationsSuccess() {

        saveConfiguration("query-act-cfg-001", "beamline");

        final SaveConfigurationActivationApiResult saveResult =
                annotationClient.saveConfigurationActivation(
                        new AnnotationClient.SaveConfigurationActivationParams(
                                "query-act-001",
                                "query-act-cfg-001",
                                timestamp(1000L),
                                timestamp(2000L),
                                null, null, null, "craigmcc"));
        assertFalse(saveResult.resultStatus.msg, saveResult.resultStatus.isError);

        final QueryConfigurationActivationsApiResult result =
                annotationClient.queryConfigurationActivations(
                        new AnnotationClient.QueryConfigurationActivationsParams(
                                null, null, null,
                                List.of("query-act-cfg-001"),
                                null, null, null, null, 100, null));

        assertFalse(result.resultStatus.msg, result.resultStatus.isError);
        assertEquals(ApiResultStatus.NONE, result.apiResultStatus);
        assertNotNull(result.configurationActivations);
        assertEquals(1, result.configurationActivations.size());
        assertEquals(
                "query-act-001",
                result.configurationActivations.get(0).getClientActivationId());
    }

    /**
     * Verifies that an activation query matching nothing is a normal SUCCESS with an empty list.
     */
    @Test
    public void testQueryConfigurationActivationsEmptyResultIsSuccess() {

        final QueryConfigurationActivationsApiResult result =
                annotationClient.queryConfigurationActivations(
                        new AnnotationClient.QueryConfigurationActivationsParams(
                                null, null, null,
                                List.of("no-such-config"),
                                null, null, null, null, 100, null));

        assertFalse(result.resultStatus.msg, result.resultStatus.isError);
        assertEquals(ApiResultStatus.NONE, result.apiResultStatus);
        assertFalse(result.isReject());
        assertNotNull(result.configurationActivations);
        assertTrue(result.configurationActivations.isEmpty());
        assertEquals("", result.nextPageToken);
    }

    /**
     * An empty criteria list is match-all, not a rejection (#245).
     *
     * <p>Replaces testQueryConfigurationActivationsRejectsEmptyCriteria, which pinned the pre-#245
     * rejection.  Note the rejection message this used to assert differed from the other two
     * queries ("criteria must not be empty", without "list"); that divergence is now moot.
     */
    @Test
    public void testQueryConfigurationActivationsEmptyCriteriaMatchesAll() {

        saveConfiguration("matchall-act-cfg-001", "beamline");

        // two non-overlapping activations: the overlap constraint forbids overlapping intervals
        // for the same configurationName
        final SaveConfigurationActivationApiResult save1 =
                annotationClient.saveConfigurationActivation(
                        new AnnotationClient.SaveConfigurationActivationParams(
                                "matchall-act-001", "matchall-act-cfg-001",
                                timestamp(1000L), timestamp(2000L),
                                null, null, null, "craigmcc"));
        assertFalse(save1.resultStatus.msg, save1.resultStatus.isError);

        final SaveConfigurationActivationApiResult save2 =
                annotationClient.saveConfigurationActivation(
                        new AnnotationClient.SaveConfigurationActivationParams(
                                "matchall-act-002", "matchall-act-cfg-001",
                                timestamp(2000L), timestamp(3000L),
                                null, null, null, "craigmcc"));
        assertFalse(save2.resultStatus.msg, save2.resultStatus.isError);

        final QueryConfigurationActivationsApiResult result =
                annotationClient.queryConfigurationActivations(
                        new AnnotationClient.QueryConfigurationActivationsParams(
                                null, null, null, null, null, null, null, null, 100, null));

        assertFalse(result.resultStatus.msg, result.resultStatus.isError);
        assertEquals(ApiResultStatus.NONE, result.apiResultStatus);
        assertFalse(result.isReject());
        assertNotNull(result.configurationActivations);
        assertEquals(2, result.configurationActivations.size());

        final List<String> ids = result.configurationActivations.stream()
                .map(a -> a.getClientActivationId()).sorted().toList();
        assertEquals(List.of("matchall-act-001", "matchall-act-002"), ids);
    }

    // =========================================================================
    // getConfigurationActivation tests
    // =========================================================================

    /**
     * Verifies a get by clientActivationId, the arm dp-desktop-app#36 uses to detect an activation
     * id collision before saving.
     */
    @Test
    public void testGetConfigurationActivationByIdSuccess() {

        saveConfiguration("get-act-cfg-001", "beamline");

        final SaveConfigurationActivationApiResult saveResult =
                annotationClient.saveConfigurationActivation(
                        new AnnotationClient.SaveConfigurationActivationParams(
                                "get-act-001",
                                "get-act-cfg-001",
                                timestamp(1000L),
                                timestamp(2000L),
                                null, null, null, "craigmcc"));
        assertFalse(saveResult.resultStatus.msg, saveResult.resultStatus.isError);

        final GetConfigurationActivationApiResult result =
                annotationClient.getConfigurationActivationById("get-act-001");

        assertFalse(result.resultStatus.msg, result.resultStatus.isError);
        assertEquals(ApiResultStatus.NONE, result.apiResultStatus);
        assertNotNull(result.configurationActivation);
        assertEquals("get-act-001", result.configurationActivation.getClientActivationId());
        assertEquals(
                "get-act-cfg-001", result.configurationActivation.getConfigurationName());
        assertEquals(1000L, result.configurationActivation.getStartTime().getEpochSeconds());
    }

    /**
     * Verifies that a missing activation id is a REJECTION, not an empty success, and that the
     * rejection is categorized on apiResultStatus rather than only in the message.  A caller using
     * this as a collision check branches on isReject(), so the status must be right.
     */
    @Test
    public void testGetConfigurationActivationByIdRejectNotFound() {

        final GetConfigurationActivationApiResult result =
                annotationClient.getConfigurationActivationById("no-such-activation");

        assertTrue(result.resultStatus.isError);
        assertEquals(ApiResultStatus.REJECT, result.apiResultStatus);
        assertTrue(result.isReject());
        assertTrue(
                result.resultStatus.msg,
                result.resultStatus.msg.contains(
                        "no ConfigurationActivation record found for: clientActivationId: "
                                + "no-such-activation"));
        assertNull(result.configurationActivation);
    }

    /**
     * Verifies a get by the composite key arm, resolving the same record as the id arm.
     */
    @Test
    public void testGetConfigurationActivationByCompositeKeySuccess() {

        saveConfiguration("get-act-cfg-002", "beamline");

        final SaveConfigurationActivationApiResult saveResult =
                annotationClient.saveConfigurationActivation(
                        new AnnotationClient.SaveConfigurationActivationParams(
                                "get-act-002",
                                "get-act-cfg-002",
                                timestamp(3000L),
                                timestamp(4000L),
                                null, null, null, "craigmcc"));
        assertFalse(saveResult.resultStatus.msg, saveResult.resultStatus.isError);

        final GetConfigurationActivationApiResult result =
                annotationClient.getConfigurationActivationByCompositeKey(
                        "get-act-cfg-002", timestamp(3000L));

        assertFalse(result.resultStatus.msg, result.resultStatus.isError);
        assertEquals(ApiResultStatus.NONE, result.apiResultStatus);
        assertNotNull(result.configurationActivation);
        assertEquals("get-act-002", result.configurationActivation.getClientActivationId());
    }

    /**
     * Verifies that a composite key matching no record is a REJECTION rather than an empty success.
     */
    @Test
    public void testGetConfigurationActivationByCompositeKeyRejectNotFound() {

        final GetConfigurationActivationApiResult result =
                annotationClient.getConfigurationActivationByCompositeKey(
                        "no-such-config", timestamp(9000L));

        assertTrue(result.resultStatus.isError);
        assertEquals(ApiResultStatus.REJECT, result.apiResultStatus);
        assertTrue(result.isReject());
        assertTrue(
                result.resultStatus.msg,
                result.resultStatus.msg.contains(
                        "no ConfigurationActivation record found for: configurationName: "
                                + "no-such-config startTime: 9000"));
        assertNull(result.configurationActivation);
    }

}
