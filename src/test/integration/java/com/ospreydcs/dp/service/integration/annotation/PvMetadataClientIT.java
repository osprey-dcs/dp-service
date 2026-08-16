package com.ospreydcs.dp.service.integration.annotation;

import com.ospreydcs.dp.client.AnnotationClient;
import com.ospreydcs.dp.client.result.SavePvMetadataApiResult;
import com.ospreydcs.dp.grpc.v1.annotation.SavePvMetadataRequest;
import com.ospreydcs.dp.service.common.bson.pvmetadata.PvMetadataDocument;
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
 * Provides integration test coverage for the savePvMetadata() support in the
 * com.ospreydcs.dp.client convenience layer, exercising AnnotationClient against a running
 * annotation service.  Server-side behavior is covered separately by PvMetadataIT; these tests
 * cover the client wrapper — request building from the params record, the success payload, and the
 * surfacing of server rejections through ApiResultBase.resultStatus.
 */
public class PvMetadataClientIT extends AnnotationIntegrationTestIntermediate {

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

    // =========================================================================
    // buildSavePvMetadataRequest tests
    // =========================================================================

    /**
     * Verifies that optional fields are omitted from the request when null, and that only pvName is
     * required.
     */
    @Test
    public void testBuildRequestOmitsUnsuppliedOptionalFields() {

        final AnnotationClient.SavePvMetadataParams params = new AnnotationClient.SavePvMetadataParams(
                "TEST:PV:100", null, null, null, null, null);

        final SavePvMetadataRequest request = AnnotationClient.buildSavePvMetadataRequest(params);

        assertEquals("TEST:PV:100", request.getPvName());
        assertTrue(request.getAliasesList().isEmpty());
        assertTrue(request.getTagsList().isEmpty());
        assertTrue(request.getAttributesList().isEmpty());
        assertEquals("", request.getDescription());
        assertEquals("", request.getModifiedBy());
    }

    /**
     * Verifies that every supplied field reaches the request, and that the attribute map is
     * converted to the repeated Attribute field.
     */
    @Test
    public void testBuildRequestPopulatesSuppliedFields() {

        // LinkedHashMap so the converted attribute order is predictable for assertion
        final Map<String, String> attributeMap = new LinkedHashMap<>();
        attributeMap.put("system", "vacuum");
        attributeMap.put("sector", "01");

        final AnnotationClient.SavePvMetadataParams params = new AnnotationClient.SavePvMetadataParams(
                "TEST:PV:101",
                List.of("alias1", "alias2"),
                List.of("TEST", "Unit"),
                attributeMap,
                "a test pv",
                "craigmcc");

        final SavePvMetadataRequest request = AnnotationClient.buildSavePvMetadataRequest(params);

        assertEquals("TEST:PV:101", request.getPvName());
        assertEquals(List.of("alias1", "alias2"), request.getAliasesList());

        // client does not normalize tags; the server lowercases, dedupes and sorts them
        assertEquals(List.of("TEST", "Unit"), request.getTagsList());

        assertEquals(2, request.getAttributesCount());
        assertEquals("system", request.getAttributes(0).getName());
        assertEquals("vacuum", request.getAttributes(0).getValue());
        assertEquals("sector", request.getAttributes(1).getName());
        assertEquals("01", request.getAttributes(1).getValue());

        assertEquals("a test pv", request.getDescription());
        assertEquals("craigmcc", request.getModifiedBy());
    }

    // =========================================================================
    // savePvMetadata success tests
    // =========================================================================

    /**
     * Verifies that a successful save returns the canonical pvName with no error, and that the
     * record reaches the database.
     */
    @Test
    public void testSavePvMetadataSuccess() {

        final Map<String, String> attributeMap = new LinkedHashMap<>();
        attributeMap.put("system", "vacuum");
        attributeMap.put("sector", "01");

        final AnnotationClient.SavePvMetadataParams params = new AnnotationClient.SavePvMetadataParams(
                "TEST:PV:001",
                List.of("alias1", "alias2"),
                List.of("TEST", "Unit", "test"),
                attributeMap,
                "A test PV",
                "craigmcc");

        final SavePvMetadataApiResult result = annotationClient.savePvMetadata(params);

        assertFalse(result.resultStatus.msg, result.resultStatus.isError);
        assertEquals("", result.resultStatus.msg);

        // success payload is the canonical pvName, not an ObjectId
        assertEquals("TEST:PV:001", result.pvName);

        // verify the record reached the database, with tags normalized server-side
        final PvMetadataDocument document = mongoClient.findPvMetadata("TEST:PV:001");
        assertNotNull(document);
        assertEquals("TEST:PV:001", document.getPvName());
        assertEquals("A test PV", document.getDescription());
        assertEquals("craigmcc", document.getModifiedBy());
        assertEquals(List.of("test", "unit"), document.getTags());
    }

    /**
     * Verifies the full-replace upsert semantics through the client: a second save omitting fields
     * that the first save supplied clears them rather than preserving them.
     */
    @Test
    public void testSavePvMetadataFullReplaceUpsert() {

        final Map<String, String> attributeMap = new LinkedHashMap<>();
        attributeMap.put("system", "vacuum");

        final AnnotationClient.SavePvMetadataParams createParams =
                new AnnotationClient.SavePvMetadataParams(
                        "TEST:PV:002",
                        List.of("alias1"),
                        List.of("tag1"),
                        attributeMap,
                        "original description",
                        "craigmcc");

        final SavePvMetadataApiResult createResult = annotationClient.savePvMetadata(createParams);
        assertFalse(createResult.resultStatus.msg, createResult.resultStatus.isError);
        assertEquals("TEST:PV:002", createResult.pvName);

        final PvMetadataDocument createdDocument = mongoClient.findPvMetadata("TEST:PV:002");
        assertNotNull(createdDocument);
        assertEquals("original description", createdDocument.getDescription());
        assertEquals(List.of("tag1"), createdDocument.getTags());
        assertNotNull(createdDocument.getCreatedAt());

        // save again supplying only pvName; the omitted fields are replaced, not preserved
        final AnnotationClient.SavePvMetadataParams updateParams =
                new AnnotationClient.SavePvMetadataParams(
                        "TEST:PV:002", null, null, null, null, null);

        final SavePvMetadataApiResult updateResult = annotationClient.savePvMetadata(updateParams);
        assertFalse(updateResult.resultStatus.msg, updateResult.resultStatus.isError);
        assertEquals("TEST:PV:002", updateResult.pvName);

        final PvMetadataDocument updatedDocument = mongoClient.findPvMetadata("TEST:PV:002");
        assertNotNull(updatedDocument);

        // createdAt is preserved across the upsert, updatedAt is now set
        assertEquals(createdDocument.getCreatedAt(), updatedDocument.getCreatedAt());
        assertNotNull(updatedDocument.getUpdatedAt());

        // the previously supplied values were replaced rather than retained.  PvMetadataDocument
        // leaves unsupplied fields unset, so after a full replace they read back as null.
        assertNull(updatedDocument.getDescription());
        assertNull(updatedDocument.getTags());
        assertNull(updatedDocument.getAliases());
        assertNull(updatedDocument.getModifiedBy());
    }

    // =========================================================================
    // savePvMetadata rejection tests
    //
    // Note: the server's duplicate-attribute-key rejection ("SavePvMetadataRequest.attributes
    // contains duplicate key: <key>") is not reachable through this client layer, because
    // SavePvMetadataParams takes attributes as a Map<String,String>, which cannot hold a duplicate
    // key.  That path stays covered by PvMetadataIT, which builds the repeated Attribute field
    // directly.
    // =========================================================================

    /**
     * Verifies that a blank pvName is surfaced as an error on resultStatus rather than thrown.
     */
    @Test
    public void testSavePvMetadataRejectBlankPvName() {

        final AnnotationClient.SavePvMetadataParams params = new AnnotationClient.SavePvMetadataParams(
                "", null, null, null, null, null);

        final SavePvMetadataApiResult result = annotationClient.savePvMetadata(params);

        assertTrue(result.resultStatus.isError);
        assertTrue(
                result.resultStatus.msg,
                result.resultStatus.msg.contains("SavePvMetadataRequest.pvName must be specified"));
        assertNull(result.pvName);
    }

    /**
     * Verifies that a pvName already registered as another record's alias is surfaced as an error.
     */
    @Test
    public void testSavePvMetadataRejectPvNameIsAliasOfOther() {

        final AnnotationClient.SavePvMetadataParams firstParams =
                new AnnotationClient.SavePvMetadataParams(
                        "TEST:PV:020", List.of("shared-name"), null, null, null, null);
        final SavePvMetadataApiResult firstResult = annotationClient.savePvMetadata(firstParams);
        assertFalse(firstResult.resultStatus.msg, firstResult.resultStatus.isError);

        // a new record whose pvName equals the first record's alias is rejected
        final AnnotationClient.SavePvMetadataParams conflictParams =
                new AnnotationClient.SavePvMetadataParams(
                        "shared-name", null, null, null, null, null);
        final SavePvMetadataApiResult conflictResult = annotationClient.savePvMetadata(conflictParams);

        assertTrue(conflictResult.resultStatus.isError);
        assertTrue(
                conflictResult.resultStatus.msg,
                conflictResult.resultStatus.msg.contains("is already registered as an alias of pvName"));
        assertNull(conflictResult.pvName);
    }

    /**
     * Verifies that an alias already used by another record is surfaced as an error.
     */
    @Test
    public void testSavePvMetadataRejectAliasConflict() {

        final AnnotationClient.SavePvMetadataParams firstParams =
                new AnnotationClient.SavePvMetadataParams(
                        "TEST:PV:010", List.of("shared-alias"), null, null, null, null);
        final SavePvMetadataApiResult firstResult = annotationClient.savePvMetadata(firstParams);
        assertFalse(firstResult.resultStatus.msg, firstResult.resultStatus.isError);

        // a second record trying to use the same alias is rejected
        final AnnotationClient.SavePvMetadataParams conflictParams =
                new AnnotationClient.SavePvMetadataParams(
                        "TEST:PV:011", List.of("shared-alias"), null, null, null, null);
        final SavePvMetadataApiResult conflictResult = annotationClient.savePvMetadata(conflictParams);

        assertTrue(conflictResult.resultStatus.isError);
        assertTrue(
                conflictResult.resultStatus.msg,
                conflictResult.resultStatus.msg.contains("is already used by pvName"));
        assertNull(conflictResult.pvName);
    }
}
