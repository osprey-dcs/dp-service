package com.ospreydcs.dp.service.integration.annotation;

import com.ospreydcs.dp.client.AnnotationClient;
import com.ospreydcs.dp.client.result.ApiResultStatus;
import com.ospreydcs.dp.client.result.GetPvMetadataApiResult;
import com.ospreydcs.dp.client.result.QueryPvMetadataApiResult;
import com.ospreydcs.dp.client.result.SavePvMetadataApiResult;
import com.ospreydcs.dp.grpc.v1.annotation.GetPvMetadataRequest;
import com.ospreydcs.dp.grpc.v1.annotation.QueryPvMetadataRequest;
import com.ospreydcs.dp.grpc.v1.annotation.SavePvMetadataRequest;
import com.ospreydcs.dp.service.common.bson.pvmetadata.PvMetadataDocument;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
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
 *
 * The queryPvMetadata() and getPvMetadata() wrappers added by #243 are covered here too.  Note the
 * naming convention: the two save-path build tests predate the query wrappers and are named
 * testBuildRequest*, so the newer tests prefix by subject (testBuildQueryPvMetadataRequest*,
 * testBuildGetPvMetadataRequest) as ConfigurationClientIT already does.
 */
public class PvMetadataClientIT extends AnnotationIntegrationTestIntermediate {

    /**
     * Mirrors MongoSyncAnnotationClient.DEFAULT_QUERY_LIMIT, which is private to the client.  If the
     * server default changes without this constant following, testQueryPvMetadataUnsetLimitReturns-
     * DefaultPageSize fails loudly rather than silently asserting the wrong bound.
     */
    private static final int DEFAULT_QUERY_LIMIT = 100;

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

    // =========================================================================
    // buildQueryPvMetadataRequest tests
    // =========================================================================

    /**
     * Verifies that a params record with nothing supplied emits NO criteria at all, rather than
     * empty ones.  This matters beyond tidiness: the server rejects an empty TagsCriterion.values
     * and an empty PvNameCriterion, so emitting an empty criterion for an omitted filter would turn
     * an unfiltered query into a rejected request.
     */
    @Test
    public void testBuildQueryPvMetadataRequestOmitsUnsuppliedCriteria() {

        final AnnotationClient.QueryPvMetadataParams params =
                new AnnotationClient.QueryPvMetadataParams(null, null, null, null, 0, null);

        final QueryPvMetadataRequest request = AnnotationClient.buildQueryPvMetadataRequest(params);

        assertEquals(0, request.getCriteriaCount());
        assertEquals(0, request.getLimit());
        assertEquals("", request.getPageToken());
    }

    /**
     * Verifies that empty (as opposed to null) collections are also treated as unsupplied, and in
     * particular that an empty tagsAnyOf does not emit an empty TagsCriterion.
     */
    @Test
    public void testBuildQueryPvMetadataRequestOmitsEmptyCollections() {

        final AnnotationClient.QueryPvMetadataParams params =
                new AnnotationClient.QueryPvMetadataParams(
                        new AnnotationClient.TextMatch(List.of(), List.of(), List.of()),
                        new AnnotationClient.TextMatch(null, null, null),
                        List.of(),
                        List.of(),
                        0,
                        "   ");

        final QueryPvMetadataRequest request = AnnotationClient.buildQueryPvMetadataRequest(params);

        assertEquals(0, request.getCriteriaCount());

        // a blank page token is left unset rather than sent as whitespace
        assertEquals("", request.getPageToken());
    }

    /**
     * Verifies that collections CONTAINING blank entries are treated as unsupplied, which is a
     * distinct case from the empty collections covered above and a materially more dangerous one.
     *
     * A blank prefix or contains value survives protobuf serialization as a zero-length repeated
     * entry, so the criterion passes the server's "at least one of exact/prefix/contains" check.
     * MongoQueryFilterBuilder.nameMatchFilter() then turns it into the regex "^" + Pattern.quote("")
     * (or ".*" + Pattern.quote("") + ".*"), both of which match EVERY value.  A caller binding an
     * unfilled optional UI field would therefore silently retrieve the entire collection rather
     * than applying no filter — a silent wrong answer, not an error, which is the failure mode this
     * repo treats as the most serious.
     */
    @Test
    public void testBuildQueryPvMetadataRequestOmitsBlankEntries() {

        final AnnotationClient.QueryPvMetadataParams params =
                new AnnotationClient.QueryPvMetadataParams(
                        new AnnotationClient.TextMatch(List.of(""), List.of("  "), List.of("\t")),
                        new AnnotationClient.TextMatch(List.of(" "), List.of(""), List.of("   ")),
                        List.of("", "  "),
                        List.of(new AnnotationClient.AttributeCriterion("  ", List.of("v"))),
                        0,
                        null);

        final QueryPvMetadataRequest request = AnnotationClient.buildQueryPvMetadataRequest(params);

        assertEquals(
                "a criterion built entirely from blank entries must not be emitted",
                0,
                request.getCriteriaCount());
    }

    /**
     * Verifies that a blank prefix alone does not produce a match-all query.  Asserted separately
     * from the aggregate test above because this is the single most dangerous input: prior to the
     * blank filter it emitted a PvNameCriterion whose prefix list held one zero-length entry, which
     * passed server validation and matched every record.
     */
    @Test
    public void testBuildQueryPvMetadataRequestBlankPrefixEmitsNoCriterion() {

        final AnnotationClient.QueryPvMetadataParams params =
                new AnnotationClient.QueryPvMetadataParams(
                        new AnnotationClient.TextMatch(null, List.of(""), null),
                        null, null, null, 0, null);

        final QueryPvMetadataRequest request = AnnotationClient.buildQueryPvMetadataRequest(params);

        assertEquals(
                "a blank prefix must emit no criterion rather than a match-all regex",
                0,
                request.getCriteriaCount());
    }

    /**
     * Verifies that blank entries are dropped individually while real values in the same list are
     * preserved — the filter removes blanks, it does not discard the whole criterion when one
     * entry happens to be blank.
     */
    @Test
    public void testBuildQueryPvMetadataRequestDropsBlanksKeepsRealValues() {

        final List<String> prefixWithBlanks = new ArrayList<>();
        prefixWithBlanks.add("REAL:PV");
        prefixWithBlanks.add("");
        prefixWithBlanks.add("   ");

        final AnnotationClient.QueryPvMetadataParams params =
                new AnnotationClient.QueryPvMetadataParams(
                        new AnnotationClient.TextMatch(null, prefixWithBlanks, null),
                        null,
                        List.of("realtag", "  "),
                        null,
                        0,
                        null);

        final QueryPvMetadataRequest request = AnnotationClient.buildQueryPvMetadataRequest(params);

        assertEquals(2, request.getCriteriaCount());

        final QueryPvMetadataRequest.QueryPvMetadataCriterion.PvNameCriterion pvNameCriterion =
                request.getCriteria(0).getPvNameCriterion();
        assertEquals(1, pvNameCriterion.getPrefixCount());
        assertEquals("REAL:PV", pvNameCriterion.getPrefix(0));

        assertEquals(
                List.of("realtag"),
                request.getCriteria(1).getTagsCriterion().getValuesList());
    }

    /**
     * Verifies that a blank attribute key emits no criterion.  The server validates
     * AttributesCriterion.key with isBlank() (QueryPvMetadataJob), so forwarding a whitespace key
     * produces an avoidable REJECT rather than the omitted filter the caller intended.
     */
    @Test
    public void testBuildQueryPvMetadataRequestOmitsBlankAttributeKey() {

        final AnnotationClient.QueryPvMetadataParams params =
                new AnnotationClient.QueryPvMetadataParams(
                        null,
                        null,
                        null,
                        List.of(
                                new AnnotationClient.AttributeCriterion("  ", List.of("v")),
                                new AnnotationClient.AttributeCriterion("realkey", List.of("v"))),
                        0,
                        null);

        final QueryPvMetadataRequest request = AnnotationClient.buildQueryPvMetadataRequest(params);

        assertEquals(1, request.getCriteriaCount());
        assertEquals("realkey", request.getCriteria(0).getAttributesCriterion().getKey());
    }

    /**
     * Verifies that a null entry inside a criterion list is dropped rather than thrown.  Protobuf's
     * addAll rejects a null element with NullPointerException, so an unfiltered null would surface
     * to the caller as an exception from the builder instead of a result.
     */
    @Test
    public void testBuildQueryPvMetadataRequestDropsNullEntries() {

        final List<String> exactWithNull = new ArrayList<>();
        exactWithNull.add(null);
        exactWithNull.add("REAL:PV");

        final AnnotationClient.QueryPvMetadataParams params =
                new AnnotationClient.QueryPvMetadataParams(
                        new AnnotationClient.TextMatch(exactWithNull, null, null),
                        null, null, null, 0, null);

        final QueryPvMetadataRequest request = AnnotationClient.buildQueryPvMetadataRequest(params);

        assertEquals(1, request.getCriteriaCount());
        assertEquals(
                List.of("REAL:PV"),
                request.getCriteria(0).getPvNameCriterion().getExactList());
    }

    /**
     * Verifies end-to-end that a blank-only query emits no criterion, and so is treated as
     * "no filters requested" rather than as a match-all regex.
     *
     * <p><b>This test asserted a rejection before #245</b>, which is no longer the right assertion.
     * It relied on a second-order mechanism: nonBlank() dropped the blank entry, the criterion was
     * omitted, and the server rejected the resulting empty criteria list.  #245 makes an empty
     * criteria list match-all, so that rejection is gone and the request now legitimately succeeds.
     *
     * <p>The #243 invariant that survives is narrower and is the one that always mattered: a blank
     * prefix must never reach the server as {@code "^" + Pattern.quote("")}, a regex matching every
     * value.  What distinguishes the two is not the result set — both return every record — but
     * whether the server was asked to filter at all.  So this asserts on the built request, where
     * the difference is visible, and additionally pins that the response is bounded by the default
     * page size rather than being the unbounded read a match-all regex used to produce.
     *
     * <p>The primary guards for the blank-drop behavior are the builder-level tests above
     * (testBuildQueryPvMetadataRequestOmitsBlankEntries and
     * testBuildQueryPvMetadataRequestBlankPrefixEmitsNoCriterion), which assert
     * getCriteriaCount() == 0 directly and are unaffected by #245.
     */
    @Test
    public void testQueryPvMetadataBlankCriteriaEmitsNoCriterion() {

        // seed two records so a match-all REGEX regression would still be visible below
        savePvsWithTag("blanktest", "BLANK:TEST:PV:1", "BLANK:TEST:PV:2");

        final AnnotationClient.QueryPvMetadataParams params =
                new AnnotationClient.QueryPvMetadataParams(
                        new AnnotationClient.TextMatch(null, List.of(""), null),
                        null, null, null, 0, null);

        // the guarantee: no criterion is emitted, so no "^" + Pattern.quote("") regex is built
        assertEquals(
                "a blank prefix must emit no criterion rather than a match-all regex",
                0,
                AnnotationClient.buildQueryPvMetadataRequest(params).getCriteriaCount());

        // and end-to-end it is an ordinary unfiltered browse, bounded by the default page size
        final QueryPvMetadataApiResult result = annotationClient.queryPvMetadata(params);

        assertFalse(result.resultStatus.msg, result.isError());
        assertFalse(result.isReject());
        assertNotNull(result.pvMetadata);
        assertEquals(2, result.pvMetadata.size());
    }

    /**
     * Verifies that a TextMatch populates exact, prefix and contains independently, and that all
     * three can be combined within one criterion.
     */
    @Test
    public void testBuildQueryPvMetadataRequestTextMatchSubLists() {

        // exact only
        final QueryPvMetadataRequest exactOnly = AnnotationClient.buildQueryPvMetadataRequest(
                new AnnotationClient.QueryPvMetadataParams(
                        new AnnotationClient.TextMatch(List.of("TEST:PV:1"), null, null),
                        null, null, null, 0, null));
        assertEquals(1, exactOnly.getCriteriaCount());
        assertEquals(
                List.of("TEST:PV:1"), exactOnly.getCriteria(0).getPvNameCriterion().getExactList());
        assertTrue(exactOnly.getCriteria(0).getPvNameCriterion().getPrefixList().isEmpty());
        assertTrue(exactOnly.getCriteria(0).getPvNameCriterion().getContainsList().isEmpty());

        // prefix only
        final QueryPvMetadataRequest prefixOnly = AnnotationClient.buildQueryPvMetadataRequest(
                new AnnotationClient.QueryPvMetadataParams(
                        new AnnotationClient.TextMatch(null, List.of("TEST:"), null),
                        null, null, null, 0, null));
        assertEquals(1, prefixOnly.getCriteriaCount());
        assertEquals(List.of("TEST:"), prefixOnly.getCriteria(0).getPvNameCriterion().getPrefixList());
        assertTrue(prefixOnly.getCriteria(0).getPvNameCriterion().getExactList().isEmpty());

        // contains only
        final QueryPvMetadataRequest containsOnly = AnnotationClient.buildQueryPvMetadataRequest(
                new AnnotationClient.QueryPvMetadataParams(
                        new AnnotationClient.TextMatch(null, null, List.of("PV")),
                        null, null, null, 0, null));
        assertEquals(1, containsOnly.getCriteriaCount());
        assertEquals(List.of("PV"), containsOnly.getCriteria(0).getPvNameCriterion().getContainsList());

        // all three combined in a single criterion
        final QueryPvMetadataRequest combined = AnnotationClient.buildQueryPvMetadataRequest(
                new AnnotationClient.QueryPvMetadataParams(
                        new AnnotationClient.TextMatch(
                                List.of("TEST:PV:1"), List.of("TEST:"), List.of("PV")),
                        null, null, null, 0, null));
        assertEquals(1, combined.getCriteriaCount());
        final QueryPvMetadataRequest.QueryPvMetadataCriterion.PvNameCriterion pvNameCriterion =
                combined.getCriteria(0).getPvNameCriterion();
        assertEquals(List.of("TEST:PV:1"), pvNameCriterion.getExactList());
        assertEquals(List.of("TEST:"), pvNameCriterion.getPrefixList());
        assertEquals(List.of("PV"), pvNameCriterion.getContainsList());
    }

    /**
     * Verifies that every supplied field maps to the right criterion type with the right values,
     * and that each params field contributes exactly one criterion.
     */
    @Test
    public void testBuildQueryPvMetadataRequestPopulatesSuppliedFields() {

        final AnnotationClient.QueryPvMetadataParams params =
                new AnnotationClient.QueryPvMetadataParams(
                        new AnnotationClient.TextMatch(List.of("TEST:PV:1"), null, null),
                        new AnnotationClient.TextMatch(null, List.of("alias-"), null),
                        List.of("tag1", "tag2"),
                        List.of(new AnnotationClient.AttributeCriterion("system", List.of("vacuum"))),
                        25,
                        "token-abc");

        final QueryPvMetadataRequest request = AnnotationClient.buildQueryPvMetadataRequest(params);

        // one criterion per supplied params field, in declaration order
        assertEquals(4, request.getCriteriaCount());

        assertTrue(request.getCriteria(0).hasPvNameCriterion());
        assertEquals(List.of("TEST:PV:1"), request.getCriteria(0).getPvNameCriterion().getExactList());

        assertTrue(request.getCriteria(1).hasAliasesCriterion());
        assertEquals(List.of("alias-"), request.getCriteria(1).getAliasesCriterion().getPrefixList());

        assertTrue(request.getCriteria(2).hasTagsCriterion());
        assertEquals(List.of("tag1", "tag2"), request.getCriteria(2).getTagsCriterion().getValuesList());

        assertTrue(request.getCriteria(3).hasAttributesCriterion());
        assertEquals("system", request.getCriteria(3).getAttributesCriterion().getKey());
        assertEquals(
                List.of("vacuum"), request.getCriteria(3).getAttributesCriterion().getValuesList());

        assertEquals(25, request.getLimit());
        assertEquals("token-abc", request.getPageToken());
    }

    /**
     * Verifies that an AttributeCriterion with no values produces a key-only existence criterion
     * rather than being dropped, and that a list of them produces one criterion each.
     */
    @Test
    public void testBuildQueryPvMetadataRequestAttributeKeyOnly() {

        final AnnotationClient.QueryPvMetadataParams params =
                new AnnotationClient.QueryPvMetadataParams(
                        null,
                        null,
                        null,
                        List.of(
                                new AnnotationClient.AttributeCriterion("system", null),
                                new AnnotationClient.AttributeCriterion("sector", List.of())),
                        0,
                        null);

        final QueryPvMetadataRequest request = AnnotationClient.buildQueryPvMetadataRequest(params);

        assertEquals(2, request.getCriteriaCount());

        assertEquals("system", request.getCriteria(0).getAttributesCriterion().getKey());
        assertTrue(request.getCriteria(0).getAttributesCriterion().getValuesList().isEmpty());

        assertEquals("sector", request.getCriteria(1).getAttributesCriterion().getKey());
        assertTrue(request.getCriteria(1).getAttributesCriterion().getValuesList().isEmpty());
    }

    /**
     * Verifies that a non-positive limit is left unset, so the server applies its own default
     * rather than receiving an explicit zero.
     */
    @Test
    public void testBuildQueryPvMetadataRequestNonPositiveLimitUnset() {

        final QueryPvMetadataRequest request = AnnotationClient.buildQueryPvMetadataRequest(
                new AnnotationClient.QueryPvMetadataParams(
                        new AnnotationClient.TextMatch(List.of("TEST:PV:1"), null, null),
                        null, null, null, -5, null));

        assertEquals(0, request.getLimit());
    }

    // =========================================================================
    // buildGetPvMetadataRequest tests
    // =========================================================================

    /**
     * Verifies that the get request carries the supplied name or alias, and that a null argument
     * leaves the field unset rather than throwing.
     */
    @Test
    public void testBuildGetPvMetadataRequest() {

        assertEquals(
                "TEST:PV:1",
                AnnotationClient.buildGetPvMetadataRequest("TEST:PV:1").getPvNameOrAlias());

        assertEquals("", AnnotationClient.buildGetPvMetadataRequest(null).getPvNameOrAlias());
    }

    // =========================================================================
    // queryPvMetadata tests
    // =========================================================================

    /*
     * Saves the given PV names with the supplied tag, so a query has something to match.
     */
    private void savePvsWithTag(String tag, String... pvNames) {
        for (String pvName : pvNames) {
            final SavePvMetadataApiResult result = annotationClient.savePvMetadata(
                    new AnnotationClient.SavePvMetadataParams(
                            pvName, null, List.of(tag), null, null, "craigmcc"));
            assertFalse(result.resultStatus.msg, result.resultStatus.isError);
        }
    }

    /**
     * Verifies that a query matching records returns them through the wrapper, with no error and
     * ApiResultStatus.NONE.
     */
    @Test
    public void testQueryPvMetadataSuccess() {

        savePvsWithTag("querytest", "TEST:QUERY:001", "TEST:QUERY:002");

        final QueryPvMetadataApiResult result = annotationClient.queryPvMetadata(
                new AnnotationClient.QueryPvMetadataParams(
                        new AnnotationClient.TextMatch(null, List.of("TEST:QUERY:"), null),
                        null, null, null, 100, null));

        assertFalse(result.resultStatus.msg, result.resultStatus.isError);
        assertEquals(ApiResultStatus.NONE, result.apiResultStatus);
        assertNotNull(result.pvMetadata);
        assertEquals(2, result.pvMetadata.size());

        final List<String> pvNames =
                result.pvMetadata.stream().map(pv -> pv.getPvName()).sorted().toList();
        assertEquals(List.of("TEST:QUERY:001", "TEST:QUERY:002"), pvNames);
    }

    /**
     * Verifies that a query matching nothing is a normal SUCCESS with an empty list, not a
     * rejection.  This is the distinction the wrapper javadoc documents: an empty collection is a
     * normal answer, unlike a missing singleton from getPvMetadata().
     */
    @Test
    public void testQueryPvMetadataEmptyResultIsSuccess() {

        final QueryPvMetadataApiResult result = annotationClient.queryPvMetadata(
                new AnnotationClient.QueryPvMetadataParams(
                        new AnnotationClient.TextMatch(List.of("NO:SUCH:PV"), null, null),
                        null, null, null, 100, null));

        assertFalse(result.resultStatus.msg, result.resultStatus.isError);
        assertEquals(ApiResultStatus.NONE, result.apiResultStatus);
        assertFalse(result.isReject());
        assertNotNull(result.pvMetadata);
        assertTrue(result.pvMetadata.isEmpty());
        assertEquals("", result.nextPageToken);
    }

    /**
     * Verifies a full paging round-trip through the wrapper: the first page carries a non-empty
     * nextPageToken, feeding it back returns the remainder, and the FINAL page's token is blank.
     * The blank final token is asserted deliberately — it is what tells a caller to stop paging,
     * and ConfigurationIT.testQueryConfigurationsPagination omits that check.
     */
    @Test
    public void testQueryPvMetadataPagingRoundTrip() {

        savePvsWithTag("pagetest", "TEST:PAGE:001", "TEST:PAGE:002", "TEST:PAGE:003");

        // page 1 of 2
        final QueryPvMetadataApiResult firstPage = annotationClient.queryPvMetadata(
                new AnnotationClient.QueryPvMetadataParams(
                        new AnnotationClient.TextMatch(null, List.of("TEST:PAGE:"), null),
                        null, null, null, 2, null));

        assertFalse(firstPage.resultStatus.msg, firstPage.resultStatus.isError);
        assertEquals(2, firstPage.pvMetadata.size());
        assertFalse(
                "expected a non-empty nextPageToken on a truncated page",
                firstPage.nextPageToken.isEmpty());

        // page 2 of 2, fed the prior token; this is the last page, so its token is blank
        final QueryPvMetadataApiResult secondPage = annotationClient.queryPvMetadata(
                new AnnotationClient.QueryPvMetadataParams(
                        new AnnotationClient.TextMatch(null, List.of("TEST:PAGE:"), null),
                        null, null, null, 2, firstPage.nextPageToken));

        assertFalse(secondPage.resultStatus.msg, secondPage.resultStatus.isError);
        assertEquals(1, secondPage.pvMetadata.size());
        assertEquals(
                "the final page must carry a blank nextPageToken", "", secondPage.nextPageToken);

        // the two pages together cover every matching record exactly once
        final List<String> allNames = new java.util.ArrayList<String>();
        firstPage.pvMetadata.forEach(pv -> allNames.add(pv.getPvName()));
        secondPage.pvMetadata.forEach(pv -> allNames.add(pv.getPvName()));
        allNames.sort(null);
        assertEquals(List.of("TEST:PAGE:001", "TEST:PAGE:002", "TEST:PAGE:003"), allNames);
    }

    /**
     * An empty criteria list is match-all, not a rejection (#245).  This is the browse-all entry
     * point the dp-desktop-app PV Metadata explore view opens with: no filters, see everything.
     *
     * <p>Replaces testQueryPvMetadataRejectsEmptyCriteria, which pinned the pre-#245 rejection.
     */
    @Test
    public void testQueryPvMetadataEmptyCriteriaMatchesAll() {

        savePvsWithTag("matchall", "TEST:ALL:001", "TEST:ALL:002", "TEST:ALL:003");

        final QueryPvMetadataApiResult result = annotationClient.queryPvMetadata(
                new AnnotationClient.QueryPvMetadataParams(null, null, null, null, 100, null));

        assertFalse(result.resultStatus.msg, result.resultStatus.isError);
        assertEquals(ApiResultStatus.NONE, result.apiResultStatus);
        assertFalse(result.isReject());
        assertNotNull(result.pvMetadata);
        assertEquals(3, result.pvMetadata.size());

        final List<String> pvNames =
                result.pvMetadata.stream().map(pv -> pv.getPvName()).sorted().toList();
        assertEquals(List.of("TEST:ALL:001", "TEST:ALL:002", "TEST:ALL:003"), pvNames);
    }

    /**
     * Match-all plus paging enumerates every record exactly once, with a blank token on the final
     * page.  Pages with an explicit limit of 2 over 5 records: 2, 2, 1.
     */
    @Test
    public void testQueryPvMetadataEmptyCriteriaPagesThroughAll() {

        savePvsWithTag(
                "matchallpage",
                "TEST:MAP:001", "TEST:MAP:002", "TEST:MAP:003", "TEST:MAP:004", "TEST:MAP:005");

        final List<String> seen = new java.util.ArrayList<String>();
        String pageToken = null;
        int pageCount = 0;

        while (true) {
            final QueryPvMetadataApiResult page = annotationClient.queryPvMetadata(
                    new AnnotationClient.QueryPvMetadataParams(null, null, null, null, 2, pageToken));

            assertFalse(page.resultStatus.msg, page.resultStatus.isError);
            assertNotNull(page.pvMetadata);
            page.pvMetadata.forEach(pv -> seen.add(pv.getPvName()));

            pageCount++;
            assertTrue("paging did not terminate", pageCount <= 10);

            if (page.nextPageToken.isEmpty()) {
                assertEquals("the final page should hold the remainder", 1, page.pvMetadata.size());
                break;
            }
            assertEquals("a non-final page should be full", 2, page.pvMetadata.size());
            pageToken = page.nextPageToken;
        }

        assertEquals(3, pageCount);
        seen.sort(null);
        assertEquals(
                List.of("TEST:MAP:001", "TEST:MAP:002", "TEST:MAP:003", "TEST:MAP:004", "TEST:MAP:005"),
                seen);
    }

    /**
     * An unset limit returns the default page size with a token, NOT the entire collection.
     *
     * <p>This is the regression guard for the hazard #245 exists to avoid.  Before #245,
     * queryPvMetadata returned every match with an always-blank nextPageToken, so a match-all
     * request at facility scale was an unbounded read the caller could not even detect had been
     * truncated (it had not been).  The default is unconditional -- see the criteria-bearing
     * companion test below -- so this asserts both halves of D1.
     */
    @Test
    public void testQueryPvMetadataUnsetLimitReturnsDefaultPageSize() {

        final String[] pvNames = new String[DEFAULT_QUERY_LIMIT + 1];
        for (int i = 0; i < pvNames.length; i++) {
            pvNames[i] = String.format("TEST:LIMIT:%03d", i);
        }
        savePvsWithTag("limitdefault", pvNames);

        // match-all with an unset limit
        final QueryPvMetadataApiResult matchAll = annotationClient.queryPvMetadata(
                new AnnotationClient.QueryPvMetadataParams(null, null, null, null, 0, null));

        assertFalse(matchAll.resultStatus.msg, matchAll.resultStatus.isError);
        assertNotNull(matchAll.pvMetadata);
        assertEquals(
                "an unset limit must return the default page size, not every record",
                DEFAULT_QUERY_LIMIT,
                matchAll.pvMetadata.size());
        assertFalse(
                "a truncated page must carry a token so the caller can detect the truncation",
                matchAll.nextPageToken.isEmpty());

        // the same default applies to a criteria-bearing query: the page size does not depend on
        // whether the caller supplied criteria (#245 plan D1)
        final QueryPvMetadataApiResult withCriteria = annotationClient.queryPvMetadata(
                new AnnotationClient.QueryPvMetadataParams(
                        new AnnotationClient.TextMatch(null, List.of("TEST:LIMIT:"), null),
                        null, null, null, 0, null));

        assertFalse(withCriteria.resultStatus.msg, withCriteria.resultStatus.isError);
        assertNotNull(withCriteria.pvMetadata);
        assertEquals(DEFAULT_QUERY_LIMIT, withCriteria.pvMetadata.size());
        assertFalse(withCriteria.nextPageToken.isEmpty());
    }

    // =========================================================================
    // getPvMetadata tests
    // =========================================================================

    /**
     * Verifies that a get by canonical PV name returns the record.
     */
    @Test
    public void testGetPvMetadataSuccessByPvName() {

        final Map<String, String> attributeMap = new LinkedHashMap<>();
        attributeMap.put("system", "vacuum");

        final SavePvMetadataApiResult saveResult = annotationClient.savePvMetadata(
                new AnnotationClient.SavePvMetadataParams(
                        "TEST:GET:001",
                        List.of("get-alias-001"),
                        List.of("TEST"),
                        attributeMap,
                        "a retrievable pv",
                        "craigmcc"));
        assertFalse(saveResult.resultStatus.msg, saveResult.resultStatus.isError);

        final GetPvMetadataApiResult result = annotationClient.getPvMetadata("TEST:GET:001");

        assertFalse(result.resultStatus.msg, result.resultStatus.isError);
        assertEquals(ApiResultStatus.NONE, result.apiResultStatus);
        assertNotNull(result.pvMetadata);
        assertEquals("TEST:GET:001", result.pvMetadata.getPvName());
        assertEquals("a retrievable pv", result.pvMetadata.getDescription());
        assertEquals("craigmcc", result.pvMetadata.getModifiedBy());
        assertEquals(List.of("get-alias-001"), result.pvMetadata.getAliasesList());

        // tags are normalized server-side
        assertEquals(List.of("test"), result.pvMetadata.getTagsList());
    }

    /**
     * Verifies that a get by alias resolves to the same record as a get by canonical name — the
     * reason the argument is named pvNameOrAlias.
     */
    @Test
    public void testGetPvMetadataSuccessByAlias() {

        final SavePvMetadataApiResult saveResult = annotationClient.savePvMetadata(
                new AnnotationClient.SavePvMetadataParams(
                        "TEST:GET:002", List.of("get-alias-002"), null, null, null, "craigmcc"));
        assertFalse(saveResult.resultStatus.msg, saveResult.resultStatus.isError);

        final GetPvMetadataApiResult result = annotationClient.getPvMetadata("get-alias-002");

        assertFalse(result.resultStatus.msg, result.resultStatus.isError);
        assertNotNull(result.pvMetadata);
        assertEquals("TEST:GET:002", result.pvMetadata.getPvName());
    }

    /**
     * Verifies that a missing record is a REJECTION, not an empty success, and that the rejection
     * is categorized on apiResultStatus rather than only in the message.
     *
     * Asserting apiResultStatus as well as isReject() is deliberate: the #235 lesson is that a
     * method's naming and its wire status can diverge silently, so a test that checks only
     * isError() would pass even if the server started reporting not-found as an ERROR.
     */
    @Test
    public void testGetPvMetadataRejectNotFound() {

        final GetPvMetadataApiResult result = annotationClient.getPvMetadata("NO:SUCH:PV");

        assertTrue(result.resultStatus.isError);
        assertEquals(ApiResultStatus.REJECT, result.apiResultStatus);
        assertTrue(result.isReject());
        assertTrue(
                result.resultStatus.msg,
                result.resultStatus.msg.contains("no PvMetadata record found for: NO:SUCH:PV"));
        assertNull(result.pvMetadata);
    }

    /**
     * Verifies that a request the server refuses to validate also surfaces as REJECT, confirming
     * that a caller cannot read REJECT as proof the record is absent without validating first —
     * the caveat the wrapper javadoc carries.
     */
    @Test
    public void testGetPvMetadataRejectBlankName() {

        final GetPvMetadataApiResult result = annotationClient.getPvMetadata("");

        assertTrue(result.resultStatus.isError);
        assertEquals(ApiResultStatus.REJECT, result.apiResultStatus);
        assertTrue(result.isReject());
        assertTrue(
                result.resultStatus.msg,
                result.resultStatus.msg.contains(
                        "GetPvMetadataRequest.pvNameOrAlias must be specified"));
        assertNull(result.pvMetadata);
    }
}
