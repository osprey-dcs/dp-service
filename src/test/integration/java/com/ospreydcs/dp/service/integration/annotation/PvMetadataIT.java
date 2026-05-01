package com.ospreydcs.dp.service.integration.annotation;

import com.ospreydcs.dp.grpc.v1.annotation.QueryPvMetadataRequest;
import com.ospreydcs.dp.grpc.v1.common.Attribute;
import com.ospreydcs.dp.grpc.v1.common.PvMetadata;
import com.ospreydcs.dp.service.annotation.AnnotationTestBase;
import com.ospreydcs.dp.service.common.bson.pvmetadata.PvMetadataDocument;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.List;

import static org.junit.Assert.*;

public class PvMetadataIT extends AnnotationIntegrationTestIntermediate {

    @Before
    public void setUp() throws Exception {
        super.setUp();
    }

    @After
    public void tearDown() {
        super.tearDown();
    }

    // =========================================================================
    // savePvMetadata tests
    // =========================================================================

    @Test
    public void testSavePvMetadataRejectBlankPvName() {
        final AnnotationTestBase.SavePvMetadataParams params = new AnnotationTestBase.SavePvMetadataParams(
                "", null, null, null, null, null);
        annotationServiceWrapper.sendAndVerifySavePvMetadata(
                params, true, "SavePvMetadataRequest.pvName must be specified");
    }

    @Test
    public void testSavePvMetadataRejectDuplicateAttributeKeys() {
        final List<Attribute> attributes = List.of(
                Attribute.newBuilder().setName("key1").setValue("v1").build(),
                Attribute.newBuilder().setName("key1").setValue("v2").build()
        );
        final AnnotationTestBase.SavePvMetadataParams params = new AnnotationTestBase.SavePvMetadataParams(
                "TEST:PV:001", null, null, attributes, null, null);
        annotationServiceWrapper.sendAndVerifySavePvMetadata(
                params, true, "duplicate key");
    }

    @Test
    public void testSavePvMetadataCreateAndUpdate() {
        // create a new PvMetadata record
        final List<String> aliases = List.of("alias1", "alias2");
        final List<String> tags = List.of("TEST", "Unit", "test"); // should be normalized to lowercase unique sorted
        final List<Attribute> attributes = List.of(
                Attribute.newBuilder().setName("system").setValue("vacuum").build(),
                Attribute.newBuilder().setName("sector").setValue("01").build()
        );
        final AnnotationTestBase.SavePvMetadataParams createParams = new AnnotationTestBase.SavePvMetadataParams(
                "TEST:PV:001", aliases, tags, attributes, "A test PV", "craigmcc");

        final String pvName = annotationServiceWrapper.sendAndVerifySavePvMetadata(
                createParams, false, null);
        assertNotNull(pvName);
        assertEquals("TEST:PV:001", pvName);

        // verify document in database: createdAt set, updatedAt null, tags normalized
        final PvMetadataDocument doc = mongoClient.findPvMetadata("TEST:PV:001");
        assertNotNull(doc);
        assertNotNull(doc.getCreatedAt());
        assertNull(doc.getUpdatedAt());
        assertEquals("TEST:PV:001", doc.getPvName());
        // tags should be normalized to lowercase unique sorted list
        final List<String> normalizedTags = doc.getTags();
        assertNotNull(normalizedTags);
        assertEquals(List.of("test", "unit"), normalizedTags);

        // update same PvMetadata record with different description
        final AnnotationTestBase.SavePvMetadataParams updateParams = new AnnotationTestBase.SavePvMetadataParams(
                "TEST:PV:001", aliases, tags, attributes, "Updated description", "allenck");

        final String updatedPvName = annotationServiceWrapper.sendAndVerifySavePvMetadata(
                updateParams, false, null);
        assertEquals("TEST:PV:001", updatedPvName);

        // verify update: createdAt preserved, updatedAt set
        final PvMetadataDocument updatedDoc = mongoClient.findPvMetadata("TEST:PV:001");
        assertNotNull(updatedDoc);
        assertNotNull(updatedDoc.getCreatedAt());
        assertNotNull(updatedDoc.getUpdatedAt());
        assertEquals("Updated description", updatedDoc.getDescription());
        assertEquals("allenck", updatedDoc.getModifiedBy());
        assertEquals(doc.getCreatedAt(), updatedDoc.getCreatedAt());
    }

    @Test
    public void testSavePvMetadataRejectPvNameIsAliasOfOther() {
        // save first record with alias "shared-name"
        final AnnotationTestBase.SavePvMetadataParams params1 = new AnnotationTestBase.SavePvMetadataParams(
                "TEST:PV:020", List.of("shared-name"), null, null, null, null);
        annotationServiceWrapper.sendAndVerifySavePvMetadata(params1, false, null);

        // attempt to save a new record whose pvName equals the alias of the first record — should be rejected
        final AnnotationTestBase.SavePvMetadataParams params2 = new AnnotationTestBase.SavePvMetadataParams(
                "shared-name", null, null, null, null, null);
        annotationServiceWrapper.sendAndVerifySavePvMetadata(
                params2, true, "is already registered as an alias of pvName");
    }

    @Test
    public void testSavePvMetadataRejectAliasConflict() {
        // save first record with alias "shared-alias"
        final AnnotationTestBase.SavePvMetadataParams params1 = new AnnotationTestBase.SavePvMetadataParams(
                "TEST:PV:010", List.of("shared-alias"), null, null, null, null);
        annotationServiceWrapper.sendAndVerifySavePvMetadata(params1, false, null);

        // save second record trying to use the same alias — should be rejected
        final AnnotationTestBase.SavePvMetadataParams params2 = new AnnotationTestBase.SavePvMetadataParams(
                "TEST:PV:011", List.of("shared-alias"), null, null, null, null);
        annotationServiceWrapper.sendAndVerifySavePvMetadata(
                params2, true, "is already used by pvName");
    }

    // =========================================================================
    // queryPvMetadata tests
    // =========================================================================

    @Test
    public void testQueryPvMetadataRejectEmptyCriteria() {
        annotationServiceWrapper.sendAndVerifyQueryPvMetadata(
                List.of(), 0, null, true, "criteria list must not be empty", 0);
    }

    @Test
    public void testQueryPvMetadataRejectBlankAttributeKey() {
        final QueryPvMetadataRequest.QueryPvMetadataCriterion.AttributesCriterion attrCriterion =
                QueryPvMetadataRequest.QueryPvMetadataCriterion.AttributesCriterion.newBuilder()
                        .setKey("")
                        .build();
        final QueryPvMetadataRequest.QueryPvMetadataCriterion criterion =
                QueryPvMetadataRequest.QueryPvMetadataCriterion.newBuilder()
                        .setAttributesCriterion(attrCriterion)
                        .build();
        annotationServiceWrapper.sendAndVerifyQueryPvMetadata(
                List.of(criterion), 0, null, true, "AttributesCriterion key must be specified", 0);
    }

    private void createQueryTestData() {
        // Create 4 PvMetadata records for query tests
        annotationServiceWrapper.sendAndVerifySavePvMetadata(
                new AnnotationTestBase.SavePvMetadataParams(
                        "SR:C01-MG:G02{GCC:1}Prs-I",
                        List.of("gcc01-alias"),
                        List.of("vacuum", "gauges"),
                        List.of(Attribute.newBuilder().setName("sector").setValue("01").build(),
                                Attribute.newBuilder().setName("type").setValue("gauge").build()),
                        "Sector 01 gauge 01",
                        "craigmcc"),
                false, null);

        annotationServiceWrapper.sendAndVerifySavePvMetadata(
                new AnnotationTestBase.SavePvMetadataParams(
                        "SR:C02-MG:G02{GCC:1}Prs-I",
                        List.of("gcc02-alias"),
                        List.of("vacuum", "gauges"),
                        List.of(Attribute.newBuilder().setName("sector").setValue("02").build(),
                                Attribute.newBuilder().setName("type").setValue("gauge").build()),
                        "Sector 02 gauge 01",
                        "craigmcc"),
                false, null);

        annotationServiceWrapper.sendAndVerifySavePvMetadata(
                new AnnotationTestBase.SavePvMetadataParams(
                        "SR:C01-MG:G02{BPM:1}Pos-X",
                        List.of("bpm01-alias"),
                        List.of("bpm", "position"),
                        List.of(Attribute.newBuilder().setName("sector").setValue("01").build(),
                                Attribute.newBuilder().setName("type").setValue("bpm").build()),
                        "Sector 01 BPM 01 X position",
                        "allenck"),
                false, null);

        annotationServiceWrapper.sendAndVerifySavePvMetadata(
                new AnnotationTestBase.SavePvMetadataParams(
                        "SR:C03-MG:G02{BPM:2}Pos-Y",
                        List.of("bpm02-alias"),
                        List.of("bpm", "position"),
                        List.of(Attribute.newBuilder().setName("sector").setValue("03").build(),
                                Attribute.newBuilder().setName("type").setValue("bpm").build()),
                        "Sector 03 BPM 02 Y position",
                        "allenck"),
                false, null);
    }

    @Test
    public void testQueryPvMetadataEmptyResult() {
        createQueryTestData();

        final QueryPvMetadataRequest.QueryPvMetadataCriterion.PvNameCriterion pvNameCriterion =
                QueryPvMetadataRequest.QueryPvMetadataCriterion.PvNameCriterion.newBuilder()
                        .addExact("NONEXISTENT:PV:999")
                        .build();
        final QueryPvMetadataRequest.QueryPvMetadataCriterion criterion =
                QueryPvMetadataRequest.QueryPvMetadataCriterion.newBuilder()
                        .setPvNameCriterion(pvNameCriterion)
                        .build();

        final List<PvMetadata> results = annotationServiceWrapper.sendAndVerifyQueryPvMetadata(
                List.of(criterion), 0, null, false, null, 0);
        assertTrue(results.isEmpty());
    }

    @Test
    public void testQueryPvMetadataByPvNameExact() {
        createQueryTestData();

        final QueryPvMetadataRequest.QueryPvMetadataCriterion.PvNameCriterion pvNameCriterion =
                QueryPvMetadataRequest.QueryPvMetadataCriterion.PvNameCriterion.newBuilder()
                        .addExact("SR:C01-MG:G02{GCC:1}Prs-I")
                        .build();
        final QueryPvMetadataRequest.QueryPvMetadataCriterion criterion =
                QueryPvMetadataRequest.QueryPvMetadataCriterion.newBuilder()
                        .setPvNameCriterion(pvNameCriterion)
                        .build();

        final List<PvMetadata> results = annotationServiceWrapper.sendAndVerifyQueryPvMetadata(
                List.of(criterion), 0, null, false, null, 1);
        assertEquals("SR:C01-MG:G02{GCC:1}Prs-I", results.get(0).getPvName());
    }

    @Test
    public void testQueryPvMetadataByPvNamePrefix() {
        createQueryTestData();

        final QueryPvMetadataRequest.QueryPvMetadataCriterion.PvNameCriterion pvNameCriterion =
                QueryPvMetadataRequest.QueryPvMetadataCriterion.PvNameCriterion.newBuilder()
                        .addPrefix("SR:C01")
                        .build();
        final QueryPvMetadataRequest.QueryPvMetadataCriterion criterion =
                QueryPvMetadataRequest.QueryPvMetadataCriterion.newBuilder()
                        .setPvNameCriterion(pvNameCriterion)
                        .build();

        // two records start with "SR:C01"
        final List<PvMetadata> results = annotationServiceWrapper.sendAndVerifyQueryPvMetadata(
                List.of(criterion), 0, null, false, null, 2);
    }

    @Test
    public void testQueryPvMetadataByPvNameContains() {
        createQueryTestData();

        final QueryPvMetadataRequest.QueryPvMetadataCriterion.PvNameCriterion pvNameCriterion =
                QueryPvMetadataRequest.QueryPvMetadataCriterion.PvNameCriterion.newBuilder()
                        .addContains("BPM")
                        .build();
        final QueryPvMetadataRequest.QueryPvMetadataCriterion criterion =
                QueryPvMetadataRequest.QueryPvMetadataCriterion.newBuilder()
                        .setPvNameCriterion(pvNameCriterion)
                        .build();

        // two records contain "BPM" in pvName
        final List<PvMetadata> results = annotationServiceWrapper.sendAndVerifyQueryPvMetadata(
                List.of(criterion), 0, null, false, null, 2);
    }

    @Test
    public void testQueryPvMetadataByAliasExact() {
        createQueryTestData();

        final QueryPvMetadataRequest.QueryPvMetadataCriterion.AliasesCriterion aliasesCriterion =
                QueryPvMetadataRequest.QueryPvMetadataCriterion.AliasesCriterion.newBuilder()
                        .addExact("bpm01-alias")
                        .build();
        final QueryPvMetadataRequest.QueryPvMetadataCriterion criterion =
                QueryPvMetadataRequest.QueryPvMetadataCriterion.newBuilder()
                        .setAliasesCriterion(aliasesCriterion)
                        .build();

        final List<PvMetadata> results = annotationServiceWrapper.sendAndVerifyQueryPvMetadata(
                List.of(criterion), 0, null, false, null, 1);
        assertEquals("SR:C01-MG:G02{BPM:1}Pos-X", results.get(0).getPvName());
    }

    @Test
    public void testQueryPvMetadataByAliasContains() {
        createQueryTestData();

        final QueryPvMetadataRequest.QueryPvMetadataCriterion.AliasesCriterion aliasesCriterion =
                QueryPvMetadataRequest.QueryPvMetadataCriterion.AliasesCriterion.newBuilder()
                        .addContains("gcc")
                        .build();
        final QueryPvMetadataRequest.QueryPvMetadataCriterion criterion =
                QueryPvMetadataRequest.QueryPvMetadataCriterion.newBuilder()
                        .setAliasesCriterion(aliasesCriterion)
                        .build();

        // two records have aliases containing "gcc"
        annotationServiceWrapper.sendAndVerifyQueryPvMetadata(
                List.of(criterion), 0, null, false, null, 2);
    }

    @Test
    public void testQueryPvMetadataByTags() {
        createQueryTestData();

        final QueryPvMetadataRequest.QueryPvMetadataCriterion.TagsCriterion tagsCriterion =
                QueryPvMetadataRequest.QueryPvMetadataCriterion.TagsCriterion.newBuilder()
                        .addValues("vacuum")
                        .build();
        final QueryPvMetadataRequest.QueryPvMetadataCriterion criterion =
                QueryPvMetadataRequest.QueryPvMetadataCriterion.newBuilder()
                        .setTagsCriterion(tagsCriterion)
                        .build();

        // two records have "vacuum" tag
        annotationServiceWrapper.sendAndVerifyQueryPvMetadata(
                List.of(criterion), 0, null, false, null, 2);
    }

    @Test
    public void testQueryPvMetadataByAttributeKeyOnly() {
        createQueryTestData();

        final QueryPvMetadataRequest.QueryPvMetadataCriterion.AttributesCriterion attrCriterion =
                QueryPvMetadataRequest.QueryPvMetadataCriterion.AttributesCriterion.newBuilder()
                        .setKey("sector")
                        .build();
        final QueryPvMetadataRequest.QueryPvMetadataCriterion criterion =
                QueryPvMetadataRequest.QueryPvMetadataCriterion.newBuilder()
                        .setAttributesCriterion(attrCriterion)
                        .build();

        // all 4 records have "sector" attribute
        annotationServiceWrapper.sendAndVerifyQueryPvMetadata(
                List.of(criterion), 0, null, false, null, 4);
    }

    @Test
    public void testQueryPvMetadataByAttributeKeyAndValue() {
        createQueryTestData();

        final QueryPvMetadataRequest.QueryPvMetadataCriterion.AttributesCriterion attrCriterion =
                QueryPvMetadataRequest.QueryPvMetadataCriterion.AttributesCriterion.newBuilder()
                        .setKey("type")
                        .addValues("bpm")
                        .build();
        final QueryPvMetadataRequest.QueryPvMetadataCriterion criterion =
                QueryPvMetadataRequest.QueryPvMetadataCriterion.newBuilder()
                        .setAttributesCriterion(attrCriterion)
                        .build();

        // two records have type=bpm
        annotationServiceWrapper.sendAndVerifyQueryPvMetadata(
                List.of(criterion), 0, null, false, null, 2);
    }

    @Test
    public void testQueryPvMetadataMultiCriterionAnd() {
        createQueryTestData();

        // sector attribute "01" AND tag "vacuum" → only the sector 01 gauge
        final QueryPvMetadataRequest.QueryPvMetadataCriterion attrCriterion =
                QueryPvMetadataRequest.QueryPvMetadataCriterion.newBuilder()
                        .setAttributesCriterion(
                                QueryPvMetadataRequest.QueryPvMetadataCriterion.AttributesCriterion.newBuilder()
                                        .setKey("sector")
                                        .addValues("01")
                                        .build())
                        .build();
        final QueryPvMetadataRequest.QueryPvMetadataCriterion tagCriterion =
                QueryPvMetadataRequest.QueryPvMetadataCriterion.newBuilder()
                        .setTagsCriterion(
                                QueryPvMetadataRequest.QueryPvMetadataCriterion.TagsCriterion.newBuilder()
                                        .addValues("vacuum")
                                        .build())
                        .build();

        final List<PvMetadata> results = annotationServiceWrapper.sendAndVerifyQueryPvMetadata(
                List.of(attrCriterion, tagCriterion), 0, null, false, null, 1);
        assertEquals("SR:C01-MG:G02{GCC:1}Prs-I", results.get(0).getPvName());
    }

    @Test
    public void testQueryPvMetadataPagination() {
        // create 3 records, query with limit=2, verify nextPageToken, then page 2
        annotationServiceWrapper.sendAndVerifySavePvMetadata(
                new AnnotationTestBase.SavePvMetadataParams(
                        "PAGE:PV:A", null, null, null, null, null), false, null);
        annotationServiceWrapper.sendAndVerifySavePvMetadata(
                new AnnotationTestBase.SavePvMetadataParams(
                        "PAGE:PV:B", null, null, null, null, null), false, null);
        annotationServiceWrapper.sendAndVerifySavePvMetadata(
                new AnnotationTestBase.SavePvMetadataParams(
                        "PAGE:PV:C", null, null, null, null, null), false, null);

        final QueryPvMetadataRequest.QueryPvMetadataCriterion criterion =
                QueryPvMetadataRequest.QueryPvMetadataCriterion.newBuilder()
                        .setPvNameCriterion(
                                QueryPvMetadataRequest.QueryPvMetadataCriterion.PvNameCriterion.newBuilder()
                                        .addPrefix("PAGE:")
                                        .build())
                        .build();

        // page 1: limit=2, expect 2 results and a non-blank nextPageToken
        final AnnotationTestBase.QueryPvMetadataResponseObserver page1Observer =
                new AnnotationTestBase.QueryPvMetadataResponseObserver();
        final com.ospreydcs.dp.grpc.v1.annotation.QueryPvMetadataRequest page1Request =
                AnnotationTestBase.buildQueryPvMetadataRequest(List.of(criterion), 2, null);
        new Thread(() -> com.ospreydcs.dp.grpc.v1.annotation.DpAnnotationServiceGrpc
                .newStub(annotationServiceWrapper.getChannel())
                .queryPvMetadata(page1Request, page1Observer)).start();
        page1Observer.await();
        assertFalse(page1Observer.getErrorMessage(), page1Observer.isError());
        assertEquals(2, page1Observer.getPvMetadataList().size());
        final String nextPageToken = page1Observer.getNextPageToken();
        assertNotNull(nextPageToken);
        assertFalse(nextPageToken.isBlank());

        // page 2: use nextPageToken, expect 1 result and empty nextPageToken (last page)
        final AnnotationTestBase.QueryPvMetadataResponseObserver page2Observer =
                new AnnotationTestBase.QueryPvMetadataResponseObserver();
        final com.ospreydcs.dp.grpc.v1.annotation.QueryPvMetadataRequest page2Request =
                AnnotationTestBase.buildQueryPvMetadataRequest(List.of(criterion), 2, nextPageToken);
        new Thread(() -> com.ospreydcs.dp.grpc.v1.annotation.DpAnnotationServiceGrpc
                .newStub(annotationServiceWrapper.getChannel())
                .queryPvMetadata(page2Request, page2Observer)).start();
        page2Observer.await();
        assertFalse(page2Observer.getErrorMessage(), page2Observer.isError());
        assertEquals(1, page2Observer.getPvMetadataList().size());
        assertTrue("expected empty nextPageToken on last page", page2Observer.getNextPageToken().isBlank());
    }

    // =========================================================================
    // getPvMetadata tests
    // =========================================================================

    @Test
    public void testGetPvMetadataRejectBlankPvNameOrAlias() {
        annotationServiceWrapper.sendAndVerifyGetPvMetadata(
                "", true, "GetPvMetadataRequest.pvNameOrAlias must be specified");
    }

    @Test
    public void testGetPvMetadataNotFound() {
        annotationServiceWrapper.sendAndVerifyGetPvMetadata(
                "NONEXISTENT:PV:999", true, "no PvMetadata record found for");
    }

    @Test
    public void testGetPvMetadataByPvName() {
        annotationServiceWrapper.sendAndVerifySavePvMetadata(
                new AnnotationTestBase.SavePvMetadataParams(
                        "GET:PV:001", List.of("get-alias"), List.of("test"),
                        List.of(Attribute.newBuilder().setName("k").setValue("v").build()),
                        "desc", "craigmcc"),
                false, null);

        final PvMetadata pvMetadata = annotationServiceWrapper.sendAndVerifyGetPvMetadata(
                "GET:PV:001", false, null);
        assertNotNull(pvMetadata);
        assertEquals("GET:PV:001", pvMetadata.getPvName());
        assertTrue(pvMetadata.getAliasesList().contains("get-alias"));
        assertEquals("desc", pvMetadata.getDescription());
        assertEquals("craigmcc", pvMetadata.getModifiedBy());
    }

    @Test
    public void testGetPvMetadataByAlias() {
        annotationServiceWrapper.sendAndVerifySavePvMetadata(
                new AnnotationTestBase.SavePvMetadataParams(
                        "GET:PV:002", List.of("get-alias-2"), null, null, null, null),
                false, null);

        final PvMetadata pvMetadata = annotationServiceWrapper.sendAndVerifyGetPvMetadata(
                "get-alias-2", false, null);
        assertNotNull(pvMetadata);
        assertEquals("GET:PV:002", pvMetadata.getPvName());
    }

    // =========================================================================
    // deletePvMetadata tests
    // =========================================================================

    @Test
    public void testDeletePvMetadataRejectBlankPvNameOrAlias() {
        annotationServiceWrapper.sendAndVerifyDeletePvMetadata(
                "", true, "DeletePvMetadataRequest.pvNameOrAlias must be specified");
    }

    @Test
    public void testDeletePvMetadataByPvName() {
        annotationServiceWrapper.sendAndVerifySavePvMetadata(
                new AnnotationTestBase.SavePvMetadataParams(
                        "DEL:PV:001", null, null, null, null, null),
                false, null);

        final String deletedPvName = annotationServiceWrapper.sendAndVerifyDeletePvMetadata(
                "DEL:PV:001", false, null);
        assertEquals("DEL:PV:001", deletedPvName);

        // verify removed from DB: findPvMetadata should return null after retry timeout (use get instead)
        annotationServiceWrapper.sendAndVerifyGetPvMetadata(
                "DEL:PV:001", true, "no PvMetadata record found for");
    }

    @Test
    public void testDeletePvMetadataByAlias() {
        annotationServiceWrapper.sendAndVerifySavePvMetadata(
                new AnnotationTestBase.SavePvMetadataParams(
                        "DEL:PV:002", List.of("del-alias"), null, null, null, null),
                false, null);

        final String deletedPvName = annotationServiceWrapper.sendAndVerifyDeletePvMetadata(
                "del-alias", false, null);
        assertEquals("DEL:PV:002", deletedPvName);

        annotationServiceWrapper.sendAndVerifyGetPvMetadata(
                "DEL:PV:002", true, "no PvMetadata record found for");
    }

    @Test
    public void testDeletePvMetadataNotFound() {
        annotationServiceWrapper.sendAndVerifyDeletePvMetadata(
                "NONEXISTENT:PV:999", true, "no PvMetadata record found for");
    }

    // =========================================================================
    // patchPvMetadata and bulkSavePvMetadata stub tests
    // =========================================================================

    @Test
    public void testPatchPvMetadataStub() {
        annotationServiceWrapper.sendAndVerifyPatchPvMetadataStub("ANY:PV:001");
    }

    @Test
    public void testBulkSavePvMetadataStub() {
        annotationServiceWrapper.sendAndVerifyBulkSavePvMetadataStub();
    }

}
