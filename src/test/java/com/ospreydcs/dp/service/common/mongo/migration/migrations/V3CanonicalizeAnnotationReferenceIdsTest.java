package com.ospreydcs.dp.service.common.mongo.migration.migrations;

import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Filters;
import com.ospreydcs.dp.service.common.exception.DpException;
import com.ospreydcs.dp.service.common.mongo.MongoClientBase;
import com.ospreydcs.dp.service.common.mongo.MongoTestClient;
import org.bson.Document;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

/**
 * Covers the annotation reference-id canonicalization migration (#248 Phase 2 review fixes).
 * Saves canonicalize dataSetIds/annotationIds to lowercase hex; this migration brings previously
 * stored references into line so string-matched reference checks (deleteDataSet's referential
 * integrity, the queryAnnotations dataSets/annotations criteria) cannot silently miss them.
 */
public class V3CanonicalizeAnnotationReferenceIdsTest {

    private MigrationTestClient testClient;
    private MongoCollection<Document> annotations;
    private V3CanonicalizeAnnotationReferenceIds migration;

    private static class MigrationTestClient extends MongoTestClient {
        MongoDatabase database() {
            return mongoDatabase;
        }
    }

    @Before
    public void setUp() {
        testClient = new MigrationTestClient();
        testClient.init();
        annotations = testClient.database().getCollection(MongoClientBase.COLLECTION_NAME_ANNOTATIONS);
        migration = new V3CanonicalizeAnnotationReferenceIds();
        annotations.deleteMany(new Document());
    }

    @After
    public void tearDown() {
        annotations.deleteMany(new Document());
        testClient.fini();
    }

    private List<String> storedList(String name, String field) {
        final Document document = annotations.find(Filters.eq("name", name)).first();
        return document == null ? null : document.getList(field, String.class);
    }

    @Test
    public void testCanonicalizesCaseVariantReferenceIds() throws DpException {
        annotations.insertOne(new Document("name", "caseVariant")
                .append("dataSetIds", List.of("66A1B2C3D4E5F60718293A4B", "66a1b2c3d4e5f60718293a4c"))
                .append("annotationIds", List.of("66A1B2C3D4E5F60718293A4D")));

        migration.apply(testClient.database());

        assertEquals(
                List.of("66a1b2c3d4e5f60718293a4b", "66a1b2c3d4e5f60718293a4c"),
                storedList("caseVariant", "dataSetIds"));
        assertEquals(
                List.of("66a1b2c3d4e5f60718293a4d"),
                storedList("caseVariant", "annotationIds"));
    }

    @Test
    public void testLeavesCanonicalAndNonObjectIdEntriesAlone() throws DpException {
        annotations.insertOne(new Document("name", "canonical")
                .append("dataSetIds", List.of("66a1b2c3d4e5f60718293a4b")));
        annotations.insertOne(new Document("name", "junkEntry")
                .append("dataSetIds", List.of("not-an-object-id", "66A1B2C3D4E5F60718293A4B")));
        annotations.insertOne(new Document("name", "noReferences"));

        migration.apply(testClient.database());

        assertEquals(List.of("66a1b2c3d4e5f60718293a4b"), storedList("canonical", "dataSetIds"));
        // the junk entry is preserved as evidence; the valid entry beside it is still canonicalized
        assertEquals(
                List.of("not-an-object-id", "66a1b2c3d4e5f60718293a4b"),
                storedList("junkEntry", "dataSetIds"));
        assertNull(storedList("noReferences", "dataSetIds"));
    }

    @Test
    public void testIdempotentReRun() throws DpException {
        annotations.insertOne(new Document("name", "rerun")
                .append("dataSetIds", List.of("66A1B2C3D4E5F60718293A4B"))
                .append("annotationIds", List.of("66a1b2c3d4e5f60718293a4d")));

        migration.apply(testClient.database());
        final List<String> afterFirstRun = storedList("rerun", "dataSetIds");
        migration.apply(testClient.database());

        assertEquals(List.of("66a1b2c3d4e5f60718293a4b"), afterFirstRun);
        assertEquals(afterFirstRun, storedList("rerun", "dataSetIds"));
        assertEquals(List.of("66a1b2c3d4e5f60718293a4d"), storedList("rerun", "annotationIds"));
    }
}
