package com.ospreydcs.dp.service.common.mongo.migration.migrations;

import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.UpdateOptions;
import com.mongodb.client.model.Updates;
import com.ospreydcs.dp.service.common.exception.DpException;
import com.ospreydcs.dp.service.common.mongo.MongoClientBase;
import com.ospreydcs.dp.service.common.mongo.MongoTestClient;
import com.ospreydcs.dp.service.common.mongo.migration.MigrationContext;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.List;

import static org.mockito.AdditionalAnswers.delegatesTo;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

/**
 * Covers the annotation reference-id canonicalization migration (#248 Phase 2 review fixes).
 * Saves canonicalize dataSetIds/annotationIds to lowercase hex; this migration brings previously
 * stored references into line so string-matched reference checks (deleteDataSet's referential
 * integrity, the queryAnnotations dataSets/annotations criteria) cannot silently miss them.
 */
public class V3CanonicalizeAnnotationReferenceIdsTest {

    private static final MigrationContext CONTEXT = MigrationContext.forClaimToken("test");

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

        migration.apply(testClient.database(), CONTEXT);

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

        migration.apply(testClient.database(), CONTEXT);

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

        migration.apply(testClient.database(), CONTEXT);
        final List<String> afterFirstRun = storedList("rerun", "dataSetIds");
        migration.apply(testClient.database(), CONTEXT);

        assertEquals(List.of("66a1b2c3d4e5f60718293a4b"), afterFirstRun);
        assertEquals(afterFirstRun, storedList("rerun", "dataSetIds"));
        assertEquals(List.of("66a1b2c3d4e5f60718293a4d"), storedList("rerun", "annotationIds"));
    }

    /**
     * A database that behaves as the real one, except that {@code concurrentSave} runs between the
     * migration's read of an annotation and its write: the position of a save path that lands
     * while a displaced holder is still inside this loop (#292).
     */
    @SuppressWarnings("unchecked")
    private MongoDatabase databaseWithASaveBeforeEachWrite(Runnable concurrentSave) {
        final MongoCollection<Document> intercepted =
                mock(MongoCollection.class, delegatesTo(annotations));
        doAnswer(invocation -> {
            concurrentSave.run();
            return annotations.updateOne(
                    invocation.<Bson>getArgument(0), invocation.<Bson>getArgument(1),
                    invocation.<UpdateOptions>getArgument(2));
        }).when(intercepted).updateOne(any(Bson.class), any(Bson.class), any(UpdateOptions.class));

        final MongoDatabase database = mock(MongoDatabase.class, delegatesTo(testClient.database()));
        doReturn(intercepted).when(database)
                .getCollection(MongoClientBase.COLLECTION_NAME_ANNOTATIONS);
        return database;
    }

    @Test
    public void testDoesNotOverwriteReferencesSavedBetweenItsReadAndItsWrite() throws DpException {
        annotations.insertOne(new Document("name", "racing")
                .append("dataSetIds", List.of("66A1B2C3D4E5F60718293A4B")));

        // the save path stores canonical ids, and these are not the ones the migration read
        final MongoDatabase database = databaseWithASaveBeforeEachWrite(() ->
                annotations.updateOne(
                        Filters.eq("name", "racing"),
                        Updates.set("dataSetIds", List.of("77a1b2c3d4e5f60718293a4c"))));

        migration.apply(database, CONTEXT);

        assertEquals(
                "a write filtered on _id alone stores the canonical form of the references that "
                        + "were read, silently discarding the save",
                List.of("77a1b2c3d4e5f60718293a4c"), storedList("racing", "dataSetIds"));
    }
}
