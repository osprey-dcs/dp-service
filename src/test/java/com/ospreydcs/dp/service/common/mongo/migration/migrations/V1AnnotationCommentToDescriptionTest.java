package com.ospreydcs.dp.service.common.mongo.migration.migrations;

import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.Indexes;
import com.ospreydcs.dp.service.common.bson.BsonConstants;
import com.ospreydcs.dp.service.common.exception.DpException;
import com.ospreydcs.dp.service.common.mongo.MongoClientBase;
import com.ospreydcs.dp.service.common.mongo.MongoTestClient;
import org.bson.Document;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Covers the annotation {@code comment} → {@code description} migration.
 *
 * <p>The index half is the part worth testing carefully: MongoDB permits only one text index per
 * collection, so failing to drop the old one makes the service's own index creation fail at startup
 * rather than merely leaving a redundant index behind.
 */
public class V1AnnotationCommentToDescriptionTest {

    private MigrationTestClient testClient;
    private MongoDatabase database;
    private MongoCollection<Document> annotations;
    private V1AnnotationCommentToDescription migration;

    private static class MigrationTestClient extends MongoTestClient {
        MongoDatabase database() {
            return mongoDatabase;
        }
    }

    @Before
    public void setUp() {
        testClient = new MigrationTestClient();
        testClient.init();
        database = testClient.database();
        annotations = database.getCollection(MongoClientBase.COLLECTION_NAME_ANNOTATIONS);
        migration = new V1AnnotationCommentToDescription();
        annotations.deleteMany(new Document());
        dropAllTextIndexes();
    }

    @After
    public void tearDown() {
        annotations.deleteMany(new Document());
        dropAllTextIndexes();
        testClient.fini();
    }

    private void dropAllTextIndexes() {
        for (Document index : annotations.listIndexes()) {
            if (index.get("weights", Document.class) != null) {
                annotations.dropIndex(index.getString("name"));
            }
        }
    }

    /** Recreates the pre-migration index: text over name/comment/event.description plus ownerId. */
    private void createOldTextIndex() {
        annotations.createIndex(
                Indexes.compoundIndex(
                        Indexes.compoundIndex(
                                Indexes.text(BsonConstants.BSON_KEY_ANNOTATION_NAME),
                                Indexes.text("comment"),
                                Indexes.text(BsonConstants.BSON_KEY_EVENT_DESCRIPTION)),
                        Indexes.ascending(BsonConstants.BSON_KEY_ANNOTATION_OWNER_ID)));
    }

    /** Creates the post-migration index, as MongoClientBase does after the rename. */
    private void createNewTextIndex() {
        annotations.createIndex(
                Indexes.compoundIndex(
                        Indexes.compoundIndex(
                                Indexes.text(BsonConstants.BSON_KEY_ANNOTATION_NAME),
                                Indexes.text("description"),
                                Indexes.text(BsonConstants.BSON_KEY_EVENT_DESCRIPTION)),
                        Indexes.ascending(BsonConstants.BSON_KEY_ANNOTATION_OWNER_ID)));
    }

    private List<String> textIndexWeightFields() {
        final List<String> fields = new ArrayList<>();
        for (Document index : annotations.listIndexes()) {
            final Document weights = index.get("weights", Document.class);
            if (weights != null) {
                fields.addAll(weights.keySet());
            }
        }
        return fields;
    }

    // ------------------------------------------------------------------
    // data
    // ------------------------------------------------------------------

    @Test
    public void testRenamesCommentToDescription() throws DpException {

        annotations.insertOne(new Document("name", "a").append("comment", "first"));
        annotations.insertOne(new Document("name", "b").append("comment", "second"));

        migration.apply(database);

        assertEquals(
                "no document should retain the old field",
                0, annotations.countDocuments(Filters.exists("comment")));

        final Document first = annotations.find(Filters.eq("name", "a")).first();
        assertNotNull(first);
        assertEquals("first", first.getString("description"));
        assertNull(first.get("comment"));
    }

    @Test
    public void testLeavesDocumentsWithoutTheOldFieldAlone() throws DpException {

        annotations.insertOne(new Document("name", "a").append("description", "already migrated"));
        annotations.insertOne(new Document("name", "b"));

        migration.apply(database);

        assertEquals(
                "already migrated",
                annotations.find(Filters.eq("name", "a")).first().getString("description"));
        assertNull(annotations.find(Filters.eq("name", "b")).first().get("description"));
    }

    @Test
    public void testStopsRatherThanOverwritingAnExistingDescription() {

        // Should never arise, since nothing ever wrote both. If it does, $rename would silently
        // destroy the description, so the migration must refuse instead.
        annotations.insertOne(
                new Document("name", "a").append("comment", "old").append("description", "new"));

        try {
            migration.apply(database);
            fail("expected the migration to refuse rather than overwrite a description");
        } catch (DpException ex) {
            assertTrue(ex.getMessage().contains("both"));
        }

        final Document document = annotations.find(Filters.eq("name", "a")).first();
        assertEquals("new", document.getString("description"));
        assertEquals("old", document.getString("comment"));
    }

    // ------------------------------------------------------------------
    // index
    // ------------------------------------------------------------------

    @Test
    public void testDropsTheOldTextIndex() throws DpException {

        createOldTextIndex();
        assertTrue(textIndexWeightFields().contains("comment"));

        migration.apply(database);

        assertFalse(
                "the old text index must be gone, or creating the replacement fails",
                textIndexWeightFields().contains("comment"));
    }

    @Test
    public void testNewIndexCanBeCreatedAfterMigrating() throws DpException {

        createOldTextIndex();
        annotations.insertOne(new Document("name", "a").append("comment", "text"));

        migration.apply(database);

        // This is the assertion that matters: before the drop, this call fails with
        // IndexOptionsConflict because Mongo permits only one text index per collection.
        createNewTextIndex();

        assertTrue(textIndexWeightFields().contains("description"));
        assertFalse(textIndexWeightFields().contains("comment"));
    }

    @Test
    public void testIdentifiesTheIndexByWeightsNotByName() throws DpException {

        // A text index created under an explicit name still has "comment" in its weights. Matching
        // on the default derived name would miss this one; matching on the key document would be
        // ambiguous, since text indexes all share the key {_fts, _ftsx, ...}.
        annotations.createIndex(
                Indexes.compoundIndex(
                        Indexes.compoundIndex(
                                Indexes.text(BsonConstants.BSON_KEY_ANNOTATION_NAME),
                                Indexes.text("comment"),
                                Indexes.text(BsonConstants.BSON_KEY_EVENT_DESCRIPTION)),
                        Indexes.ascending(BsonConstants.BSON_KEY_ANNOTATION_OWNER_ID)),
                new com.mongodb.client.model.IndexOptions().name("customAnnotationTextIndex"));

        migration.apply(database);

        assertFalse(textIndexWeightFields().contains("comment"));
    }

    @Test
    public void testNoIndexToDropIsNotAnError() throws DpException {
        // A fresh database has no such index; the migration must treat that as normal.
        migration.apply(database);
        assertFalse(textIndexWeightFields().contains("comment"));
    }

    // ------------------------------------------------------------------
    // idempotency
    // ------------------------------------------------------------------

    @Test
    public void testApplyingTwiceLeavesTheSameState() throws DpException {

        createOldTextIndex();
        annotations.insertOne(new Document("name", "a").append("comment", "first"));
        annotations.insertOne(new Document("name", "b").append("comment", "second"));

        migration.apply(database);

        // A crash between applying and recording the version can re-run a migration, and the
        // operator recovery path for a stuck claim can too.
        migration.apply(database);

        assertEquals(0, annotations.countDocuments(Filters.exists("comment")));
        assertEquals(2, annotations.countDocuments(Filters.exists("description")));
        assertEquals(
                "first", annotations.find(Filters.eq("name", "a")).first().getString("description"));
        assertFalse(textIndexWeightFields().contains("comment"));
    }
}
