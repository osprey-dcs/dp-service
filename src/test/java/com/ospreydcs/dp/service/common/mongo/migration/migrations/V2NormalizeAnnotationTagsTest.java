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
 * Covers the annotation tags normalization migration (#248 Phase 2, plan D12).  Saves normalize
 * tags to lowercase/deduplicated/sorted as of Phase 2; this migration brings previously stored
 * tags into line so a normalized {@code TagsCriterion} value cannot silently miss them.
 */
public class V2NormalizeAnnotationTagsTest {

    private static final MigrationContext CONTEXT = MigrationContext.forClaimToken("test");

    private MigrationTestClient testClient;
    private MongoCollection<Document> annotations;
    private V2NormalizeAnnotationTags migration;

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
        migration = new V2NormalizeAnnotationTags();
        annotations.deleteMany(new Document());
    }

    @After
    public void tearDown() {
        annotations.deleteMany(new Document());
        testClient.fini();
    }

    private List<String> storedTags(String name) {
        final Document document = annotations.find(Filters.eq("name", name)).first();
        return document == null ? null : document.getList("tags", String.class);
    }

    @Test
    public void testNormalizesMixedCaseAndDuplicateTags() throws DpException {
        annotations.insertOne(new Document("name", "mixed")
                .append("tags", List.of("Beam Loss", "OUTAGE", "beam loss", "outage", "Vacuum")));

        migration.apply(testClient.database(), CONTEXT);

        assertEquals(List.of("beam loss", "outage", "vacuum"), storedTags("mixed"));
    }

    @Test
    public void testLeavesNormalizedAndUntaggedDocumentsAlone() throws DpException {
        annotations.insertOne(new Document("name", "normalized")
                .append("tags", List.of("alpha", "beta")));
        annotations.insertOne(new Document("name", "untagged"));

        migration.apply(testClient.database(), CONTEXT);

        assertEquals(List.of("alpha", "beta"), storedTags("normalized"));
        assertNull(storedTags("untagged"));
    }

    @Test
    public void testIdempotentReRun() throws DpException {
        annotations.insertOne(new Document("name", "rerun")
                .append("tags", List.of("Zeta", "alpha", "ALPHA")));

        migration.apply(testClient.database(), CONTEXT);
        final List<String> afterFirstRun = storedTags("rerun");
        migration.apply(testClient.database(), CONTEXT);

        assertEquals(List.of("alpha", "zeta"), afterFirstRun);
        assertEquals(afterFirstRun, storedTags("rerun"));
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
    public void testDoesNotOverwriteTagsSavedBetweenItsReadAndItsWrite() throws DpException {
        annotations.insertOne(new Document("name", "racing").append("tags", List.of("Old", "TAGS")));

        // the save path stores normalized tags, and these are not the ones the migration read
        final MongoDatabase database = databaseWithASaveBeforeEachWrite(() ->
                annotations.updateOne(
                        Filters.eq("name", "racing"),
                        Updates.set("tags", List.of("replaced", "since"))));

        migration.apply(database, CONTEXT);

        assertEquals(
                "a write filtered on _id alone stores the normalized form of the tags that were "
                        + "read, silently discarding the save",
                List.of("replaced", "since"), storedTags("racing"));
    }
}
