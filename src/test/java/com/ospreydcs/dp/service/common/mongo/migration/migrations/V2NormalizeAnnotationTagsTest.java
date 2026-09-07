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
 * Covers the annotation tags normalization migration (#248 Phase 2, plan D12).  Saves normalize
 * tags to lowercase/deduplicated/sorted as of Phase 2; this migration brings previously stored
 * tags into line so a normalized {@code TagsCriterion} value cannot silently miss them.
 */
public class V2NormalizeAnnotationTagsTest {

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

        migration.apply(testClient.database());

        assertEquals(List.of("beam loss", "outage", "vacuum"), storedTags("mixed"));
    }

    @Test
    public void testLeavesNormalizedAndUntaggedDocumentsAlone() throws DpException {
        annotations.insertOne(new Document("name", "normalized")
                .append("tags", List.of("alpha", "beta")));
        annotations.insertOne(new Document("name", "untagged"));

        migration.apply(testClient.database());

        assertEquals(List.of("alpha", "beta"), storedTags("normalized"));
        assertNull(storedTags("untagged"));
    }

    @Test
    public void testIdempotentReRun() throws DpException {
        annotations.insertOne(new Document("name", "rerun")
                .append("tags", List.of("Zeta", "alpha", "ALPHA")));

        migration.apply(testClient.database());
        final List<String> afterFirstRun = storedTags("rerun");
        migration.apply(testClient.database());

        assertEquals(List.of("alpha", "zeta"), afterFirstRun);
        assertEquals(afterFirstRun, storedTags("rerun"));
    }
}
