package com.ospreydcs.dp.service.common.mongo;

import com.mongodb.client.MongoCursor;
import com.ospreydcs.dp.service.ingest.handler.mongo.MongoIngestionHandler;
import com.ospreydcs.dp.service.ingest.handler.mongo.MongoIngestionHandlerTestBase;
import org.bson.Document;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class MongoIndexTest {

    protected static class MongoIndexTestClient extends MongoTestClient {

        public void checkIndexes() {

            // check annotations collection indexes
            List<Document> annotationsIndexesDocuments =
                    this.mongoCollectionAnnotations.listIndexes().into(new ArrayList<>());
            assertEquals(4, annotationsIndexesDocuments.size());
            List<String> annotationsIndexNames = annotationsIndexesDocuments.stream()
                    .map(document -> (String) document.get("name"))
                    .collect(Collectors.toList());
            assertTrue(annotationsIndexNames.contains("_id_"));
            assertTrue(annotationsIndexNames.contains("ownerId_1"));
            assertTrue(annotationsIndexNames.contains("ownerId_1_type_1"));
            assertTrue(annotationsIndexNames.contains("type_1_comment_text"));

            // check buckets collection indexes
            List<Document> bucketsIndexesDocuments =
                    this.mongoCollectionBuckets.listIndexes().into(new ArrayList<>());
            assertEquals(3, bucketsIndexesDocuments.size());
            List<String> bucketsIndexNames = bucketsIndexesDocuments.stream()
                    .map(document -> (String) document.get("name"))
                    .collect(Collectors.toList());
            assertTrue(bucketsIndexNames.contains("_id_"));
            assertTrue(bucketsIndexNames.contains("pvName_1"));
            assertTrue(bucketsIndexNames.contains("pvName_1_firstSeconds_1_firstNanos_1_lastSeconds_1_lastNanos_1"));

            // check datasets collection indexes
            List<Document> datasetsIndexesDocuments =
                    this.mongoCollectionDataSets.listIndexes().into(new ArrayList<>());
            assertEquals(5, datasetsIndexesDocuments.size());
            List<String> datasetsIndexNames = datasetsIndexesDocuments.stream()
                    .map(document -> (String) document.get("name"))
                    .collect(Collectors.toList());
            assertTrue(datasetsIndexNames.contains("_id_"));
            assertTrue(datasetsIndexNames.contains("description_text"));
            assertTrue(datasetsIndexNames.contains("name_1"));
            assertTrue(datasetsIndexNames.contains("ownerId_1"));
            assertTrue(datasetsIndexNames.contains("ownerId_1_name_1"));

            // check requestStatus collection indexes
            List<Document> requestStatusIndexesDocuments =
                    this.mongoCollectionRequestStatus.listIndexes().into(new ArrayList<>());
            assertEquals(3, requestStatusIndexesDocuments.size());
            List<String> requestStatusIndexNames = requestStatusIndexesDocuments.stream()
                    .map(document -> (String) document.get("name"))
                    .collect(Collectors.toList());
            assertTrue(requestStatusIndexNames.contains("_id_"));
            assertTrue(requestStatusIndexNames.contains("providerId_1_requestId_1"));
            assertTrue(requestStatusIndexNames.contains("providerId_1_updateTime_1"));

        }
    }

    @BeforeClass
    public static void setUp() throws Exception {
        // Use test db client to set database name globally to "dp-test" and remove that database if it already exists
        MongoTestClient.prepareTestDatabase();
    }

    @Test
    public void testIndexesExist() {
        MongoIndexTestClient testClient = new MongoIndexTestClient();
        testClient.init();
        testClient.checkIndexes();
        testClient.fini();
    }
}
