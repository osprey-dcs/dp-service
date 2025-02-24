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
            final List<Document> annotationsIndexesDocuments =
                    this.mongoCollectionAnnotations.listIndexes().into(new ArrayList<>());
            assertEquals(7, annotationsIndexesDocuments.size());
            final List<String> annotationsIndexNames = annotationsIndexesDocuments.stream()
                    .map(document -> (String) document.get("name"))
                    .collect(Collectors.toList());
            assertTrue(annotationsIndexNames.contains("_id_"));
            assertTrue(annotationsIndexNames.contains("ownerId_1"));
            assertTrue(annotationsIndexNames.contains("dataSetIds_1"));
            assertTrue(annotationsIndexNames.contains("annotationIds_1"));
            assertTrue(annotationsIndexNames.contains("tags_1"));
            assertTrue(annotationsIndexNames.contains("attributeMap.$**_1"));
            assertTrue(annotationsIndexNames.contains("name_text_comment_text_eventMetadata.description_text_ownerId_1"));

            // check buckets collection indexes
            final List<Document> bucketsIndexesDocuments =
                    this.mongoCollectionBuckets.listIndexes().into(new ArrayList<>());
            assertEquals(3, bucketsIndexesDocuments.size());
            final List<String> bucketsIndexNames = bucketsIndexesDocuments.stream()
                    .map(document -> (String) document.get("name"))
                    .collect(Collectors.toList());
            assertTrue(bucketsIndexNames.contains("_id_"));
            assertTrue(bucketsIndexNames.contains("pvName_1"));
            assertTrue(bucketsIndexNames.contains("pvName_1_firstSeconds_1_firstNanos_1_lastSeconds_1_lastNanos_1"));

            // check calculations collection indexes
            final List<Document> calculationsIndexesDocuments =
                    this.mongoCollectionCalculations.listIndexes().into(new ArrayList<>());
            assertEquals(0, calculationsIndexesDocuments.size());
            final List<String> calculationsIndexNames = calculationsIndexesDocuments.stream()
                    .map(document -> (String) document.get("name"))
                    .collect(Collectors.toList());
            // oddly no indexes are listed for this collection, apparently because 1) it is empty and 2) no other indexes exist
//            assertTrue(calculationsIndexNames.contains("_id_"));

            // check datasets collection indexes
            final List<Document> datasetsIndexesDocuments =
                    this.mongoCollectionDataSets.listIndexes().into(new ArrayList<>());
            assertEquals(3, datasetsIndexesDocuments.size());
            final List<String> datasetsIndexNames = datasetsIndexesDocuments.stream()
                    .map(document -> (String) document.get("name"))
                    .collect(Collectors.toList());
            assertTrue(datasetsIndexNames.contains("_id_"));
            assertTrue(datasetsIndexNames.contains("ownerId_1"));
            assertTrue(datasetsIndexNames.contains("name_text_description_text_ownerId_1"));

            // check providers collection indexes
            final List<Document> providersIndexesDocuments =
                    this.mongoCollectionProviders.listIndexes().into(new ArrayList<>());
            assertEquals(2, providersIndexesDocuments.size());
            final List<String> providersIndexNames = providersIndexesDocuments.stream()
                    .map(document -> (String) document.get("name"))
                    .collect(Collectors.toList());
            assertTrue(providersIndexNames.contains("_id_"));
            assertTrue(providersIndexNames.contains("name_1"));

            // check requestStatus collection indexes
            final List<Document> requestStatusIndexesDocuments =
                    this.mongoCollectionRequestStatus.listIndexes().into(new ArrayList<>());
            assertEquals(4, requestStatusIndexesDocuments.size());
            final List<String> requestStatusIndexNames = requestStatusIndexesDocuments.stream()
                    .map(document -> (String) document.get("name"))
                    .collect(Collectors.toList());
            assertTrue(requestStatusIndexNames.contains("_id_"));
            assertTrue(requestStatusIndexNames.contains("providerId_1_requestId_1"));
            assertTrue(requestStatusIndexNames.contains("providerId_1_requestStatusCase_1_updateTime_1"));
            assertTrue(requestStatusIndexNames.contains("requestStatusCase_1_updateTime_1"));
        }
    }

    @BeforeClass
    public static void setUp() throws Exception {
        // Use test db client to set database name globally to "dp-test" and remove that database if it already exists
        MongoTestClient.prepareTestDatabase();
    }

    @Test
    public void testIndexesExist() {
        final MongoIndexTestClient testClient = new MongoIndexTestClient();
        testClient.init();
        testClient.checkIndexes();
        testClient.fini();
    }
}
