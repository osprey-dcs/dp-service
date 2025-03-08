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
            {
                final List<Document> annotationsIndexesDocuments =
                        this.mongoCollectionAnnotations.listIndexes().into(new ArrayList<>());
                final List<String> annotationsIndexNames = annotationsIndexesDocuments.stream()
                        .map(document -> (String) document.get("name"))
                        .collect(Collectors.toList());
                final List<String> expectedIndexNames =
                        List.of(
                                "_id_",
                                "ownerId_1",
                                "dataSetIds_1",
                                "annotationIds_1",
                                "tags_1",
                                "attributeMap.$**_1",
                                "name_text_comment_text_eventMetadata.description_text_ownerId_1"
                        );
                assertEquals(expectedIndexNames, annotationsIndexNames);
            }

            // check buckets collection indexes
            {
                final List<Document> bucketsIndexesDocuments =
                        this.mongoCollectionBuckets.listIndexes().into(new ArrayList<>());
                final List<String> bucketsIndexNames = bucketsIndexesDocuments.stream()
                        .map(document -> (String) document.get("name"))
                        .collect(Collectors.toList());
                final List<String> expectedIndexNames =
                        List.of(
                                "_id_",
                                "pvName_1",
                                "pvName_1_dataTimestamps.firstTime.seconds_1_dataTimestamps.firstTime.nanos_1_dataTimestamps.lastTime.seconds_1_dataTimestamps.lastTime.nanos_1",
                                "providerId_1"
                        );
                assertEquals(expectedIndexNames, bucketsIndexNames);
            }

            // check calculations collection indexes
            {
                final List<Document> calculationsIndexesDocuments =
                        this.mongoCollectionCalculations.listIndexes().into(new ArrayList<>());
                final List<String> calculationsIndexNames = calculationsIndexesDocuments.stream()
                        .map(document -> (String) document.get("name"))
                        .collect(Collectors.toList());
                final List<String> expectedIndexNames = new ArrayList<>();
                assertEquals(expectedIndexNames, calculationsIndexNames);
                // NOTE: oddly no indexes are listed for this collection,
                // apparently because 1) it is empty and 2) no other indexes exist
                // assertTrue(calculationsIndexNames.contains("_id_"));
            }

            // check datasets collection indexes
            {
                final List<Document> datasetsIndexesDocuments =
                        this.mongoCollectionDataSets.listIndexes().into(new ArrayList<>());
                final List<String> datasetsIndexNames = datasetsIndexesDocuments.stream()
                        .map(document -> (String) document.get("name"))
                        .collect(Collectors.toList());
                final List<String> expectedIndexNames =
                        List.of(
                                "_id_",
                                "ownerId_1",
                                "name_text_description_text_ownerId_1",
                                "dataBlocks.pvNames_1"
                        );
                assertEquals(expectedIndexNames, datasetsIndexNames);
            }

            // check providers collection indexes
            {
                final List<Document> providersIndexesDocuments =
                        this.mongoCollectionProviders.listIndexes().into(new ArrayList<>());
                final List<String> providersIndexNames = providersIndexesDocuments.stream()
                        .map(document -> (String) document.get("name"))
                        .collect(Collectors.toList());
                final List<String> expectedIndexNames =
                        List.of(
                                "_id_",
                                "name_1",
                                "name_text_description_text",
                                "tags_1",
                                "attributeMap.$**_1"
                        );
                assertEquals(expectedIndexNames, providersIndexNames);
            }

            // check requestStatus collection indexes
            {
                final List<Document> requestStatusIndexesDocuments =
                        this.mongoCollectionRequestStatus.listIndexes().into(new ArrayList<>());
                final List<String> requestStatusIndexNames = requestStatusIndexesDocuments.stream()
                        .map(document -> (String) document.get("name"))
                        .collect(Collectors.toList());
                final List<String> expectedIndexNames =
                        List.of(
                                "_id_",
                                "providerId_1_requestId_1",
                                "providerId_1_requestStatusCase_1_createdAt_1",
                                "requestStatusCase_1_createdAt_1"
                        );
                assertEquals(expectedIndexNames, requestStatusIndexNames);
            }
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
