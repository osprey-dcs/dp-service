package com.ospreydcs.dp.service.ingest.handler.mongo;

import com.ospreydcs.dp.service.common.bson.bucket.BucketDocument;
import com.ospreydcs.dp.service.common.bson.RequestStatusDocument;
import com.ospreydcs.dp.service.common.mongo.MongoTestClient;
import org.bson.conversions.Bson;
import org.junit.*;

import java.util.ArrayList;
import java.util.List;

import static com.mongodb.client.model.Filters.and;
import static com.mongodb.client.model.Filters.eq;

/**
 * Provides jUnit test coverage for the MongoSyncDbHandler class.
 */
public class MongoSyncIngestionHandlerTest extends MongoIngestionHandlerTestBase {

    protected static class TestSyncClient extends MongoSyncIngestionClient implements TestClientInterface {

        @Override
        protected String getCollectionNameBuckets() {
            return getTestCollectionNameBuckets();
        }

        @Override
        protected String getCollectionNameRequestStatus() {
            return getTestCollectionNameRequestStatus();
        }

        public BucketDocument findBucketWithId(String id) {
            List<BucketDocument> matchingBuckets = new ArrayList<>();
            mongoCollectionBuckets.find(eq("_id", id)).into(matchingBuckets);
            if (matchingBuckets.size() > 0) {
                return matchingBuckets.get(0);
            } else {
                return null;
            }
        }

        public List<RequestStatusDocument> findRequestStatusList(Integer providerId, String requestId) {
            List<RequestStatusDocument> matchingDocuments = new ArrayList<>();
            Bson filter = and(eq("providerId", providerId), eq("requestId", requestId));
            mongoCollectionRequestStatus.find(filter).into(matchingDocuments);
            return matchingDocuments;
        }

    }

    /**
     * Sets up for jUnit test execution.
     *
     * @throws Exception
     */
    @BeforeClass
    public static void setUp() throws Exception {

        // Use test db client to set database name globally to "dp-test" and remove that database if it already exists
        MongoTestClient.prepareTestDatabase();

        TestSyncClient testClient = new TestSyncClient();
        MongoIngestionHandler handler = new MongoIngestionHandler(testClient);
        setUp(handler, testClient);
    }

    /**
     * Cleans up after jUnit test execution.
     * @throws Exception
     */
    @AfterClass
    public static void tearDown() throws Exception {
        MongoIngestionHandlerTestBase.tearDown();
    }

    @Test
    public void testHandleIngestionRequestSuccessFloat() {
        super.testHandleIngestionRequestSuccessFloat();
    }

    @Test
    public void testHandleIngestionRequestReject() {
        super.testHandleIngestionRequestReject();
    }

    @Test
    public void testHandleIngestionRequestSuccessString() {
        super.testHandleIngestionRequestSuccessString();
    }

    @Test
    public void testHandleIngestionRequestSuccessInt() {
        super.testHandleIngestionRequestSuccessInt();
    }

    @Test
    public void testHandleIngestionRequestSuccessBoolean() {
        super.testHandleIngestionRequestSuccessBoolean();
    }

    @Test
    public void testHandleIngestionRequestSuccessArray() {
        super.testHandleIngestionRequestSuccessArray();
    }

//    @Test
//    public void testHandleIngestionRequestErrorDataTypeMismatch() {
//        super.testHandleIngestionRequestErrorDataTypeMismatch();
//    }

}
