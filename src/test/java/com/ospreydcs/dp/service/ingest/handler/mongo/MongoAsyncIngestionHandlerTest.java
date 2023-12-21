package com.ospreydcs.dp.service.ingest.handler.mongo;

import com.mongodb.reactivestreams.client.FindPublisher;
import com.ospreydcs.dp.service.common.bson.BucketDocument;
import com.ospreydcs.dp.service.common.bson.RequestStatusDocument;
import org.bson.conversions.Bson;
import org.junit.*;
import org.junit.runners.MethodSorters;

import java.util.List;

import static com.mongodb.client.model.Filters.and;
import static com.mongodb.client.model.Filters.eq;
import static org.junit.Assert.assertTrue;

/**
 * Provides jUnit test coverage for the MongoAsyncDbHandler class.
 */
@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class MongoAsyncIngestionHandlerTest extends MongoIngestionHandlerTestBase {

    protected static class TestAsyncClient extends MongoAsyncIngestionClient implements TestClientInterface {

        @Override
        protected String getCollectionNameBuckets() {
            return getTestCollectionNameBuckets();
        }

        @Override
        protected String getCollectionNameRequestStatus() {
            return getTestCollectionNameRequestStatus();
        }

        public BucketDocument findBucketWithId(String id) {
            FindPublisher<BucketDocument> publisher = mongoCollectionBuckets.find(eq("_id", id));
            ObservableSubscriber<BucketDocument> subscriber = new ObservableSubscriber<>();
            publisher.subscribe(subscriber);
            subscriber.await(); // wait for async query results
            List<BucketDocument> matchingBuckets = subscriber.getReceived(); // get the list of received documents
            if (matchingBuckets.size() > 0) {
                return matchingBuckets.get(0);
            } else {
                return null;
            }
        }

        public List<RequestStatusDocument> findRequestStatusList(Integer providerId, String requestId) {
            Bson filter = and(eq("providerId", providerId), eq("requestId", requestId));
            FindPublisher<RequestStatusDocument> publisher = mongoCollectionRequestStatus.find(filter);
            ObservableSubscriber<RequestStatusDocument> subscriber = new ObservableSubscriber<>();
            publisher.subscribe(subscriber);
            subscriber.await(); // wait for async query results
            return subscriber.getReceived(); // return the list of received documents
        }

    }

    /**
     * Sets up for jUnit test execution.
     *
     * @throws Exception
     */
    @BeforeClass
    public static void setUp() throws Exception {
        TestAsyncClient testClient = new TestAsyncClient();
        MongoIngestionHandler handler = new MongoIngestionHandler(testClient);
        setUp(handler, testClient);
    }

    /**
     * Cleans up after jUnit test execution.
     *
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
    public void testHandleIngestionRequestErrorDataTypeArray() {
        super.testHandleIngestionRequestErrorDataTypeArray();
    }

    @Test
    public void testHandleIngestionRequestErrorDataTypeMismatch() {
        super.testHandleIngestionRequestErrorDataTypeMismatch();
    }

}