package com.ospreydcs.dp.service.query.handler.mongo;

import com.mongodb.client.result.InsertManyResult;
import com.ospreydcs.dp.service.common.bson.bucket.BucketDocument;
import com.ospreydcs.dp.service.common.mongo.MongoTestClient;
import com.ospreydcs.dp.service.query.handler.mongo.client.MongoSyncQueryClient;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.Collection;
import java.util.Collections;
import java.util.List;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class MongoSyncQueryHandlerTest extends MongoQueryHandlerTestBase {

    protected static class TestSyncClient extends MongoSyncQueryClient implements TestClientInterface {

        @Override
        protected String getCollectionNameBuckets() {
            return getTestCollectionNameBuckets();
        }

        @Override
        protected String getCollectionNameRequestStatus() {
            return getTestCollectionNameRequestStatus();
        }

        public int insertBucketDocuments(List<BucketDocument> documentList) {
            InsertManyResult result = mongoCollectionBuckets.insertMany(documentList);
            return result.getInsertedIds().size();
        }
    }

    @BeforeClass
    public static void setUp() throws Exception {

        // Use test db client to set database name globally to "dp-test" and remove that database if it already exists
        MongoTestClient.prepareTestDatabase();

        TestSyncClient testClient = new TestSyncClient();
        MongoQueryHandler handler = new MongoQueryHandler(testClient);
        setUp(handler, testClient);
    }

    @AfterClass
    public static void tearDown() throws Exception {
        MongoQueryHandlerTestBase.tearDown();
    }

    @Test
    public void testResponseStreamDispatcher() {
        super.testResponseStreamDispatcher();
    }

    @Test
    public void testResponseCursorDispatcher() {
        super.testResponseCursorDispatcher();
    }

    /**
     * Verifies the null/empty short-circuit in executeQueryPvExistence: both null and empty input
     * return a non-null empty collection without hitting MongoDB (and without a null $in filter).
     * The regular callers guard against empty input upstream, so this branch is not exercised by
     * the integration tests.
     */
    @Test
    public void testExecuteQueryPvExistenceNullAndEmpty() {

        final Collection<String> nullResult = clientTestInterface.executeQueryPvExistence(null);
        assertNotNull("null pvNameList should return an empty collection, not null", nullResult);
        assertTrue("null pvNameList should return an empty collection", nullResult.isEmpty());

        final Collection<String> emptyResult =
                clientTestInterface.executeQueryPvExistence(Collections.emptyList());
        assertNotNull("empty pvNameList should return an empty collection, not null", emptyResult);
        assertTrue("empty pvNameList should return an empty collection", emptyResult.isEmpty());
    }
}
