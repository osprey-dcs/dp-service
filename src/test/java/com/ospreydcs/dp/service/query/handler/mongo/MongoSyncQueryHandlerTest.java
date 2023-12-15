package com.ospreydcs.dp.service.query.handler.mongo;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

public class MongoSyncQueryHandlerTest extends MongoQueryHandlerTestBase {

    protected static class TestSyncClient extends MongoSyncQueryClient implements TestClientInterface {
    }

    @BeforeClass
    public static void setUp() throws Exception {
        TestSyncClient testClient = new TestSyncClient();
        MongoQueryHandler handler = new MongoQueryHandler(testClient);
        setUp(handler, testClient);
    }

    @AfterClass
    public static void tearDown() throws Exception {
        MongoQueryHandlerTestBase.tearDown();
    }

    @Test
    public void testProcessQueryRequestNoData() {
        super.testProcessQueryRequestNoData();
    }
}
