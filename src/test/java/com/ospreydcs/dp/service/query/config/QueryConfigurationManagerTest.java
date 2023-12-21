package com.ospreydcs.dp.service.query.config;

import com.ospreydcs.dp.common.config.ConfigurationManager;
import com.ospreydcs.dp.service.common.mongo.MongoClientBase;
import com.ospreydcs.dp.service.query.benchmark.QueryPerformanceBenchmark;
import com.ospreydcs.dp.service.query.handler.mongo.MongoQueryHandler;
import com.ospreydcs.dp.service.query.server.QueryGrpcServer;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import static org.junit.Assert.assertTrue;

public class QueryConfigurationManagerTest {

    private static ConfigurationManager configMgr;

    @BeforeClass
    public static void setUp() {
        configMgr = ConfigurationManager.getInstance();
    }

    @AfterClass
    public static void tearDown() {
        configMgr = null;
    }

    @Test
    public void testGrpcServer() {
        assertTrue("unexpected value for resource: " + QueryGrpcServer.CFG_KEY_PORT,
                configMgr.getConfigInteger(QueryGrpcServer.CFG_KEY_PORT)
                        == QueryGrpcServer.DEFAULT_PORT);
    }

    @Test
    public void testMongoQueryHandler() {
        assertTrue("unexpected value for resource: " + MongoQueryHandler.CFG_KEY_NUM_WORKERS,
                configMgr.getConfigInteger(MongoQueryHandler.CFG_KEY_NUM_WORKERS)
                        == MongoQueryHandler.DEFAULT_NUM_WORKERS);
    }

    @Test
    public void testMongoQueryClient() {
        assertTrue("unexpected value for resource: " + MongoClientBase.CFG_KEY_DB_HOST,
                configMgr.getConfigString(MongoClientBase.CFG_KEY_DB_HOST)
                        .equals(MongoClientBase.DEFAULT_DB_HOST));
        assertTrue("unexpected value for resource: " + MongoClientBase.CFG_KEY_DB_PORT,
                configMgr.getConfigInteger(MongoClientBase.CFG_KEY_DB_PORT)
                        == (MongoClientBase.DEFAULT_DB_PORT));
        assertTrue("unexpected value for resource: " + MongoClientBase.CFG_KEY_DB_USER,
                configMgr.getConfigString(MongoClientBase.CFG_KEY_DB_USER)
                        .equals(MongoClientBase.DEFAULT_DB_USER));
        assertTrue("unexpected value for resource: " + MongoClientBase.CFG_KEY_DB_PASSWORD,
                configMgr.getConfigString(MongoClientBase.CFG_KEY_DB_PASSWORD)
                        .equals(MongoClientBase.DEFAULT_DB_PASSWORD));
    }

    @Test
    public void testBenchmark() {
        assertTrue("unexpected value for resource: " + QueryPerformanceBenchmark.CFG_KEY_GRPC_CONNECT_STRING,
                configMgr.getConfigString(QueryPerformanceBenchmark.CFG_KEY_GRPC_CONNECT_STRING)
                        .equals(QueryPerformanceBenchmark.DEFAULT_GRPC_CONNECT_STRING));
    }

}
