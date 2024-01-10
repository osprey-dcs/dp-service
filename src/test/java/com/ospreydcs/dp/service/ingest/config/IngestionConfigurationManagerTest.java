package com.ospreydcs.dp.service.ingest.config;

import com.ospreydcs.dp.common.config.ConfigurationManager;
import com.ospreydcs.dp.service.common.mongo.MongoClientBase;
import com.ospreydcs.dp.service.ingest.benchmark.IngestionBenchmarkBase;
import com.ospreydcs.dp.service.ingest.handler.mongo.MongoIngestionHandler;
import com.ospreydcs.dp.service.ingest.server.IngestionGrpcServer;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import static org.junit.Assert.assertTrue;

public class IngestionConfigurationManagerTest {

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
        assertTrue("unexpected value for resource: " + IngestionGrpcServer.CFG_KEY_PORT,
                configMgr.getConfigInteger(IngestionGrpcServer.CFG_KEY_PORT)
                        == IngestionGrpcServer.DEFAULT_PORT);
    }

    @Test
    public void testMongoIngestionHandler() {
        assertTrue("unexpected value for resource: " + MongoIngestionHandler.CFG_KEY_NUM_WORKERS,
                configMgr.getConfigInteger(MongoIngestionHandler.CFG_KEY_NUM_WORKERS)
                        == MongoIngestionHandler.DEFAULT_NUM_WORKERS);
    }

    @Test
    public void testMongoIngestionClient() {
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
        assertTrue("unexpected value for resource: " + IngestionBenchmarkBase.CFG_KEY_GRPC_CONNECT_STRING,
                configMgr.getConfigString(IngestionBenchmarkBase.CFG_KEY_GRPC_CONNECT_STRING)
                        .equals(IngestionBenchmarkBase.DEFAULT_GRPC_CONNECT_STRING));
    }

}
