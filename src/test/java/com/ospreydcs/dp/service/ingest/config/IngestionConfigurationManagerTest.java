package com.ospreydcs.dp.service.ingest.config;

import com.ospreydcs.dp.common.config.ConfigurationManager;
import com.ospreydcs.dp.service.ingest.benchmark.IngestionPerformanceBenchmark;
import com.ospreydcs.dp.service.ingest.handler.mongo.MongoHandlerBase;
import com.ospreydcs.dp.service.ingest.server.IngestionGrpcServer;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;

import static org.junit.Assert.assertTrue;

@FixMethodOrder(MethodSorters.NAME_ASCENDING)
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
    public void test01GrpcServer() {
        assertTrue("unexpected GrpcServer.port value",
                configMgr.getConfigInteger(IngestionGrpcServer.CFG_KEY_PORT)
                        == IngestionGrpcServer.DEFAULT_PORT);
    }

    @Test
    public void test02MongoHandler() {
        assertTrue("unexpected MongoHandler.numWorkers value",
                configMgr.getConfigInteger(MongoHandlerBase.CFG_KEY_NUM_WORKERS)
                        == MongoHandlerBase.DEFAULT_NUM_WORKERS);
        assertTrue("unexpected MongoHandler.dbHost value",
                configMgr.getConfigString(MongoHandlerBase.CFG_KEY_DB_HOST)
                        .equals(MongoHandlerBase.DEFAULT_DB_HOST));
        assertTrue("unexpected MongoHandler.dbPort value",
                configMgr.getConfigInteger(MongoHandlerBase.CFG_KEY_DB_PORT)
                        == (MongoHandlerBase.DEFAULT_DB_PORT));
        assertTrue("unexpected MongoHandler.dbUser value",
                configMgr.getConfigString(MongoHandlerBase.CFG_KEY_DB_USER)
                        .equals(MongoHandlerBase.DEFAULT_DB_USER));
        assertTrue("unexpected MongoHandler.dbPassword value",
                configMgr.getConfigString(MongoHandlerBase.CFG_KEY_DB_PASSWORD)
                        .equals(MongoHandlerBase.DEFAULT_DB_PASSWORD));
    }

    @Test
    public void test03Benchmark() {
        assertTrue("unexpected Benchmark.grpcConnectString value",
                configMgr.getConfigString(IngestionPerformanceBenchmark.CFG_KEY_GRPC_CONNECT_STRING)
                        .equals(IngestionPerformanceBenchmark.DEFAULT_GRPC_CONNECT_STRING));
    }

}
