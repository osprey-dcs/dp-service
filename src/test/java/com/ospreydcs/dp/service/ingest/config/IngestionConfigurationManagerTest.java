package com.ospreydcs.dp.service.ingest.config;

import com.ospreydcs.dp.common.config.ConfigurationManager;
import com.ospreydcs.dp.service.common.mongo.MongoClientBase;
import com.ospreydcs.dp.service.ingest.benchmark.IngestionPerformanceBenchmark;
import com.ospreydcs.dp.service.ingest.handler.mongo.MongoIngestionHandler;
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
    public void test02MongoIngestionHandler() {
        assertTrue("unexpected MongoHandler.numWorkers value",
                configMgr.getConfigInteger(MongoIngestionHandler.CFG_KEY_NUM_WORKERS)
                        == MongoIngestionHandler.DEFAULT_NUM_WORKERS);
    }

    @Test
    public void test03MongoIngestionClient() {
        assertTrue("unexpected MongoHandler.dbHost value",
                configMgr.getConfigString(MongoClientBase.CFG_KEY_DB_HOST)
                        .equals(MongoClientBase.DEFAULT_DB_HOST));
        assertTrue("unexpected MongoHandler.dbPort value",
                configMgr.getConfigInteger(MongoClientBase.CFG_KEY_DB_PORT)
                        == (MongoClientBase.DEFAULT_DB_PORT));
        assertTrue("unexpected MongoHandler.dbUser value",
                configMgr.getConfigString(MongoClientBase.CFG_KEY_DB_USER)
                        .equals(MongoClientBase.DEFAULT_DB_USER));
        assertTrue("unexpected MongoHandler.dbPassword value",
                configMgr.getConfigString(MongoClientBase.CFG_KEY_DB_PASSWORD)
                        .equals(MongoClientBase.DEFAULT_DB_PASSWORD));
    }

    @Test
    public void test04Benchmark() {
        assertTrue("unexpected Benchmark.grpcConnectString value",
                configMgr.getConfigString(IngestionPerformanceBenchmark.CFG_KEY_GRPC_CONNECT_STRING)
                        .equals(IngestionPerformanceBenchmark.DEFAULT_GRPC_CONNECT_STRING));
    }

}
