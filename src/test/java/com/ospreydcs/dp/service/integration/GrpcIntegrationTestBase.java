package com.ospreydcs.dp.service.integration;

import com.ospreydcs.dp.service.common.config.ConfigurationManager;
import com.ospreydcs.dp.service.common.mongo.MongoTestClient;
import com.ospreydcs.dp.service.integration.annotation.GrpcIntegrationAnnotationServiceWrapper;
import com.ospreydcs.dp.service.integration.ingest.GrpcIntegrationIngestionServiceWrapper;
import com.ospreydcs.dp.service.integration.ingestionstream.GrpcIntegrationIngestionStreamServiceWrapper;
import com.ospreydcs.dp.service.integration.query.GrpcIntegrationQueryServiceWrapper;
import io.grpc.ManagedChannel;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import static org.mockito.Mockito.mock;

public abstract class GrpcIntegrationTestBase {

    // static variables
    private static final Logger logger = LogManager.getLogger();

    // constants
    public static final String CFG_KEY_START_SECONDS = "IngestionBenchmark.startSeconds";
    public static final Long DEFAULT_START_SECONDS = 1698767462L;

    // instance variables
    protected MongoTestClient mongoClient;
    protected GrpcIntegrationIngestionServiceWrapper ingestionServiceWrapper =
            new GrpcIntegrationIngestionServiceWrapper();
    protected GrpcIntegrationQueryServiceWrapper queryServiceWrapper =
            new GrpcIntegrationQueryServiceWrapper();
    protected GrpcIntegrationAnnotationServiceWrapper annotationServiceWrapper =
            new GrpcIntegrationAnnotationServiceWrapper();
    protected GrpcIntegrationIngestionStreamServiceWrapper ingestionStreamServiceWrapper =
            new GrpcIntegrationIngestionStreamServiceWrapper();

    protected static ConfigurationManager configMgr() {
        return ConfigurationManager.getInstance();
    }

    public void setUp() throws Exception {

        // init the mongo client interface for db verification, globally changes database name to dp-test
        mongoClient = new MongoTestClient();
        mongoClient.init();

        // init ingestion service
        ingestionServiceWrapper.init(mongoClient);
        ManagedChannel ingestionChannel = ingestionServiceWrapper.getIngestionChannel();

        // init query service
        queryServiceWrapper.init(mongoClient);

        // init annotation service
        annotationServiceWrapper.init(mongoClient);

        // init ingestion stream service
        ingestionStreamServiceWrapper.init(mongoClient, ingestionChannel);
    }

    public void tearDown() {

        logger.debug("GrpcIntegrationTestBase tearDown");

        ingestionStreamServiceWrapper.fini();
        annotationServiceWrapper.fini();
        queryServiceWrapper.fini();
        ingestionServiceWrapper.fini();

        mongoClient.fini();
        mongoClient = null;
    }

}
