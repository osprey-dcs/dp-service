package com.ospreydcs.dp.service.inprocess;

import com.ospreydcs.dp.client.mongo.MongoDemoClient;
import com.ospreydcs.dp.service.common.config.ConfigurationManager;
import io.grpc.ManagedChannel;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public abstract class InprocessServiceEcosystem {

    // static variables
    private static final Logger logger = LogManager.getLogger();

    // constants

    // instance variables
    protected MongoDemoClient mongoClient;
    protected InprocessIngestionService ingestionService = new InprocessIngestionService();
    protected InprocessQueryService queryService = new InprocessQueryService();
    protected InprocessAnnotationService annotationService = new InprocessAnnotationService();
    protected InprocessIngestionStreamService ingestionStreamService = new InprocessIngestionStreamService();

    protected static ConfigurationManager configMgr() {
        return ConfigurationManager.getInstance();
    }

    public void init() {

        logger.debug("InprocessGrpcServiceEcosystem init");

        // init the mongo client interface for db verification, globally changes database name to dp-test
        mongoClient = new MongoDemoClient();
        mongoClient.init();

        // init ingestion service
        ingestionService.init(mongoClient);
        ManagedChannel ingestionChannel = ingestionService.getIngestionChannel();

        // init query service
        queryService.init(mongoClient);

        // init annotation service
        annotationService.init(mongoClient);

        // init ingestion stream service
        ingestionStreamService.init(mongoClient, ingestionChannel);
    }

    public void fini() {

        logger.debug("InprocessGrpcServiceEcosystem tearDown");

        ingestionStreamService.fini();
        annotationService.fini();
        queryService.fini();
        ingestionService.fini();

        mongoClient.fini();
        mongoClient = null;
    }

}
