package com.ospreydcs.dp.service.inprocess;

import com.ospreydcs.dp.client.MongoInterface;
import com.ospreydcs.dp.service.common.config.ConfigurationManager;
import io.grpc.ManagedChannel;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class InprocessServiceEcosystem {

    // static variables
    private static final Logger logger = LogManager.getLogger();

    // constants

    // instance variables
    protected MongoInterface mongoClient;
    public InprocessIngestionService ingestionService = new InprocessIngestionService();
    public InprocessQueryService queryService = new InprocessQueryService();
    public InprocessAnnotationService annotationService = new InprocessAnnotationService();
    public InprocessIngestionStreamService ingestionStreamService = new InprocessIngestionStreamService();

    protected static ConfigurationManager configMgr() {
        return ConfigurationManager.getInstance();
    }

    public boolean init() {

        logger.debug("InprocessGrpcServiceEcosystem init");

        // init the mongo client interface for db verification, globally changes database name to dp-test
        mongoClient = new MongoInterface();
        if (!mongoClient.init()) {
            return false;
        }

        // init ingestion service
        if (!ingestionService.init(mongoClient)) {
            return false;
        }
        ManagedChannel ingestionChannel = ingestionService.getIngestionChannel();

        // init query service
        if (!queryService.init(mongoClient)) {
            return false;
        }

        // init annotation service
        if (!annotationService.init(mongoClient)) {
            return false;
        }

        // init ingestion stream service
        if (!ingestionStreamService.init(mongoClient, ingestionChannel)) {
            return false;
        }

        return true;
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
