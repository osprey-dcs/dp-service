package com.ospreydcs.dp.service.ingest.handler.mongo;

import com.ospreydcs.dp.service.common.handler.QueueHandlerBase;
import com.ospreydcs.dp.service.ingest.handler.interfaces.IngestionHandlerInterface;
import com.ospreydcs.dp.service.ingest.handler.model.HandlerIngestionRequest;
import com.ospreydcs.dp.service.ingest.handler.mongo.job.IngestDataJob;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.*;

public class MongoIngestionHandler extends QueueHandlerBase implements IngestionHandlerInterface {

    private static final Logger logger = LogManager.getLogger();

    // configuration
    public static final String CFG_KEY_NUM_WORKERS = "IngestionHandler.numWorkers";
    public static final int DEFAULT_NUM_WORKERS = 7;

    final private MongoIngestionClientInterface mongoIngestionClient;

    public MongoIngestionHandler(MongoIngestionClientInterface client) {
        this.mongoIngestionClient = client;
    }

    public static MongoIngestionHandler newMongoSyncIngestionHandler() {
        return new MongoIngestionHandler(new MongoSyncIngestionClient());
    }

    public static MongoIngestionHandler newMongoAsyncIngestionHandler() {
        return new MongoIngestionHandler(new MongoAsyncIngestionClient());
    }

    protected int getNumWorkers_() {
        return configMgr().getConfigInteger(CFG_KEY_NUM_WORKERS, DEFAULT_NUM_WORKERS);
    }

    @Override
    protected boolean init_() {
        logger.trace("init_");
        if (!mongoIngestionClient.init()) {
            logger.error("error in mongoIngestionClient.init");
            return false;
        }
        return true;
    }

    @Override
    protected boolean fini_() {
        if (!mongoIngestionClient.fini()) {
            logger.error("error in mongoIngestionClient.fini");
        }
        return true;
    }

    @Override
    public void handleIngestDataStream(HandlerIngestionRequest handlerIngestionRequest) {

        final IngestDataJob job = new IngestDataJob(handlerIngestionRequest, mongoIngestionClient, this);

        logger.debug(
                "adding IngestDataJob id: {} provider: {} request: {}",
                handlerIngestionRequest.responseObserver.hashCode(),
                handlerIngestionRequest.request.getProviderId(),
                handlerIngestionRequest.request.getClientRequestId());

        try {
            requestQueue.put(job);
        } catch (InterruptedException e) {
            logger.error("InterruptedException waiting for requestQueue.put");
            Thread.currentThread().interrupt();
        }
    }

}
