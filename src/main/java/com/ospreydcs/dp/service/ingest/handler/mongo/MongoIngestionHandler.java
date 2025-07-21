package com.ospreydcs.dp.service.ingest.handler.mongo;

import com.ospreydcs.dp.grpc.v1.ingestion.*;
import com.ospreydcs.dp.service.common.handler.QueueHandlerBase;
import com.ospreydcs.dp.service.ingest.handler.interfaces.IngestionHandlerInterface;
import com.ospreydcs.dp.service.ingest.handler.model.HandlerIngestionRequest;
import com.ospreydcs.dp.service.ingest.handler.mongo.client.MongoIngestionClientInterface;
import com.ospreydcs.dp.service.ingest.handler.mongo.client.MongoSyncIngestionClient;
import com.ospreydcs.dp.service.ingest.handler.mongo.job.IngestDataJob;
import com.ospreydcs.dp.service.ingest.handler.mongo.job.QueryRequestStatusJob;
import com.ospreydcs.dp.service.ingest.handler.mongo.job.RegisterProviderJob;
import com.ospreydcs.dp.service.ingest.handler.mongo.job.SubscribeDataJob;
import com.ospreydcs.dp.service.ingest.model.SourceMonitor;
import com.ospreydcs.dp.service.query.handler.mongo.client.MongoQueryClientInterface;
import com.ospreydcs.dp.service.query.handler.mongo.client.MongoSyncQueryClient;
import io.grpc.stub.StreamObserver;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class MongoIngestionHandler extends QueueHandlerBase implements IngestionHandlerInterface {

    private static final Logger logger = LogManager.getLogger();

    // configuration

    public static final String CFG_KEY_NUM_WORKERS = "IngestionHandler.numWorkers";
    public static final int DEFAULT_NUM_WORKERS = 7;

    // instance variables

    final private MongoIngestionClientInterface mongoIngestionClient;
    final private MongoQueryClientInterface mongoQueryClient;
    final private SourceMonitorPublisher sourceMonitorPublisher = new SourceMonitorPublisher();

    public MongoIngestionHandler(
            MongoIngestionClientInterface mongoIngestionClient,
            MongoQueryClientInterface mongoQueryClient
    ) {
        this.mongoIngestionClient = mongoIngestionClient;
        this.mongoQueryClient = mongoQueryClient;
    }

    public static MongoIngestionHandler newMongoSyncIngestionHandler() {
        return new MongoIngestionHandler(new MongoSyncIngestionClient(), new MongoSyncQueryClient());
    }

//    public static MongoIngestionHandler newMongoAsyncIngestionHandler() {
//        return new MongoIngestionHandler(new MongoAsyncIngestionClient());
//    }
//
    protected int getNumWorkers_() {
        return configMgr().getConfigInteger(CFG_KEY_NUM_WORKERS, DEFAULT_NUM_WORKERS);
    }

    public SourceMonitorPublisher getSourceMonitorPublisher() {
        return sourceMonitorPublisher;
    }

    @Override
    protected boolean init_() {
        logger.trace("init_");
        if (!mongoIngestionClient.init()) {
            logger.error("error in mongoIngestionClient.init");
            return false;
        }
        if (!mongoQueryClient.init()) {
            logger.error("error in mongoQueryClient.init");
            return false;
        }
        if (!sourceMonitorPublisher.init()) {
            logger.error("error in SourceMonitorPublisher.init");
            return false;
        }
        return true;
    }

    @Override
    protected boolean fini_() {
        if (!sourceMonitorPublisher.fini()) {
            logger.error("error in SourceMonitorPublisher.fini");
        }
        if (!mongoQueryClient.fini()) {
            logger.error("error in MongoQueryClient.fini");
        }
        if (!mongoIngestionClient.fini()) {
            logger.error("error in mongoIngestionClient.fini");
        }
        return true;
    }

    @Override
    public void handleRegisterProvider(
            RegisterProviderRequest request,
            StreamObserver<RegisterProviderResponse> responseObserver
    ) {
        final RegisterProviderJob job = new RegisterProviderJob(
                request, responseObserver, mongoIngestionClient, this);

        logger.debug("adding RegisterProviderJob id: {} to queue", responseObserver.hashCode());

        try {
            requestQueue.put(job);
        } catch (InterruptedException e) {
            logger.error("InterruptedException waiting for requestQueue.put");
            Thread.currentThread().interrupt();
        }

    }

    @Override
    public void handleIngestionRequest(HandlerIngestionRequest handlerIngestionRequest) {

        final IngestDataJob job = new IngestDataJob(handlerIngestionRequest, mongoIngestionClient, this);

        logger.debug(
                "adding IngestDataJob id: {} provider: {} request: {}",
                job.hashCode(),
                handlerIngestionRequest.request.getProviderId(),
                handlerIngestionRequest.request.getClientRequestId());

        try {
            requestQueue.put(job);
        } catch (InterruptedException e) {
            logger.error("InterruptedException waiting for requestQueue.put");
            Thread.currentThread().interrupt();
        }
    }

    @Override
    public void handleQueryRequestStatus(
            QueryRequestStatusRequest request,
            StreamObserver<QueryRequestStatusResponse> responseObserver
    ) {
        final QueryRequestStatusJob job =
                new QueryRequestStatusJob(request, responseObserver, mongoIngestionClient);

        logger.debug("adding QueryRequestStatusJob id: {} to queue", responseObserver.hashCode());

        try {
            requestQueue.put(job);
        } catch (InterruptedException e) {
            logger.error("InterruptedException waiting for requestQueue.put");
            Thread.currentThread().interrupt();
        }
    }

    @Override
    public void handleSubscribeData(
            SubscribeDataRequest request, StreamObserver<SubscribeDataResponse> responseObserver, SourceMonitor monitor
    ) {
        final SubscribeDataJob job =
                new SubscribeDataJob(
                        request, 
                        responseObserver, 
                        monitor,
                        sourceMonitorPublisher,
                        mongoIngestionClient,
                        mongoQueryClient);

        logger.debug(
                "adding SubscribeDataJob id: {} to queue",
                monitor.responseObserver.hashCode());

        try {
            requestQueue.put(job);
        } catch (InterruptedException e) {
            logger.error("InterruptedException waiting for requestQueue.put");
            Thread.currentThread().interrupt();
        }
    }

    @Override
    public void removeSourceMonitor(SourceMonitor monitor) {
        logger.debug(
                "cancelDataSubscriptions removing subscriptions for id: {}",
                monitor.responseObserver.hashCode());
        this.sourceMonitorPublisher.removeMonitor(monitor);
    }
}
