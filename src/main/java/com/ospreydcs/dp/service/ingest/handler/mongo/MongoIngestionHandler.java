package com.ospreydcs.dp.service.ingest.handler.mongo;

import com.ospreydcs.dp.grpc.v1.ingestion.*;
import com.ospreydcs.dp.service.common.handler.QueueHandlerBase;
import com.ospreydcs.dp.service.ingest.handler.interfaces.IngestionHandlerInterface;
import com.ospreydcs.dp.service.ingest.handler.model.HandlerIngestionRequest;
import com.ospreydcs.dp.service.ingest.handler.mongo.client.MongoAsyncIngestionClient;
import com.ospreydcs.dp.service.ingest.handler.mongo.client.MongoIngestionClientInterface;
import com.ospreydcs.dp.service.ingest.handler.mongo.client.MongoSyncIngestionClient;
import com.ospreydcs.dp.service.ingest.handler.mongo.job.IngestDataJob;
import com.ospreydcs.dp.service.ingest.handler.mongo.job.QueryRequestStatusJob;
import com.ospreydcs.dp.service.ingest.handler.mongo.job.RegisterProviderJob;
import com.ospreydcs.dp.service.ingest.service.IngestionServiceImpl;
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
    final private DataSubscriptionManager subscriptionManager = new DataSubscriptionManager();

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

    public DataSubscriptionManager getSubscriptionManager() {
        return subscriptionManager;
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
            SubscribeDataRequest request,
            StreamObserver<SubscribeDataResponse> responseObserver
    ) {
        logger.debug("handleSubscribeData adding subscription for id: {}", responseObserver.hashCode());
        this.subscriptionManager.addSubscription(request.getPvNamesList(), responseObserver);
        IngestionServiceImpl.sendSubscribeDataResponseAck(responseObserver);
    }

    @Override
    public void cancelDataSubscriptions(StreamObserver<SubscribeDataResponse> responseObserver) {
        logger.debug("cancelDataSubscriptions removing subscriptions for id: {}", responseObserver.hashCode());
        this.subscriptionManager.removeSubscriptions(responseObserver);
    }
}
