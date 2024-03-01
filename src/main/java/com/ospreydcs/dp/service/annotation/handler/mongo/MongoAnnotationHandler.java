package com.ospreydcs.dp.service.annotation.handler.mongo;

import com.ospreydcs.dp.grpc.v1.annotation.CreateAnnotationRequest;
import com.ospreydcs.dp.grpc.v1.annotation.CreateAnnotationResponse;
import com.ospreydcs.dp.service.annotation.handler.interfaces.AnnotationHandlerInterface;
import com.ospreydcs.dp.service.annotation.handler.mongo.client.MongoAnnotationClientInterface;
import com.ospreydcs.dp.service.annotation.handler.mongo.client.MongoSyncAnnotationClient;
import com.ospreydcs.dp.service.annotation.handler.mongo.job.CreateCommentAnnotationJob;
import com.ospreydcs.dp.service.common.handler.QueueHandlerBase;
import io.grpc.stub.StreamObserver;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class MongoAnnotationHandler extends QueueHandlerBase implements AnnotationHandlerInterface {

    // static variables
    private static final Logger logger = LogManager.getLogger();

    // configuration
    public static final String CFG_KEY_NUM_WORKERS = "AnnotationHandler.numWorkers";
    public static final int DEFAULT_NUM_WORKERS = 7;

    // instance variables
    private final MongoAnnotationClientInterface mongoClient;

    public MongoAnnotationHandler(MongoAnnotationClientInterface clientInterface) {
        this.mongoClient = clientInterface;
    }

    public static MongoAnnotationHandler newMongoSyncAnnotationHandler() {
        return new MongoAnnotationHandler(new MongoSyncAnnotationClient());
    }

    protected int getNumWorkers_() {
        return configMgr().getConfigInteger(CFG_KEY_NUM_WORKERS, DEFAULT_NUM_WORKERS);
    }

    @Override
    protected boolean init_() {
        logger.trace("init_");
        if (!mongoClient.init()) {
            logger.error("error in mongoClient.init()");
            return false;
        }
        return true;
    }

    @Override
    protected boolean fini_() {
        if (!mongoClient.fini()) {
            logger.error("error in mongoClient.fini()");
        }
        return true;
    }

    @Override
    public void handleCreateCommentAnnotationRequest(
            CreateAnnotationRequest request,
            StreamObserver<CreateAnnotationResponse> responseObserver
    ) {
        final CreateCommentAnnotationJob job = new CreateCommentAnnotationJob(
                request,
                responseObserver,
                mongoClient);

        logger.debug("adding CreateCommentAnnotationJob id: {} to queue", responseObserver.hashCode());

        try {
            requestQueue.put(job);
        } catch (InterruptedException e) {
            logger.error("InterruptedException waiting for requestQueue.put");
            Thread.currentThread().interrupt();
        }
    }

}
