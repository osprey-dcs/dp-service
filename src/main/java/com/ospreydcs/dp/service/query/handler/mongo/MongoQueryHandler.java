package com.ospreydcs.dp.service.query.handler.mongo;

import com.ospreydcs.dp.grpc.v1.query.*;
import com.ospreydcs.dp.service.common.handler.QueueHandlerBase;
import com.ospreydcs.dp.service.common.model.ValidationResult;
import com.ospreydcs.dp.service.query.handler.QueryHandlerUtility;
import com.ospreydcs.dp.service.query.handler.interfaces.QueryHandlerInterface;
import com.ospreydcs.dp.service.query.handler.mongo.client.MongoQueryClientInterface;
import com.ospreydcs.dp.service.query.handler.mongo.client.MongoSyncQueryClient;
import com.ospreydcs.dp.service.query.handler.mongo.dispatch.*;
import com.ospreydcs.dp.service.query.handler.mongo.job.*;
import io.grpc.stub.StreamObserver;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class MongoQueryHandler extends QueueHandlerBase implements QueryHandlerInterface {

    // static variables
    private static final Logger logger = LogManager.getLogger();

    // configuration
    public static final String CFG_KEY_NUM_WORKERS = "QueryHandler.numWorkers";
    public static final int DEFAULT_NUM_WORKERS = 7;
    private static final String CFG_KEY_OUTGOING_MESSAGE_SIZE_LIMIT_BYTES = "GrpcServer.incomingMessageSizeLimitBytes";
    private static final int DEFAULT_OUTGOING_MESSAGE_SIZE_LIMIT_BYTES = 4_096_000;

    // instance variables
    private final MongoQueryClientInterface mongoQueryClient;

    public MongoQueryHandler(MongoQueryClientInterface clientInterface) {
        this.mongoQueryClient = clientInterface;
    }

    public static MongoQueryHandler newMongoSyncQueryHandler() {
        return new MongoQueryHandler(new MongoSyncQueryClient());
    }

    protected int getNumWorkers_() {
        return configMgr().getConfigInteger(CFG_KEY_NUM_WORKERS, DEFAULT_NUM_WORKERS);
    }

    public static int getOutgoingMessageSizeLimitBytes() {
        return configMgr().getConfigInteger(
                CFG_KEY_OUTGOING_MESSAGE_SIZE_LIMIT_BYTES,
                DEFAULT_OUTGOING_MESSAGE_SIZE_LIMIT_BYTES);
    }

    @Override
    protected boolean init_() {
        logger.trace("init_");
        if (!mongoQueryClient.init()) {
            logger.error("error in mongoQueryClient.init()");
            return false;
        }
        return true;
    }

    @Override
    protected boolean fini_() {
        if (!mongoQueryClient.fini()) {
            logger.error("error in mongoQueryClient.fini()");
        }
        return true;
    }

    @Override
    public ValidationResult validateQuerySpecData(QueryDataRequest.QuerySpec querySpec) {
        return QueryHandlerUtility.validateQuerySpecData(querySpec);
    }

    @Override
    public ValidationResult validateQueryTableRequest(QueryTableRequest request) {
        return QueryHandlerUtility.validateQueryTableRequest(request);
    }

    @Override
    public void handleQueryDataStream(
            QueryDataRequest.QuerySpec querySpec, StreamObserver<QueryDataResponse> responseObserver) {

        final QueryDataStreamDispatcher dispatcher = new QueryDataStreamDispatcher(responseObserver);
        final QueryDataJob job = new QueryDataJob(querySpec, dispatcher, responseObserver, mongoQueryClient);

        logger.debug("adding queryResponseStream job id: {} to queue", responseObserver.hashCode());

        try {
            requestQueue.put(job);
        } catch (InterruptedException e) {
            logger.error("InterruptedException waiting for requestQueue.put");
            Thread.currentThread().interrupt();
        }
    }

    @Override
    public QueryResultCursor handleQueryDataBidiStream(
            QueryDataRequest.QuerySpec querySpec, StreamObserver<QueryDataResponse> responseObserver) {


        final QueryDataBidiStreamDispatcher dispatcher = new QueryDataBidiStreamDispatcher(responseObserver);
        final QueryDataJob job = new QueryDataJob(querySpec, dispatcher, responseObserver, mongoQueryClient);
        final QueryResultCursor resultCursor = new QueryResultCursor(this, dispatcher);

        logger.debug("adding queryResponseCursor job id: {} to queue", responseObserver.hashCode());

        try {
            requestQueue.put(job);
        } catch (InterruptedException e) {
            logger.error("InterruptedException waiting for requestQueue.put");
            Thread.currentThread().interrupt();
        }

        return resultCursor;
    }

    @Override
    public void handleQueryData(
            QueryDataRequest.QuerySpec querySpec, StreamObserver<QueryDataResponse> responseObserver) {

        final QueryDataDispatcher dispatcher = new QueryDataDispatcher(responseObserver);
        final QueryDataJob job = new QueryDataJob(querySpec, dispatcher, responseObserver, mongoQueryClient);

        logger.debug("adding queryResponseSingle job id: {} to queue", responseObserver.hashCode());

        try {
            requestQueue.put(job);
        } catch (InterruptedException e) {
            logger.error("InterruptedException waiting for requestQueue.put");
            Thread.currentThread().interrupt();
        }
    }

    @Override
    public void handleQueryTable(
            QueryTableRequest request, StreamObserver<QueryTableResponse> responseObserver) {

        final QueryTableJob job = new QueryTableJob(request, responseObserver, mongoQueryClient);

        logger.debug("adding queryResponseTable job id: {} to queue", responseObserver.hashCode());

        try {
            requestQueue.put(job);
        } catch (InterruptedException e) {
            logger.error("InterruptedException waiting for requestQueue.put");
            Thread.currentThread().interrupt();
        }
    }

    @Override
    public void handleQueryPvMetadata(
            QueryPvMetadataRequest request,
            StreamObserver<QueryPvMetadataResponse> responseObserver
    ) {
        final QueryPvMetadataJob job =
                new QueryPvMetadataJob(request, responseObserver, mongoQueryClient);

        logger.debug("adding queryMetadata job id: {} to queue", responseObserver.hashCode());

        try {
            requestQueue.put(job);
        } catch (InterruptedException e) {
            logger.error("InterruptedException waiting for requestQueue.put");
            Thread.currentThread().interrupt();
        }
    }

    @Override
    public void handleQueryProviders(
            QueryProvidersRequest request,
            StreamObserver<QueryProvidersResponse> responseObserver
    ) {
        final QueryProvidersJob job =
                new QueryProvidersJob(request, responseObserver, mongoQueryClient);

        logger.debug("adding QueryProvidersJob id: {} to queue", responseObserver.hashCode());

        try {
            requestQueue.put(job);
        } catch (InterruptedException e) {
            logger.error("InterruptedException waiting for requestQueue.put");
            Thread.currentThread().interrupt();
        }
    }

    @Override
    public void handleQueryProviderMetadata(
            QueryProviderMetadataRequest request, 
            StreamObserver<QueryProviderMetadataResponse> responseObserver
    ) {
        final QueryProviderMetadataJob job =
                new QueryProviderMetadataJob(request, responseObserver, mongoQueryClient);

        logger.debug("adding QueryProviderMetadataJob id: {} to queue", responseObserver.hashCode());

        try {
            requestQueue.put(job);
        } catch (InterruptedException e) {
            logger.error("InterruptedException waiting for requestQueue.put");
            Thread.currentThread().interrupt();
        }
    }

}
