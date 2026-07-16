package com.ospreydcs.dp.service.query.handler.mongo;

import com.ospreydcs.dp.grpc.v1.query.*;
import com.ospreydcs.dp.service.common.handler.QueueHandlerBase;
import com.ospreydcs.dp.service.common.model.ResultStatus;
import com.ospreydcs.dp.service.query.handler.QueryHandlerUtility;
import com.ospreydcs.dp.service.query.handler.QueryV2Resolver;
import com.ospreydcs.dp.service.query.handler.interfaces.QueryHandlerInterface;
import com.ospreydcs.dp.service.query.handler.model.ResolutionResult;
import com.ospreydcs.dp.service.query.handler.model.ResolvedQuery;
import com.ospreydcs.dp.service.query.handler.mongo.client.MongoQueryClientInterface;
import com.ospreydcs.dp.service.query.handler.mongo.client.MongoSyncQueryClient;
import com.ospreydcs.dp.service.query.handler.mongo.dispatch.*;
import com.ospreydcs.dp.service.query.handler.mongo.job.*;
import com.ospreydcs.dp.service.query.service.QueryServiceImpl;
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

    // Query API V2 paging / resolution limits (Q7/Q10)
    private static final String CFG_KEY_QUERY_V2_DEFAULT_PAGE_SIZE = "QueryHandler.queryV2DefaultPageSize";
    private static final int DEFAULT_QUERY_V2_DEFAULT_PAGE_SIZE = 10_000;
    private static final String CFG_KEY_QUERY_V2_MAX_PAGE_SIZE = "QueryHandler.queryV2MaxPageSize";
    private static final int DEFAULT_QUERY_V2_MAX_PAGE_SIZE = 100_000;
    private static final String CFG_KEY_QUERY_V2_MAX_RESOLVED_PV_COUNT = "QueryHandler.queryV2MaxResolvedPvCount";
    private static final int DEFAULT_QUERY_V2_MAX_RESOLVED_PV_COUNT = 10_000;

    // instance variables
    private final MongoQueryClientInterface mongoQueryClient;
    private final QueryV2Resolver queryV2Resolver;

    public MongoQueryHandler(MongoQueryClientInterface clientInterface) {
        this.mongoQueryClient = clientInterface;
        this.queryV2Resolver = new QueryV2Resolver(
                clientInterface,
                configMgr().getConfigInteger(CFG_KEY_QUERY_V2_DEFAULT_PAGE_SIZE, DEFAULT_QUERY_V2_DEFAULT_PAGE_SIZE),
                configMgr().getConfigInteger(CFG_KEY_QUERY_V2_MAX_PAGE_SIZE, DEFAULT_QUERY_V2_MAX_PAGE_SIZE),
                configMgr().getConfigInteger(
                        CFG_KEY_QUERY_V2_MAX_RESOLVED_PV_COUNT, DEFAULT_QUERY_V2_MAX_RESOLVED_PV_COUNT));
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
    public ResultStatus validateQuerySpecData(QueryDataRequest.QuerySpec querySpec) {
        return QueryHandlerUtility.validateQuerySpecData(querySpec);
    }

    @Override
    public ResultStatus validateQueryTableRequest(QueryTableRequest request) {
        return QueryHandlerUtility.validateQueryTableRequest(request);
    }

    @Override
    public void handleQueryDataStream(
            QueryDataRequest.QuerySpec querySpec, StreamObserver<QueryDataResponse> responseObserver) {

        final QueryDataStreamDispatcher dispatcher = new QueryDataStreamDispatcher(responseObserver, querySpec);
        final QueryDataJob job = new QueryDataJob(querySpec, dispatcher, responseObserver, mongoQueryClient);

        logger.debug(
                "handleQueryDataStream() adding QueryDataJob id: {}",
                responseObserver.hashCode());

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


        final QueryDataBidiStreamDispatcher dispatcher = new QueryDataBidiStreamDispatcher(responseObserver, querySpec);
        final QueryDataJob job = new QueryDataJob(querySpec, dispatcher, responseObserver, mongoQueryClient);
        final QueryResultCursor resultCursor = new QueryResultCursor(this, dispatcher);

        logger.debug(
                "handleQueryDataBidiStream() adding QueryDataJob id: {}",
                responseObserver.hashCode());

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

        final QueryDataDispatcher dispatcher = new QueryDataDispatcher(responseObserver, querySpec);
        final QueryDataJob job = new QueryDataJob(querySpec, dispatcher, responseObserver, mongoQueryClient);

        logger.debug(
                "handleQueryData() adding QueryDataJob id: {}",
                responseObserver.hashCode());

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
    public void handleQueryPvStats(
            QueryPvStatsRequest request,
            StreamObserver<QueryPvStatsResponse> responseObserver
    ) {
        final QueryPvStatsJob job =
                new QueryPvStatsJob(request, responseObserver, mongoQueryClient);

        logger.debug("adding QueryPvStatsJob id: {} to queue", responseObserver.hashCode());

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
    public void handleQueryProviderStats(
            QueryProviderStatsRequest request,
            StreamObserver<QueryProviderStatsResponse> responseObserver
    ) {
        final QueryProviderStatsJob job =
                new QueryProviderStatsJob(request, responseObserver, mongoQueryClient);

        logger.debug("adding QueryProviderStatsJob id: {} to queue", responseObserver.hashCode());

        try {
            requestQueue.put(job);
        } catch (InterruptedException e) {
            logger.error("InterruptedException waiting for requestQueue.put");
            Thread.currentThread().interrupt();
        }
    }

    @Override
    public void handleQueryBuckets(
            QueryBucketsRequest request,
            StreamObserver<QueryBucketsResponse> responseObserver
    ) {
        final ResolvedQuery resolvedQuery = resolveBucketsOrReject(request, false, responseObserver);
        if (resolvedQuery == null) {
            return; // reject already sent
        }
        final QueryBucketsUnaryDispatcher dispatcher = new QueryBucketsUnaryDispatcher(responseObserver);
        enqueueQueryV2Job(resolvedQuery, dispatcher, "queryBuckets", responseObserver.hashCode());
    }

    @Override
    public void handleQueryBucketsStream(
            QueryBucketsRequest request,
            StreamObserver<QueryBucketsResponse> responseObserver
    ) {
        final ResolvedQuery resolvedQuery = resolveBucketsOrReject(request, true, responseObserver);
        if (resolvedQuery == null) {
            return; // reject already sent (includes the non-empty-pageToken streaming rule)
        }
        final QueryBucketsStreamDispatcher dispatcher = new QueryBucketsStreamDispatcher(responseObserver);
        enqueueQueryV2Job(resolvedQuery, dispatcher, "queryBucketsStream", responseObserver.hashCode());
    }

    /**
     * Validates + resolves a bucket request (§6 invariants, PV/config resolution, paging
     * normalization). On error, sends an ExceptionalResult reject and returns null; otherwise returns
     * the ResolvedQuery. The {@code streaming} flag drives the paging-token rule (Q7).
     */
    private ResolvedQuery resolveBucketsOrReject(
            QueryBucketsRequest request, boolean streaming,
            StreamObserver<QueryBucketsResponse> responseObserver) {

        final ResolutionResult resolution = queryV2Resolver.resolve(
                request.getQuerySpec(),
                request.getExecutionOptions(),
                request.getResultRepresentation(),
                ResolvedQuery.ResultMode.BUCKET,
                streaming);

        if (resolution.isError()) {
            QueryServiceImpl.sendQueryBucketsResponseReject(
                    resolution.getErrorStatus().msg, responseObserver);
            return null;
        }
        return resolution.getResolvedQuery();
    }

    @Override
    public void handleQuerySamples(
            QuerySamplesRequest request,
            StreamObserver<QuerySamplesResponse> responseObserver
    ) {
        final ResolvedQuery resolvedQuery = resolveSamplesOrReject(request, false, responseObserver);
        if (resolvedQuery == null) {
            return; // reject already sent
        }
        final QuerySamplesUnaryDispatcher dispatcher = new QuerySamplesUnaryDispatcher(responseObserver);
        enqueueQueryV2Job(resolvedQuery, dispatcher, "querySamples", responseObserver.hashCode());
    }

    @Override
    public void handleQuerySamplesStream(
            QuerySamplesRequest request,
            StreamObserver<QuerySamplesResponse> responseObserver
    ) {
        final ResolvedQuery resolvedQuery = resolveSamplesOrReject(request, true, responseObserver);
        if (resolvedQuery == null) {
            return; // reject already sent (includes the non-empty-pageToken streaming rule)
        }
        final QuerySamplesStreamDispatcher dispatcher = new QuerySamplesStreamDispatcher(responseObserver);
        enqueueQueryV2Job(resolvedQuery, dispatcher, "querySamplesStream", responseObserver.hashCode());
    }

    /**
     * Validates + resolves a sample request (mode=SAMPLE). On error, sends an ExceptionalResult
     * reject and returns null; otherwise returns the ResolvedQuery. The {@code streaming} flag drives
     * the paging-token rule (Q7).
     */
    private ResolvedQuery resolveSamplesOrReject(
            QuerySamplesRequest request, boolean streaming,
            StreamObserver<QuerySamplesResponse> responseObserver) {

        final ResolutionResult resolution = queryV2Resolver.resolve(
                request.getQuerySpec(),
                request.getExecutionOptions(),
                request.getResultRepresentation(),
                ResolvedQuery.ResultMode.SAMPLE,
                streaming);

        if (resolution.isError()) {
            QueryServiceImpl.sendQuerySamplesResponseReject(
                    resolution.getErrorStatus().msg, responseObserver);
            return null;
        }
        return resolution.getResolvedQuery();
    }

    private void enqueueQueryV2Job(
            ResolvedQuery resolvedQuery,
            com.ospreydcs.dp.service.query.handler.mongo.dispatch.QueryV2Dispatcher dispatcher,
            String label, int observerId) {

        final QueryV2Job job = new QueryV2Job(resolvedQuery, dispatcher, mongoQueryClient);
        logger.debug("adding QueryV2Job ({}) id: {} to queue", label, observerId);
        try {
            requestQueue.put(job);
        } catch (InterruptedException e) {
            logger.error("InterruptedException waiting for requestQueue.put");
            Thread.currentThread().interrupt();
        }
    }

}
