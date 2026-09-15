package com.ospreydcs.dp.service.query.handler.mongo;

import com.ospreydcs.dp.grpc.v1.query.*;
import com.ospreydcs.dp.service.common.handler.QueueHandlerBase;
import com.ospreydcs.dp.service.common.model.ResultStatus;
import com.ospreydcs.dp.service.query.handler.QueryHandlerUtility;
import com.ospreydcs.dp.service.query.handler.QueryTelemetry;
import com.ospreydcs.dp.service.query.handler.QueryV2Resolver;
import com.ospreydcs.dp.service.query.handler.interfaces.QueryHandlerInterface;
import com.ospreydcs.dp.service.query.handler.model.ResolutionResult;
import com.ospreydcs.dp.service.query.handler.model.ResolvedQuery;
import com.ospreydcs.dp.service.query.handler.mongo.client.MongoQueryClientInterface;
import com.ospreydcs.dp.service.query.handler.mongo.client.MongoSyncQueryClient;
import com.ospreydcs.dp.service.query.handler.mongo.dispatch.*;
import com.ospreydcs.dp.service.query.handler.mongo.job.*;
import com.ospreydcs.dp.service.query.service.QueryServiceImpl;
import com.ospreydcs.dp.service.common.telemetry.DpMetrics;
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

    // querySamples time-sliced retrieval (#274, plan D2): the first slice of every page/stream
    private static final String CFG_KEY_QUERY_V2_SAMPLES_INITIAL_SLICE_SECONDS =
            "QueryHandler.queryV2SamplesInitialSliceSeconds";
    private static final int DEFAULT_QUERY_V2_SAMPLES_INITIAL_SLICE_SECONDS = 60;

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

    @Override
    protected String getServiceName_() {
        return DpMetrics.SERVICE_QUERY;
    }

    public static int getOutgoingMessageSizeLimitBytes() {
        return configMgr().getConfigInteger(
                CFG_KEY_OUTGOING_MESSAGE_SIZE_LIMIT_BYTES,
                DEFAULT_OUTGOING_MESSAGE_SIZE_LIMIT_BYTES);
    }

    /**
     * The length in nanos of the first retrieval slice of a querySamples page or stream (#274,
     * plan D2). A non-positive configured value is treated as the default: a zero-length slice
     * would never advance.
     */
    public static long getQuerySamplesInitialSliceNanos() {
        final int seconds = configMgr().getConfigInteger(
                CFG_KEY_QUERY_V2_SAMPLES_INITIAL_SLICE_SECONDS,
                DEFAULT_QUERY_V2_SAMPLES_INITIAL_SLICE_SECONDS);
        return (seconds > 0 ? seconds : DEFAULT_QUERY_V2_SAMPLES_INITIAL_SLICE_SECONDS) * 1_000_000_000L;
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

        final QueryTelemetry telemetry = newTelemetry("queryDataStream", querySpec);
        final QueryDataStreamDispatcher dispatcher =
                new QueryDataStreamDispatcher(responseObserver, querySpec, telemetry);
        final QueryDataJob job =
                new QueryDataJob(querySpec, dispatcher, responseObserver, mongoQueryClient, telemetry);

        telemetry.markEnqueued();
        enqueueJob(job, responseObserver.hashCode());
    }

    @Override
    public QueryResultCursor handleQueryDataBidiStream(
            QueryDataRequest.QuerySpec querySpec, StreamObserver<QueryDataResponse> responseObserver) {


        final QueryTelemetry telemetry = newTelemetry("queryDataBidiStream", querySpec);
        final QueryDataBidiStreamDispatcher dispatcher =
                new QueryDataBidiStreamDispatcher(responseObserver, querySpec, telemetry);
        final QueryDataJob job =
                new QueryDataJob(querySpec, dispatcher, responseObserver, mongoQueryClient, telemetry);
        final QueryResultCursor resultCursor = new QueryResultCursor(this, dispatcher);

        telemetry.markEnqueued();
        enqueueJob(job, responseObserver.hashCode());

        return resultCursor;
    }

    @Override
    public void handleQueryData(
            QueryDataRequest.QuerySpec querySpec, StreamObserver<QueryDataResponse> responseObserver) {

        final QueryTelemetry telemetry = newTelemetry("queryData", querySpec);
        final QueryDataDispatcher dispatcher =
                new QueryDataDispatcher(responseObserver, querySpec, telemetry);
        final QueryDataJob job =
                new QueryDataJob(querySpec, dispatcher, responseObserver, mongoQueryClient, telemetry);

        telemetry.markEnqueued();
        enqueueJob(job, responseObserver.hashCode());
    }

    @Override
    public void handleQueryTable(
            QueryTableRequest request, StreamObserver<QueryTableResponse> responseObserver) {

        final QueryTelemetry telemetry = new QueryTelemetry("queryTable");
        telemetry.captureShape(request);
        final QueryTableJob job =
                new QueryTableJob(request, responseObserver, mongoQueryClient, telemetry);

        telemetry.markEnqueued();
        enqueueJob(job, responseObserver.hashCode());
    }

    @Override
    public void handleQueryPvStats(
            QueryPvStatsRequest request,
            StreamObserver<QueryPvStatsResponse> responseObserver
    ) {
        final QueryPvStatsJob job =
                new QueryPvStatsJob(request, responseObserver, mongoQueryClient);

        enqueueJob(job, responseObserver.hashCode());
    }

    @Override
    public void handleQueryProviders(
            QueryProvidersRequest request,
            StreamObserver<QueryProvidersResponse> responseObserver
    ) {
        final QueryProvidersJob job =
                new QueryProvidersJob(request, responseObserver, mongoQueryClient);

        enqueueJob(job, responseObserver.hashCode());
    }

    @Override
    public void handleQueryProviderStats(
            QueryProviderStatsRequest request,
            StreamObserver<QueryProviderStatsResponse> responseObserver
    ) {
        final QueryProviderStatsJob job =
                new QueryProviderStatsJob(request, responseObserver, mongoQueryClient);

        enqueueJob(job, responseObserver.hashCode());
    }

    @Override
    public void handleQueryBuckets(
            QueryBucketsRequest request,
            StreamObserver<QueryBucketsResponse> responseObserver
    ) {
        final QueryTelemetry telemetry = new QueryTelemetry("queryBuckets");
        final ResolvedQuery resolvedQuery =
                resolveBucketsOrReject(request, false, responseObserver, telemetry);
        if (resolvedQuery == null) {
            return; // reject already sent and recorded
        }
        final QueryBucketsUnaryDispatcher dispatcher =
                new QueryBucketsUnaryDispatcher(responseObserver, telemetry);
        enqueueQueryV2Job(resolvedQuery, dispatcher, telemetry, responseObserver.hashCode());
    }

    @Override
    public void handleQueryBucketsStream(
            QueryBucketsRequest request,
            StreamObserver<QueryBucketsResponse> responseObserver
    ) {
        final QueryTelemetry telemetry = new QueryTelemetry("queryBucketsStream");
        final ResolvedQuery resolvedQuery =
                resolveBucketsOrReject(request, true, responseObserver, telemetry);
        if (resolvedQuery == null) {
            return; // reject already sent and recorded (includes the non-empty-pageToken streaming rule)
        }
        final QueryBucketsStreamDispatcher dispatcher =
                new QueryBucketsStreamDispatcher(responseObserver, telemetry);
        enqueueQueryV2Job(resolvedQuery, dispatcher, telemetry, responseObserver.hashCode());
    }

    /**
     * Validates + resolves a bucket request (§6 invariants, PV/config resolution, paging
     * normalization). On error, sends an ExceptionalResult reject and returns null; otherwise returns
     * the ResolvedQuery. The {@code streaming} flag drives the paging-token rule (Q7).
     */
    private ResolvedQuery resolveBucketsOrReject(
            QueryBucketsRequest request, boolean streaming,
            StreamObserver<QueryBucketsResponse> responseObserver,
            QueryTelemetry telemetry) {

        final ResolutionResult resolution = queryV2Resolver.resolve(
                request.getQuerySpec(),
                request.getExecutionOptions(),
                request.getResultRepresentation(),
                ResolvedQuery.ResultMode.BUCKET,
                streaming);

        if (resolution.isError()) {
            QueryServiceImpl.sendQueryBucketsResponseReject(
                    resolution.getErrorStatus().msg, responseObserver);
            completeRejectedResolution(telemetry);
            return null;
        }
        telemetry.captureShape(resolution.getResolvedQuery());
        return resolution.getResolvedQuery();
    }

    @Override
    public void handleQuerySamples(
            QuerySamplesRequest request,
            StreamObserver<QuerySamplesResponse> responseObserver
    ) {
        final QueryTelemetry telemetry = new QueryTelemetry("querySamples");
        final ResolvedQuery resolvedQuery =
                resolveSamplesOrReject(request, false, responseObserver, telemetry);
        if (resolvedQuery == null) {
            return; // reject already sent and recorded
        }
        final QuerySamplesUnaryDispatcher dispatcher =
                new QuerySamplesUnaryDispatcher(responseObserver, telemetry);
        enqueueQueryV2Job(resolvedQuery, dispatcher, telemetry, responseObserver.hashCode());
    }

    @Override
    public void handleQuerySamplesStream(
            QuerySamplesRequest request,
            StreamObserver<QuerySamplesResponse> responseObserver
    ) {
        final QueryTelemetry telemetry = new QueryTelemetry("querySamplesStream");
        final ResolvedQuery resolvedQuery =
                resolveSamplesOrReject(request, true, responseObserver, telemetry);
        if (resolvedQuery == null) {
            return; // reject already sent and recorded (includes the non-empty-pageToken streaming rule)
        }
        final QuerySamplesStreamDispatcher dispatcher =
                new QuerySamplesStreamDispatcher(responseObserver, telemetry);
        enqueueQueryV2Job(resolvedQuery, dispatcher, telemetry, responseObserver.hashCode());
    }

    /**
     * Validates + resolves a sample request (mode=SAMPLE). On error, sends an ExceptionalResult
     * reject and returns null; otherwise returns the ResolvedQuery. The {@code streaming} flag drives
     * the paging-token rule (Q7).
     */
    private ResolvedQuery resolveSamplesOrReject(
            QuerySamplesRequest request, boolean streaming,
            StreamObserver<QuerySamplesResponse> responseObserver,
            QueryTelemetry telemetry) {

        final ResolutionResult resolution = queryV2Resolver.resolve(
                request.getQuerySpec(),
                request.getExecutionOptions(),
                request.getResultRepresentation(),
                ResolvedQuery.ResultMode.SAMPLE,
                streaming);

        if (resolution.isError()) {
            QueryServiceImpl.sendQuerySamplesResponseReject(
                    resolution.getErrorStatus().msg, responseObserver);
            completeRejectedResolution(telemetry);
            return null;
        }
        telemetry.captureShape(resolution.getResolvedQuery());
        return resolution.getResolvedQuery();
    }

    private void enqueueQueryV2Job(
            ResolvedQuery resolvedQuery,
            com.ospreydcs.dp.service.query.handler.mongo.dispatch.QueryV2Dispatcher dispatcher,
            QueryTelemetry telemetry, int observerId) {

        final QueryV2Job job = new QueryV2Job(resolvedQuery, dispatcher, mongoQueryClient, telemetry);
        logger.debug("adding QueryV2Job ({}) id: {} to queue", telemetry.getRpcMethod(), observerId);
        telemetry.markEnqueued();
        enqueueJob(job, observerId);
    }

    /**
     * Builds the telemetry context for a legacy (V1) query and captures its request shape.
     *
     * <p>The V1 methods have no resolution phase, so their {@code resolve} stage is just the
     * handler-entry-to-enqueue interval -- microseconds. That is not a defect in the measurement:
     * it is the honest reading, and it is what makes a V2 method's resolve stage legible by
     * contrast when comparing the two on one dashboard.
     */
    private static QueryTelemetry newTelemetry(String rpcMethod, QueryDataRequest.QuerySpec querySpec) {
        final QueryTelemetry telemetry = new QueryTelemetry(rpcMethod);
        telemetry.captureShape(querySpec);
        return telemetry;
    }

    /**
     * Completes the telemetry of a V2 request rejected during resolution, before any job exists.
     *
     * <p>Without this, the rejects that never reach a job -- a malformed page token, an unresolvable
     * PV selector, a streaming request carrying a page token -- would be counted nowhere, and
     * {@code dp.query.requests} would report a reject rate of zero for exactly the failures a client
     * is most likely to be generating. Their stage breakdown is genuinely almost all {@code resolve},
     * which is the right answer: resolution is where the work happened and where it stopped.
     */
    private static void completeRejectedResolution(QueryTelemetry telemetry) {
        telemetry.markEnqueued(); // ends the resolve stage; no job follows
        telemetry.markReject();
        telemetry.complete();
    }

}
