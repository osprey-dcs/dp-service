package com.ospreydcs.dp.service.query.handler.mongo.job;

import com.ospreydcs.dp.service.common.handler.HandlerJob;
import com.ospreydcs.dp.service.query.handler.QueryTelemetry;
import com.ospreydcs.dp.service.query.handler.model.ResolvedQuery;
import com.ospreydcs.dp.service.query.handler.mongo.client.MongoQueryClientInterface;
import com.ospreydcs.dp.service.query.handler.mongo.dispatch.QueryV2Dispatcher;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * Shared worker job for all Query API V2 methods. Carries the resolved query and an injected V2
 * dispatcher; on execution it hands both the resolved query and the client to the dispatcher, which
 * owns retrieval and formatting (bucket vs. sample, unary vs. stream). One job, many dispatcher
 * variants — mirroring how {@code QueryDataJob} serves the V1 unary/stream/bidi paths.
 */
public class QueryV2Job extends HandlerJob {

    private static final Logger logger = LogManager.getLogger();

    private final ResolvedQuery resolvedQuery;
    private final QueryV2Dispatcher dispatcher;
    private final MongoQueryClientInterface mongoClient;
    private final QueryTelemetry telemetry;

    public QueryV2Job(
            ResolvedQuery resolvedQuery,
            QueryV2Dispatcher dispatcher,
            MongoQueryClientInterface mongoClient,
            QueryTelemetry telemetry) {
        this.resolvedQuery = resolvedQuery;
        this.dispatcher = dispatcher;
        this.mongoClient = mongoClient;
        this.telemetry = telemetry;
    }

    /**
     * {@inheritDoc}
     *
     * <p>The telemetry completion is in a {@code finally} so that a request is recorded even when
     * the dispatcher throws. That escape is the hang documented throughout CLAUDE.md -- the worker
     * swallows the exception, the dispatcher never answers, and the caller's stream stays open
     * until it times out. The metrics cannot prevent it, but completing here means it is at least
     * visible, and the {@code catch} classifies it: the request appears in
     * {@code dp.query.requests} as an <em>error</em> rather than carrying the default outcome of
     * success, which is what it silently did before.
     */
    @Override
    public void execute() {
        logger.debug("executing QueryV2Job");
        telemetry.markJobStarted(this);
        try {
            dispatcher.executeAndDispatch(resolvedQuery, mongoClient);
        } catch (RuntimeException e) {
            // Classify before completing. The worker swallows whatever escapes here, so without
            // this the request -- which never sent a response and left the caller's stream hanging
            // -- would be recorded with the telemetry's default outcome of success.
            telemetry.markFailedWithException();
            throw e;
        } finally {
            telemetry.complete();
        }
    }

    /**
     * Records the request as an error when the job is dropped before it ever runs; see
     * {@code HandlerJob.discarded()}.
     */
    @Override
    public void discarded() {
        telemetry.markFailedWithException();
        telemetry.complete();
    }
}
