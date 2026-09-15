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
     * visible: the request appears in {@code dp.query.requests}, and the outcome it carries is
     * whichever one had been set before the throw.
     */
    @Override
    public void execute() {
        logger.debug("executing QueryV2Job");
        telemetry.markJobStarted(this);
        try {
            dispatcher.executeAndDispatch(resolvedQuery, mongoClient);
        } finally {
            telemetry.complete();
        }
    }
}
