package com.ospreydcs.dp.service.query.handler.mongo.job;

import com.ospreydcs.dp.service.common.handler.HandlerJob;
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

    public QueryV2Job(
            ResolvedQuery resolvedQuery,
            QueryV2Dispatcher dispatcher,
            MongoQueryClientInterface mongoClient) {
        this.resolvedQuery = resolvedQuery;
        this.dispatcher = dispatcher;
        this.mongoClient = mongoClient;
    }

    @Override
    public void execute() {
        logger.debug("executing QueryV2Job");
        dispatcher.executeAndDispatch(resolvedQuery, mongoClient);
    }
}
