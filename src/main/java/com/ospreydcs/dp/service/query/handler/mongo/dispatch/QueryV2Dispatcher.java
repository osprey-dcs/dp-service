package com.ospreydcs.dp.service.query.handler.mongo.dispatch;

import com.ospreydcs.dp.service.common.handler.Dispatcher;
import com.ospreydcs.dp.service.query.handler.model.ResolvedQuery;
import com.ospreydcs.dp.service.query.handler.mongo.client.MongoQueryClientInterface;

/**
 * Base class for Query API V2 result dispatchers (formatters). A single {@code QueryV2Job} drives any
 * V2 query by delegating to {@link #executeAndDispatch}: the concrete dispatcher owns both its
 * retrieval strategy (bucket keyset paging vs. sample timestamp-window assembly) and its response
 * formatting, so bucket-vs-sample and unary-vs-stream are dispatcher variants over one job.
 */
public abstract class QueryV2Dispatcher extends Dispatcher {

    /**
     * Executes retrieval for the resolved query against the given client and dispatches the formatted
     * response(s) to the response observer, closing the stream. Implementations must handle the empty
     * result (empty payload, not an ExceptionalResult) and any retrieval/formatting error
     * (ExceptionalResult) themselves.
     */
    public abstract void executeAndDispatch(ResolvedQuery resolvedQuery, MongoQueryClientInterface mongoClient);
}
