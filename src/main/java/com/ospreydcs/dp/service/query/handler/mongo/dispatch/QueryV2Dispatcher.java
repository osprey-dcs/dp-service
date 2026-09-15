package com.ospreydcs.dp.service.query.handler.mongo.dispatch;

import com.mongodb.client.MongoCursor;
import com.ospreydcs.dp.service.common.handler.Dispatcher;
import com.ospreydcs.dp.service.common.mongo.TimedMongoCursor;
import com.ospreydcs.dp.service.query.handler.QueryTelemetry;
import com.ospreydcs.dp.service.query.handler.model.ResolvedQuery;
import com.ospreydcs.dp.service.query.handler.mongo.client.MongoQueryClientInterface;

/**
 * Base class for Query API V2 result dispatchers (formatters). A single {@code QueryV2Job} drives any
 * V2 query by delegating to {@link #executeAndDispatch}: the concrete dispatcher owns both its
 * retrieval strategy (bucket keyset paging vs. sample timestamp-window assembly) and its response
 * formatting, so bucket-vs-sample and unary-vs-stream are dispatcher variants over one job.
 */
public abstract class QueryV2Dispatcher extends Dispatcher {

    protected final QueryTelemetry telemetry;

    protected QueryV2Dispatcher(QueryTelemetry telemetry) {
        this.telemetry = telemetry;
    }

    /**
     * Executes retrieval for the resolved query against the given client and dispatches the formatted
     * response(s) to the response observer, closing the stream. Implementations must handle the empty
     * result (empty payload, not an ExceptionalResult) and any retrieval/formatting error
     * (ExceptionalResult) themselves.
     */
    public abstract void executeAndDispatch(ResolvedQuery resolvedQuery, MongoQueryClientInterface mongoClient);

    /**
     * Folds a finished cursor's accumulated time and document count into the request's {@code db}
     * stage (issue #212, D3).
     *
     * <p>Shared here, and called from a {@code finally} at every retrieval site, because the cursor
     * carries its totals internally: a dispatcher that returns early on an error without folding
     * them in reports a {@code db} stage of zero for precisely the request whose database time the
     * operator wants to see. The cursor may legitimately not be a {@link TimedMongoCursor} -- the
     * unit tests inject plain cursors, and {@code MongoAsyncQueryClient} is unwrapped -- so the type
     * check is a normal condition, not a defensive one.
     */
    protected void recordCursorTime(MongoCursor<?> cursor) {
        if (cursor instanceof TimedMongoCursor<?> timedCursor) {
            telemetry.addCursorTime(timedCursor.elapsedNanos(), timedCursor.documentCount());
        }
    }
}
