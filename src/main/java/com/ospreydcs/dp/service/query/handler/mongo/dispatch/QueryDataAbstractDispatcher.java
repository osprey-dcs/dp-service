package com.ospreydcs.dp.service.query.handler.mongo.dispatch;

import com.mongodb.client.MongoCursor;
import com.ospreydcs.dp.grpc.v1.query.QueryDataRequest;
import com.ospreydcs.dp.grpc.v1.query.QueryDataResponse;
import com.ospreydcs.dp.service.common.bson.bucket.BucketDocument;
import com.ospreydcs.dp.service.common.grpc.OutboundReadinessGate;
import com.ospreydcs.dp.service.common.handler.Dispatcher;
import com.ospreydcs.dp.service.common.mongo.TimedMongoCursor;
import com.ospreydcs.dp.service.query.handler.QueryTelemetry;
import com.ospreydcs.dp.service.query.service.QueryServiceImpl;
import io.grpc.stub.StreamObserver;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public abstract class QueryDataAbstractDispatcher extends Dispatcher {

    // static variables
    private static final Logger logger = LogManager.getLogger();

    // instance variables
    protected final StreamObserver<QueryDataResponse> responseObserver;
    protected final QueryDataRequest.QuerySpec querySpec;
    protected final QueryTelemetry telemetry;

    public QueryDataAbstractDispatcher(
            StreamObserver<QueryDataResponse> responseObserver,
            QueryDataRequest.QuerySpec querySpec,
            QueryTelemetry telemetry
    ) {
        this.responseObserver = responseObserver;
        this.querySpec = querySpec;
        this.telemetry = telemetry;
    }

    /**
     * Folds a finished cursor's accumulated time and document count into the request's {@code db}
     * stage (issue #212, D3). See {@code QueryV2Dispatcher.recordCursorTime} for why the type check
     * is a normal condition rather than a defensive one.
     */
    protected void recordCursorTime(MongoCursor<?> cursor) {
        if (cursor instanceof TimedMongoCursor<?> timedCursor) {
            telemetry.addCursorTime(timedCursor.elapsedNanos(), timedCursor.documentCount());
        }
    }

    /**
     * Records the error outcome and sends the error response, unless the client has cancelled
     * (#326): then nothing is sent and the call is not completed. The outcome is {@code error}
     * either way, because the request did fail. Cancellation only, not readiness: one small
     * message to a slow reader is cheaper than holding a worker for it, and a reader that
     * resumes should find its error. Shared with the unary and bidi dispatchers: a unary
     * {@code onNext} does not throw on a cancelled call, so there the skip changes nothing; the
     * bidi call has no cancel handler, so there it avoids the {@code StatusRuntimeException}
     * gRPC throws from {@code onNext} once cancelled.
     */
    protected void sendError(String msg) {
        telemetry.markError();
        if (OutboundReadinessGate.isCancelled(getResponseObserver())) {
            logger.debug("error response not sent, client cancelled id: {}", getResponseObserver().hashCode());
            return;
        }
        QueryServiceImpl.sendQueryDataResponseError(msg, getResponseObserver());
    }

    protected abstract void handleResult_(MongoCursor<BucketDocument> cursor);

    protected StreamObserver<QueryDataResponse> getResponseObserver() {
        return this.responseObserver;
    }

    protected QueryDataRequest.QuerySpec getQuerySpec() {
        return this.querySpec;
    }

    public void handleResult(MongoCursor<BucketDocument> cursor) {

        // send error response and close response stream if cursor is null
        if (cursor == null) {
            final String msg = "executeQuery returned null cursor";
            logger.error(msg);
            sendError(msg);
            return;
        }

        handleResult_(cursor);
    }

}
