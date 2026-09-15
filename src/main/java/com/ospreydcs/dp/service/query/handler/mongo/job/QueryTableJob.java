package com.ospreydcs.dp.service.query.handler.mongo.job;

import com.ospreydcs.dp.grpc.v1.query.QueryTableRequest;
import com.ospreydcs.dp.grpc.v1.query.QueryTableResponse;
import com.ospreydcs.dp.service.common.handler.HandlerJob;
import com.ospreydcs.dp.service.query.handler.QueryTelemetry;
import com.ospreydcs.dp.service.query.handler.mongo.client.MongoQueryClientInterface;
import com.ospreydcs.dp.service.query.handler.mongo.dispatch.QueryTableDispatcher;
import io.grpc.stub.StreamObserver;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class QueryTableJob extends HandlerJob {

    // static variables
    private static final Logger logger = LogManager.getLogger();

    // instance variables
    private final QueryTableRequest request;
    private final QueryTableDispatcher dispatcher;
    private final StreamObserver<QueryTableResponse> responseObserver;
    private final MongoQueryClientInterface mongoClient;
    private final QueryTelemetry telemetry;

    public QueryTableJob(QueryTableRequest request,
                         StreamObserver<QueryTableResponse> responseObserver,
                         MongoQueryClientInterface mongoClient,
                         QueryTelemetry telemetry
    ) {
        this.request = request;
        this.responseObserver = responseObserver;
        this.mongoClient = mongoClient;
        this.telemetry = telemetry;
        this.dispatcher = new QueryTableDispatcher(responseObserver, this.request, telemetry);
    }

    /** See {@code QueryDataJob.execute()} for the db-stage and finally-completion rationale. */
    public void execute() {
        logger.debug("executing QueryTableJob id: {}", this.responseObserver.hashCode());
        telemetry.markJobStarted(this);
        try {
            final long queryStartNanos = System.nanoTime();
            final var cursor = this.mongoClient.executeQueryTable(this.request);
            telemetry.addDbNanos(System.nanoTime() - queryStartNanos);
            logger.debug("dispatching QueryTableJob id: {}", this.responseObserver.hashCode());
            dispatcher.handleResult(cursor);
        } catch (RuntimeException e) {
            // See QueryV2Job: the worker swallows this, so classify it before complete() records
            // the request with its default outcome of success.
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
