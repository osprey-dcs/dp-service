package com.ospreydcs.dp.service.query.handler.mongo.job;

import com.ospreydcs.dp.grpc.v1.query.QueryDataRequest;
import com.ospreydcs.dp.grpc.v1.query.QueryDataResponse;
import com.ospreydcs.dp.service.common.handler.HandlerJob;
import com.ospreydcs.dp.service.query.handler.QueryTelemetry;
import com.ospreydcs.dp.service.query.handler.mongo.client.MongoQueryClientInterface;
import com.ospreydcs.dp.service.query.handler.mongo.dispatch.QueryDataAbstractDispatcher;
import io.grpc.stub.StreamObserver;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class QueryDataJob extends HandlerJob {

    // static variables
    private static final Logger logger = LogManager.getLogger();

    // instance variables
    private final QueryDataRequest.QuerySpec querySpec;
    private final QueryDataAbstractDispatcher dispatcher;
    private final StreamObserver<QueryDataResponse> responseObserver;
    private final MongoQueryClientInterface mongoClient;
    private final QueryTelemetry telemetry;

    public QueryDataJob(QueryDataRequest.QuerySpec spec,
                        QueryDataAbstractDispatcher dispatcher,
                        StreamObserver<QueryDataResponse> responseObserver,
                        MongoQueryClientInterface mongoClient,
                        QueryTelemetry telemetry
    ) {
        this.querySpec = spec;
        this.dispatcher = dispatcher;
        this.responseObserver = responseObserver;
        this.mongoClient = mongoClient;
        this.telemetry = telemetry;
    }

    /**
     * Runs the query and dispatches its result, with the {@code db} stage measured around the
     * retrieval call and the telemetry completed in a {@code finally} (see {@code QueryV2Job} for
     * why the completion cannot be on the normal path only).
     *
     * <p>The wall time of {@code executeQueryData} is attributed to {@code db} in addition to the
     * cursor's own accumulated time: the call resolves the per-PV bucket span from {@code pvStats}
     * (#232) and opens the find, both of which are database round-trips that happen before the
     * dispatcher touches the cursor.
     */
    public void execute() {
        logger.debug("executing QueryJob id: {}", this.responseObserver.hashCode());
        telemetry.markJobStarted(this);
        try {
            final long queryStartNanos = System.nanoTime();
            final var cursor = this.mongoClient.executeQueryData(this.querySpec);
            telemetry.addDbNanos(System.nanoTime() - queryStartNanos);
            logger.debug("dispatching QueryJob id: {}", this.responseObserver.hashCode());
            dispatcher.handleResult(cursor);
        } finally {
            telemetry.complete();
        }
    }

}
