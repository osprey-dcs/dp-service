package com.ospreydcs.dp.service.query.handler.mongo.dispatch;

import com.mongodb.client.MongoCursor;
import com.ospreydcs.dp.grpc.v1.query.QueryDataRequest;
import com.ospreydcs.dp.grpc.v1.query.QueryDataResponse;
import com.ospreydcs.dp.service.common.bson.bucket.BucketDocument;
import com.ospreydcs.dp.service.common.handler.Dispatcher;
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

    public QueryDataAbstractDispatcher(
            StreamObserver<QueryDataResponse> responseObserver,
            QueryDataRequest.QuerySpec querySpec
    ) {
        this.responseObserver = responseObserver;
        this.querySpec = querySpec;
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
            QueryServiceImpl.sendQueryDataResponseError(msg, getResponseObserver());
            return;
        }

        handleResult_(cursor);
    }

}
