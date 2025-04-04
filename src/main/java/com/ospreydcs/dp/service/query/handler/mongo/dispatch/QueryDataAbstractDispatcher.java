package com.ospreydcs.dp.service.query.handler.mongo.dispatch;

import com.mongodb.client.MongoCursor;
import com.ospreydcs.dp.grpc.v1.common.DataValue;
import com.ospreydcs.dp.grpc.v1.query.QueryDataResponse;
import com.ospreydcs.dp.service.common.bson.bucket.BucketDocument;
import com.ospreydcs.dp.service.common.exception.DpException;
import com.ospreydcs.dp.service.common.handler.Dispatcher;
import com.ospreydcs.dp.service.query.handler.mongo.MongoQueryHandler;
import com.ospreydcs.dp.service.query.service.QueryServiceImpl;
import io.grpc.stub.StreamObserver;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;
import java.util.Objects;

public abstract class QueryDataAbstractDispatcher extends Dispatcher {

    // static variables
    private static final Logger logger = LogManager.getLogger();

    // instance variables
    private StreamObserver<QueryDataResponse> responseObserver;

    public QueryDataAbstractDispatcher() {
    }

    public QueryDataAbstractDispatcher(StreamObserver<QueryDataResponse> responseObserver) {
        this.responseObserver = responseObserver;
    }

    protected abstract void handleResult_(MongoCursor<BucketDocument> cursor);

    public StreamObserver<QueryDataResponse> getResponseObserver() {
        return this.responseObserver;
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
