package com.ospreydcs.dp.service.query.handler.mongo.dispatch;

import com.mongodb.client.MongoCursor;
import com.ospreydcs.dp.grpc.v1.query.QueryResponse;
import com.ospreydcs.dp.service.common.bson.BucketDocument;
import io.grpc.stub.StreamObserver;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class ResponseSingleDispatcher extends ResultDispatcher {

    private static final Logger LOGGER = LogManager.getLogger();

    public ResponseSingleDispatcher(StreamObserver<QueryResponse> responseObserver) {
        super(responseObserver);
    }

    @Override
    public void handleResult_(MongoCursor<BucketDocument> cursor) {
        throw new UnsupportedOperationException("ResponseCursorDispatcher.handleResult() not yet implemented");
    }
}
