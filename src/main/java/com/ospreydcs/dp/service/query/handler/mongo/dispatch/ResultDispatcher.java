package com.ospreydcs.dp.service.query.handler.mongo.dispatch;

import com.mongodb.client.MongoCursor;
import com.ospreydcs.dp.grpc.v1.query.QueryResponse;
import com.ospreydcs.dp.service.common.bson.BucketDocument;
import io.grpc.stub.StreamObserver;

public abstract class ResultDispatcher {

    final private StreamObserver<QueryResponse> responseObserver;
    public ResultDispatcher(StreamObserver<QueryResponse> responseObserver) {
        this.responseObserver = responseObserver;
    }

    public abstract void handleResult(MongoCursor<BucketDocument> cursor);

    protected StreamObserver<QueryResponse> getResponseObserver() {
        return this.responseObserver;
    }
}
