package com.ospreydcs.dp.service.query.handler.mongo.dispatch;

import com.ospreydcs.dp.grpc.v1.common.DataTable;
import io.grpc.stub.StreamObserver;

public class ResponseTableDispatcher extends Dispatcher {

    private final StreamObserver<DataTable> responseObserver;

    public ResponseTableDispatcher(StreamObserver<DataTable> responseObserver) {
        this.responseObserver = responseObserver;
    }
}
