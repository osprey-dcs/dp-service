package com.ospreydcs.dp.service.query.handler.mongo.job;

import com.ospreydcs.dp.grpc.v1.common.DataTable;
import com.ospreydcs.dp.grpc.v1.query.QueryRequest;
import com.ospreydcs.dp.service.query.handler.mongo.client.MongoQueryClientInterface;
import com.ospreydcs.dp.service.query.handler.mongo.dispatch.ResponseTableDispatcher;
import io.grpc.stub.StreamObserver;

public class QueryTableJob extends HandlerJob {

    private final QueryRequest.QuerySpec querySpec;
    private final StreamObserver<DataTable> responseObserver;
    private final ResponseTableDispatcher dispatcher;
    private final MongoQueryClientInterface mongoClient;

    public QueryTableJob(
            QueryRequest.QuerySpec querySpec,
            StreamObserver<DataTable> responseObserver,
            MongoQueryClientInterface mongoClient
    ) {
        this.querySpec = querySpec;
        this.responseObserver = responseObserver;
        this.mongoClient = mongoClient;
        this.dispatcher = new ResponseTableDispatcher(responseObserver);
    }

    public void execute() {

    }
}
