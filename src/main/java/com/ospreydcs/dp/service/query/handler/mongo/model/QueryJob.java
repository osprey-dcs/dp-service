package com.ospreydcs.dp.service.query.handler.mongo.model;

import com.ospreydcs.dp.grpc.v1.query.QueryRequest;
import com.ospreydcs.dp.service.query.handler.mongo.dispatch.ResultDispatcher;

public class QueryJob {
    private final QueryRequest.QuerySpec querySpec;
    private final ResultDispatcher dispatcher;
    public QueryJob(QueryRequest.QuerySpec spec, ResultDispatcher dispatcher) {
        this.querySpec = spec;
        this.dispatcher = dispatcher;
    }
    public QueryRequest.QuerySpec getQuerySpec() {
        return this.querySpec;
    }
    public ResultDispatcher getDispatcher() {
        return this.dispatcher;
    }
}
