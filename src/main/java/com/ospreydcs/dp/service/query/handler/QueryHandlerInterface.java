package com.ospreydcs.dp.service.query.handler;

import com.ospreydcs.dp.grpc.v1.query.QueryRequest;
import com.ospreydcs.dp.service.common.model.ValidationResult;
import com.ospreydcs.dp.service.query.handler.model.HandlerQueryRequest;

public interface QueryHandlerInterface {
    boolean init();
    boolean fini();
    boolean start();
    boolean stop();
    ValidationResult validateQueryRequest(QueryRequest request);
    void handleQueryRequest(HandlerQueryRequest request);
}
