package com.ospreydcs.dp.service.query.handler;

import com.ospreydcs.dp.grpc.v1.query.QueryDataByTimeRequest;
import com.ospreydcs.dp.service.query.handler.model.ValidateQueryRequestResult;

public interface QueryHandlerInterface {
    boolean init();
    boolean fini();
    boolean start();
    boolean stop();
    ValidateQueryRequestResult validateQueryDataByTimeRequest(QueryDataByTimeRequest request);
}
