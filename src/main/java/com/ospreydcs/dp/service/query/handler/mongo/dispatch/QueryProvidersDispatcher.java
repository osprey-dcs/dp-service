package com.ospreydcs.dp.service.query.handler.mongo.dispatch;

import com.ospreydcs.dp.grpc.v1.query.QueryProvidersRequest;
import com.ospreydcs.dp.grpc.v1.query.QueryProvidersResponse;
import com.ospreydcs.dp.service.common.handler.Dispatcher;
import com.ospreydcs.dp.service.query.service.QueryServiceImpl;
import io.grpc.stub.StreamObserver;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;

public class QueryProvidersDispatcher extends Dispatcher {

    // static variables
    private static final Logger logger = LogManager.getLogger();

    // instance variables
    private final QueryProvidersRequest request;
    private final StreamObserver<QueryProvidersResponse> responseObserver;

    public QueryProvidersDispatcher(
            StreamObserver<QueryProvidersResponse> responseObserver, 
            QueryProvidersRequest request
    ) {
        this.request = request;
        this.responseObserver = responseObserver;
    }

    public void handleResult(List<QueryProvidersResponse.ProvidersResult.ProviderInfo> providerInfos) {

        QueryProvidersResponse.ProvidersResult.Builder providersResultBuilder =
                QueryProvidersResponse.ProvidersResult.newBuilder();

        providerInfos.forEach(providersResultBuilder::addProviderInfos);

        // send response and close response stream
        final QueryProvidersResponse.ProvidersResult providersResult = providersResultBuilder.build();
        QueryServiceImpl.sendQueryProvidersResponse(providersResult, this.responseObserver);
    }

}