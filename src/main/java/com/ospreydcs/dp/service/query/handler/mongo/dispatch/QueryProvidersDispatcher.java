package com.ospreydcs.dp.service.query.handler.mongo.dispatch;

import com.mongodb.client.MongoCursor;
import com.ospreydcs.dp.grpc.v1.query.QueryProvidersRequest;
import com.ospreydcs.dp.grpc.v1.query.QueryProvidersResponse;
import com.ospreydcs.dp.service.common.bson.ProviderDocument;
import com.ospreydcs.dp.service.common.handler.Dispatcher;
import com.ospreydcs.dp.service.query.service.QueryServiceImpl;
import io.grpc.stub.StreamObserver;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

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

    public void handleResult(MongoCursor<ProviderDocument> cursor) {

        // validate cursor
        if (cursor == null) {
            // send error response and close response stream if cursor is null
            final String msg = "providers query returned null cursor";
            logger.error(msg);
            QueryServiceImpl.sendQueryProvidersResponseError(msg, this.responseObserver);
            return;
        } else if (!cursor.hasNext()) {
            // send empty QueryStatus and close response stream if query matched no data
            logger.trace("providers query matched no data, cursor is empty");
            QueryServiceImpl.sendQueryProvidersResponseEmpty(this.responseObserver);
            return;
        }

        QueryProvidersResponse.ProvidersResult.Builder providersResultBuilder =
                QueryProvidersResponse.ProvidersResult.newBuilder();

        while (cursor.hasNext()) {
            // add grpc object for each document in cursor

            final ProviderDocument providerDocument = cursor.next();

            final QueryProvidersResponse.ProvidersResult.ProviderInfo responseProviderInfo = 
                    providerDocument.toProviderInfo();
            providersResultBuilder.addProviderInfos(responseProviderInfo);
        }
        
        // send response and close response stream
        final QueryProvidersResponse.ProvidersResult providersResult = providersResultBuilder.build();
        QueryServiceImpl.sendQueryProvidersResponse(providersResult, this.responseObserver);
    }

}