package com.ospreydcs.dp.service.ingest.handler.mongo.dispatch;

import com.mongodb.client.MongoCursor;
import com.ospreydcs.dp.grpc.v1.ingestion.QueryRequestStatusRequest;
import com.ospreydcs.dp.grpc.v1.ingestion.QueryRequestStatusResponse;
import com.ospreydcs.dp.service.common.bson.RequestStatusDocument;
import com.ospreydcs.dp.service.common.handler.Dispatcher;
import com.ospreydcs.dp.service.ingest.handler.mongo.client.MongoIngestionClientInterface;
import com.ospreydcs.dp.service.ingest.service.IngestionServiceImpl;
import io.grpc.stub.StreamObserver;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class QueryRequestStatusResponseDispatcher extends Dispatcher {

    // static variables
    private static final Logger logger = LogManager.getLogger();

    // instance variables
    private final QueryRequestStatusRequest request;
    private final StreamObserver<QueryRequestStatusResponse> responseObserver;
    private final MongoIngestionClientInterface mongoClient;

    public QueryRequestStatusResponseDispatcher(
            StreamObserver<QueryRequestStatusResponse> responseObserver,
            QueryRequestStatusRequest request,
            MongoIngestionClientInterface mongoClient
    ) {
        super();
        this.request = request;
        this.responseObserver = responseObserver;
        this.mongoClient = mongoClient;
    }

    public void handleResult(MongoCursor<RequestStatusDocument> cursor) {

        // validate cursor
        if (cursor == null) {
            // send error response and close response stream if cursor is null
            final String msg = "query returned null cursor";
            logger.debug(msg);
            IngestionServiceImpl.sendQueryRequestStatusResponseError(msg, this.responseObserver);
            return;
        } else if (!cursor.hasNext()) {
            logger.trace("query matched no data, cursor is empty");
            IngestionServiceImpl.sendQueryRequestStatusResponseEmpty(this.responseObserver);
            return;
        }

        final QueryRequestStatusResponse.RequestStatusResult.Builder requestStatusBuilder =
                QueryRequestStatusResponse.RequestStatusResult.newBuilder();

        while (cursor.hasNext()) {

            // add grpc object for each document in cursor
            final RequestStatusDocument requestStatusDocument = cursor.next();

            // build grpc response and add to result
            final QueryRequestStatusResponse.RequestStatusResult.RequestStatus responseRequestStatus =
                    requestStatusDocument.buildRequestStatus(requestStatusDocument);
            requestStatusBuilder.addRequestStatus(responseRequestStatus);
        }

        // send response and close response stream
        final QueryRequestStatusResponse.RequestStatusResult requestStatusResult = requestStatusBuilder.build();
        IngestionServiceImpl.sendQueryRequestStatusResponse(requestStatusResult, this.responseObserver);
    }
}
