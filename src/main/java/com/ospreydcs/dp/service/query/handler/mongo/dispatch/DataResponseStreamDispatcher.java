package com.ospreydcs.dp.service.query.handler.mongo.dispatch;

import com.mongodb.client.MongoCursor;
import com.ospreydcs.dp.grpc.v1.common.ResponseType;
import com.ospreydcs.dp.grpc.v1.query.QueryDataResponse;
import com.ospreydcs.dp.service.common.bson.BucketDocument;
import io.grpc.stub.StreamObserver;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class DataResponseStreamDispatcher extends BucketDocumentResponseDispatcher {

    private static final Logger logger = LogManager.getLogger();
    public DataResponseStreamDispatcher(StreamObserver<QueryDataResponse> responseObserver) {
        super(responseObserver);
    }

    @Override
    public void handleResult_(MongoCursor<BucketDocument> cursor) {

        while (cursor.hasNext()) {
            final QueryDataResponse response = super.nextQueryResponseFromCursor(cursor);

            if (response != null) {
                final boolean isError = (response.getResponseType() == ResponseType.ERROR_RESPONSE);
                getResponseObserver().onNext(response);

                if (isError) {
                    // cursor iteration generated an error so break out and close response stream
                    break;
                }

            } else {
                // no further results to process
                break;
            }
        }

        getResponseObserver().onCompleted();
        cursor.close();
    }

}
