package com.ospreydcs.dp.service.query.handler.mongo.dispatch;

import com.mongodb.client.MongoCursor;
import com.ospreydcs.dp.grpc.v1.query.QueryDataResponse;
import com.ospreydcs.dp.service.common.bson.BucketDocument;
import com.ospreydcs.dp.service.query.service.QueryServiceImpl;
import io.grpc.stub.StreamObserver;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class DataResponseUnaryDispatcher extends BucketDocumentResponseDispatcher {

    private static final Logger LOGGER = LogManager.getLogger();

    public DataResponseUnaryDispatcher(StreamObserver<QueryDataResponse> responseObserver) {
        super(responseObserver);
    }

    @Override
    public void handleResult_(MongoCursor<BucketDocument> cursor) {

        QueryDataResponse response = super.nextQueryResponseFromCursor(cursor);

        if (response == null) {
            final String msg = "unexpected error building QueryResponse from cursor";
            QueryServiceImpl.sendQueryResponseDataError(msg, getResponseObserver());

        } else if (cursor.hasNext()) {
            // query returned more data than will fit in a single response, so return an error instead of partial data
            final String msg = "query returned more data than will fit in single QueryResponse message";
            QueryServiceImpl.sendQueryResponseDataError(msg, getResponseObserver());

        } else {
            getResponseObserver().onNext(response);
        }

        getResponseObserver().onCompleted();
        cursor.close();
    }

}
