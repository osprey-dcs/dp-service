package com.ospreydcs.dp.service.query.handler.mongo.dispatch;

import com.mongodb.client.MongoCursor;
import com.ospreydcs.dp.grpc.v1.common.ExceptionalResult;
import com.ospreydcs.dp.grpc.v1.query.QueryDataResponse;
import com.ospreydcs.dp.service.common.bson.bucket.BucketDocument;
import io.grpc.stub.StreamObserver;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class QueryDataStreamDispatcher extends QueryDataAbstractDispatcher {

    private static final Logger logger = LogManager.getLogger();
    public QueryDataStreamDispatcher(StreamObserver<QueryDataResponse> responseObserver) {
        super(responseObserver);
    }

    @Override
    public void handleResult_(MongoCursor<BucketDocument> cursor) {

        while (cursor.hasNext()) {
            final QueryDataResponse response = super.nextQueryResponseFromCursor(cursor);

            if (response != null) {
                boolean isError = false;
                if (response.hasExceptionalResult()) {
                    ExceptionalResult exceptionalResult = response.getExceptionalResult();
                    if (exceptionalResult.getExceptionalResultStatus()
                            == ExceptionalResult.ExceptionalResultStatus.RESULT_STATUS_ERROR) {
                        isError = true;
                    }
                }
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
