package com.ospreydcs.dp.service.query.handler.mongo.dispatch;

import com.mongodb.client.MongoCursor;
import com.ospreydcs.dp.grpc.v1.common.ResponseType;
import com.ospreydcs.dp.grpc.v1.query.QueryResponse;
import com.ospreydcs.dp.service.common.bson.BucketDocument;
import com.ospreydcs.dp.service.query.handler.mongo.MongoQueryHandler;
import com.ospreydcs.dp.service.query.service.QueryServiceImpl;
import io.grpc.stub.StreamObserver;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class ResponseStreamDispatcher extends ResultDispatcher {

    private static final Logger LOGGER = LogManager.getLogger();
    public ResponseStreamDispatcher(StreamObserver<QueryResponse> responseObserver) {
        super(responseObserver);
    }

//    @Override
//    public void handleResult_(MongoCursor<BucketDocument> cursor) {
//
//        // build response from query result cursor
//        QueryResponse.QueryReport.QueryData.Builder resultDataBuilder =
//                QueryResponse.QueryReport.QueryData.newBuilder();
//
//        int messageSize = 0;
//        try {
//            while (cursor.hasNext()){
//                final BucketDocument document = cursor.next();
//                final QueryResponse.QueryReport.QueryData.DataBucket bucket =
//                        MongoQueryHandler.dataBucketFromDocument(document);
//                int bucketSerializedSize = bucket.getSerializedSize();
//                if (messageSize + bucketSerializedSize > MongoQueryHandler.MAX_GRPC_MESSAGE_SIZE) {
//                    // hit size limit for message so send current data response and create a new one
//                    LOGGER.debug("processQueryRequest: sending multiple responses for result");
//                    QueryServiceImpl.sendQueryResponseData(resultDataBuilder, getResponseObserver());
//                    messageSize = 0;
//                    resultDataBuilder = QueryResponse.QueryReport.QueryData.newBuilder();
//                }
//                resultDataBuilder.addDataBuckets(bucket);
//                messageSize = messageSize + bucketSerializedSize;
//            }
//
//            if (messageSize > 0) {
//                // send final data response
//                QueryServiceImpl.sendQueryResponseData(resultDataBuilder, getResponseObserver());
//            }
//
//        } catch (Exception ex) {
//            // send error response and close response stream
//            final String msg = ex.getMessage();
//            LOGGER.error("processQueryRequest: exception accessing result via cursor: " + msg);
//            QueryServiceImpl.sendQueryResponseError(msg, getResponseObserver());
//            cursor.close();
//            return;
//        }
//
//        cursor.close();
//        QueryServiceImpl.closeResponseStream(getResponseObserver());
//    }

    @Override
    public void handleResult_(MongoCursor<BucketDocument> cursor) {

        while (cursor.hasNext()) {
            QueryResponse response = super.nextQueryResponseFromCursor(cursor);

            if (response != null) {
                boolean isError = (response.getResponseType() == ResponseType.ERROR_RESPONSE);
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
