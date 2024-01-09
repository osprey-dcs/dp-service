package com.ospreydcs.dp.service.query.handler.mongo.dispatch;

import com.mongodb.client.MongoCursor;
import com.ospreydcs.dp.grpc.v1.query.QueryResponse;
import com.ospreydcs.dp.service.common.bson.BucketDocument;
import com.ospreydcs.dp.service.query.handler.mongo.MongoQueryHandler;
import com.ospreydcs.dp.service.query.service.QueryServiceImpl;
import io.grpc.stub.StreamObserver;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public abstract class ResultDispatcher {

    private static final Logger LOGGER = LogManager.getLogger();

    final private StreamObserver<QueryResponse> responseObserver;

    public ResultDispatcher(StreamObserver<QueryResponse> responseObserver) {
        this.responseObserver = responseObserver;
    }

    protected abstract void handleResult_(MongoCursor<BucketDocument> cursor);

    protected StreamObserver<QueryResponse> getResponseObserver() {
        return this.responseObserver;
    }

    public void handleResult(MongoCursor<BucketDocument> cursor) {

        // send error response and close response stream if cursor is null
        if (cursor == null) {
            final String msg = "executeQuery returned null cursor";
            LOGGER.error(msg);
            QueryServiceImpl.sendQueryResponseError(msg, getResponseObserver());
            return;
        }

        // send empty QueryStatus and close response stream if query matched no data
        if (!cursor.hasNext()) {
            LOGGER.debug("processQueryRequest: query matched no data, cursor is empty");
            QueryServiceImpl.sendQueryResponseEmpty(getResponseObserver());
            return;
        }

        handleResult_(cursor);
    }

    protected QueryResponse nextQueryResponseFromCursor(MongoCursor<BucketDocument> cursor) {

        // build response from query result cursor
        QueryResponse.QueryReport.QueryData.Builder resultDataBuilder =
                QueryResponse.QueryReport.QueryData.newBuilder();

        int messageSize = 0;
        while (cursor.hasNext()){

            final BucketDocument document = cursor.next();
            final QueryResponse.QueryReport.QueryData.DataBucket bucket =
                    MongoQueryHandler.dataBucketFromDocument(document);

            // determine bucket size and check if too large
            int bucketSerializedSize = bucket.getSerializedSize();
            if (bucketSerializedSize > MongoQueryHandler.MAX_GRPC_MESSAGE_SIZE) {
                // single bucket is larger than maximum message size, so send error response
                return QueryServiceImpl.queryResponseError(
                        "bucket size: " + bucketSerializedSize
                                + " greater than maximum message size: " + MongoQueryHandler.MAX_GRPC_MESSAGE_SIZE);
            }

            // add bucket to result
            resultDataBuilder.addDataBuckets(bucket);
            messageSize = messageSize + bucketSerializedSize;

            // break out of cursor handling loop if next bucket might exceed maximum size
            if (messageSize + bucketSerializedSize > MongoQueryHandler.MAX_GRPC_MESSAGE_SIZE) {
                break;
            }
        }

        if (messageSize > 0) {
            // create response from buckets in result
            return QueryServiceImpl.queryResponseData(resultDataBuilder);
        }

        return null;
    }

}
