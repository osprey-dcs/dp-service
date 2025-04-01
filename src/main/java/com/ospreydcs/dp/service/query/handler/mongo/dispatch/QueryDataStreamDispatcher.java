package com.ospreydcs.dp.service.query.handler.mongo.dispatch;

import com.mongodb.client.MongoCursor;
import com.ospreydcs.dp.grpc.v1.common.ExceptionalResult;
import com.ospreydcs.dp.grpc.v1.query.QueryDataResponse;
import com.ospreydcs.dp.service.common.bson.bucket.BucketDocument;
import com.ospreydcs.dp.service.common.exception.DpException;
import com.ospreydcs.dp.service.query.handler.mongo.MongoQueryHandler;
import com.ospreydcs.dp.service.query.service.QueryServiceImpl;
import io.grpc.stub.StreamObserver;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Objects;

public class QueryDataStreamDispatcher extends QueryDataAbstractDispatcher {

    private static final Logger logger = LogManager.getLogger();
    public QueryDataStreamDispatcher(StreamObserver<QueryDataResponse> responseObserver) {
        super(responseObserver);
    }

    @Override
    public void handleResult_(MongoCursor<BucketDocument> cursor) {

        // send stream of responses from query result cursor

        QueryDataResponse.QueryData.Builder queryDataBuilder =
                QueryDataResponse.QueryData.newBuilder();
        int messageSize = 0;

        boolean isError = false;
        while (cursor.hasNext()){

            final BucketDocument document = cursor.next();

            // create result DataBucket from BucketDocument
            QueryDataResponse.QueryData.DataBucket bucket = null;
            try {
                bucket = BucketDocument.dataBucketFromDocument(document);
            } catch (DpException e) {
                // exception deserialzing BucketDocument contents, so send error response
                isError = true;
                final String errorMsg =
                        "exception deserializing protobuf data for BucketDocument id: " + getResponseObserver().hashCode()
                                + " exception: " + e.getMessage();
                logger.error(errorMsg);
                QueryServiceImpl.sendQueryDataResponseError(errorMsg, getResponseObserver());
                break;
            }
            Objects.requireNonNull(bucket);

            // determine bucket size
            int bucketSerializedSize = bucket.getSerializedSize();

            // check if single bucket exceeds maximum response message size limit
            if (bucketSerializedSize > MongoQueryHandler.getOutgoingMessageSizeLimitBytes()) {
                // single bucket is larger than maximum message size, so send error response
                isError = true;
                final String errorMsg = "bucket size: " + bucketSerializedSize
                        + " greater than maximum message size: "
                        + MongoQueryHandler.getOutgoingMessageSizeLimitBytes();
                logger.error(errorMsg);
                QueryServiceImpl.sendQueryDataResponseError(errorMsg, getResponseObserver());
                break;
            }

            // send current response and start a new one if bucket size makes us exceed response message size limit
            if (messageSize + bucketSerializedSize > MongoQueryHandler.getOutgoingMessageSizeLimitBytes()) {
                logger.trace(
                        "query data response message size limit exceeded for id: {}, sending intermediate response",
                        getResponseObserver().hashCode());
                QueryServiceImpl.sendQueryDataResponse(queryDataBuilder, getResponseObserver());
                queryDataBuilder = QueryDataResponse.QueryData.newBuilder();
                messageSize = 0;
            }

            // add bucket to response message and update message size
            queryDataBuilder.addDataBuckets(bucket);
            messageSize = messageSize + bucketSerializedSize;
        }

        // close database cursor
        cursor.close();

        if (!isError) {
            // send last response message
            if (messageSize > 0) {
                QueryServiceImpl.sendQueryDataResponse(queryDataBuilder, getResponseObserver());
            }

            // close response stream and cursor
            getResponseObserver().onCompleted();
        }
    }

}
