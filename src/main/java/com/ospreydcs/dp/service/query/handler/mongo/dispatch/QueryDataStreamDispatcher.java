package com.ospreydcs.dp.service.query.handler.mongo.dispatch;

import com.mongodb.client.MongoCursor;
import com.ospreydcs.dp.grpc.v1.query.QueryDataRequest;
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

    // static variables
    private static final Logger logger = LogManager.getLogger();

    public QueryDataStreamDispatcher(
            StreamObserver<QueryDataResponse> responseObserver,
            QueryDataRequest.QuerySpec querySpec
    ) {
        super(responseObserver, querySpec);
    }

    @Override
    public void handleResult_(MongoCursor<BucketDocument> cursor) {

        // send stream of responses from query result cursor

        QueryDataResponse.QueryData.Builder queryDataBuilder =
                QueryDataResponse.QueryData.newBuilder();
        int messageSize = 0;

        boolean emptyResponse = true;
        boolean isError = false;
        String errorMsg = "";
        while (cursor.hasNext()) {
            emptyResponse = false;

            final BucketDocument document = cursor.next();

            // create result DataBucket from BucketDocument
            QueryDataResponse.QueryData.DataBucket bucket = null;
            try {
                bucket = BucketDocument.dataBucketFromDocument(document, querySpec);
            } catch (DpException e) {
                // exception deserialzing BucketDocument contents, so send error response
                isError = true;
                errorMsg =
                        "exception deserializing protobuf data for BucketDocument id: " + getResponseObserver().hashCode()
                                + " exception: " + e.getMessage();
                logger.error(errorMsg);
                break;
            }
            Objects.requireNonNull(bucket);

            // determine bucket size
            int bucketSerializedSize = bucket.getSerializedSize();

            // check if single bucket exceeds maximum response message size limit
            if (bucketSerializedSize > MongoQueryHandler.getOutgoingMessageSizeLimitBytes()) {
                // single bucket is larger than maximum message size, so send error response
                isError = true;
                errorMsg = "bucket size: " + bucketSerializedSize
                        + " greater than maximum message size: "
                        + MongoQueryHandler.getOutgoingMessageSizeLimitBytes();
                logger.error(errorMsg);
                break;
            }

            // send current response and start a new one if bucket size makes us exceed response message size limit
            if (messageSize + bucketSerializedSize > MongoQueryHandler.getOutgoingMessageSizeLimitBytes()) {
                logger.trace("sending intermediate response id: " + getResponseObserver().hashCode());
                QueryServiceImpl.sendQueryDataResponse(queryDataBuilder, getResponseObserver());
                queryDataBuilder = QueryDataResponse.QueryData.newBuilder();
                messageSize = 0;
            }

            // add bucket to response message and update message size
            queryDataBuilder.addDataBuckets(bucket);
            messageSize = messageSize + bucketSerializedSize;
        }

        // close database cursor
        logger.trace("closing cursor id: " + getResponseObserver().hashCode());
        cursor.close();

        if ( ! isError) {

            // send empty response message if cursor is empty
            if (emptyResponse) {
                logger.trace("sending empty response id: " + getResponseObserver().hashCode());
                QueryServiceImpl.sendQueryDataResponse(queryDataBuilder, getResponseObserver());
            }

            // send last response message
            else if (messageSize > 0) {
                logger.trace("sending residual response id: " + getResponseObserver().hashCode());
                QueryServiceImpl.sendQueryDataResponse(queryDataBuilder, getResponseObserver());
            }

            // close response stream
            logger.trace("closing response stream id: " + getResponseObserver().hashCode());
            getResponseObserver().onCompleted();

        } else {
            logger.trace("sending error response id: " + getResponseObserver().hashCode() + " msg: " + errorMsg);
            QueryServiceImpl.sendQueryDataResponseError(errorMsg, getResponseObserver());
        }
    }

}
