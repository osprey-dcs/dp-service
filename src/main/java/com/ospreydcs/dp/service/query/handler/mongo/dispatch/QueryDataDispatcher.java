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

public class QueryDataDispatcher extends QueryDataAbstractDispatcher {

    // static variables
    private static final Logger logger = LogManager.getLogger();

    public QueryDataDispatcher(
            StreamObserver<QueryDataResponse> responseObserver,
            QueryDataRequest.QuerySpec querySpec
    ) {
        super(responseObserver, querySpec);
    }

    @Override
    public void handleResult_(MongoCursor<BucketDocument> cursor) {

        // build response from query result cursor
        final QueryDataResponse.QueryData.Builder queryDataBuilder =
                QueryDataResponse.QueryData.newBuilder();
        int messageSize = 0;

        boolean isError = false;
        String errorMsg = "";
        while (cursor.hasNext()){

            final BucketDocument document = cursor.next();
            QueryDataResponse.QueryData.DataBucket bucket = null;
            try {
                bucket = BucketDocument.dataBucketFromDocument(document, querySpec);
            } catch (DpException e) {
                // exception deserializing BucketDocument contents, so send error response
                isError = true;
                errorMsg =
                        "exception deserializing protobuf data for BucketDocument id: "
                                + getResponseObserver().hashCode()
                                + " exception: " + e.getMessage();
                logger.error(errorMsg);
                break;
            }
            Objects.requireNonNull(bucket);

            // determine bucket size
            int bucketSerializedSize = bucket.getSerializedSize();

            // check if bucket size exceeds response message size limit
            if (bucketSerializedSize > MongoQueryHandler.getOutgoingMessageSizeLimitBytes()) {
                // single bucket is larger than maximum message size, so send error response
                isError = true;
                errorMsg = "bucket size: " + bucketSerializedSize
                        + " greater than maximum message size: " + MongoQueryHandler.getOutgoingMessageSizeLimitBytes();
                break;
            }

            // add bucket to result
            queryDataBuilder.addDataBuckets(bucket);
            messageSize = messageSize + bucketSerializedSize;

            if (messageSize > MongoQueryHandler.getOutgoingMessageSizeLimitBytes()) {
                // query response exceeds message size limit
                isError = true;
                errorMsg = "query returned more data than will fit in single QueryResponse message";
                break;
            }
        }

        cursor.close();

        if (isError) {
            logger.trace("sending error response id: " + getResponseObserver().hashCode() + " msg: " + errorMsg);
            QueryServiceImpl.sendQueryDataResponseError(errorMsg, getResponseObserver());

        } else {
            // create response from buckets in result
            final QueryDataResponse response = QueryServiceImpl.queryDataResponse(queryDataBuilder);
            logger.trace("sending query response and closing response stream id: " + getResponseObserver().hashCode());
            getResponseObserver().onNext(response);
            getResponseObserver().onCompleted();
        }
    }

}
