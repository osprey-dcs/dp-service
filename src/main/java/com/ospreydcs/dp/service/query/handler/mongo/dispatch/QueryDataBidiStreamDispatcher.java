package com.ospreydcs.dp.service.query.handler.mongo.dispatch;

import com.mongodb.client.MongoCursor;
import com.ospreydcs.dp.grpc.v1.query.QueryDataResponse;
import com.ospreydcs.dp.service.common.bson.bucket.BucketDocument;
import com.ospreydcs.dp.service.common.exception.DpException;
import com.ospreydcs.dp.service.query.handler.mongo.MongoQueryHandler;
import com.ospreydcs.dp.service.query.service.QueryServiceImpl;
import io.grpc.stub.StreamObserver;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;

public class QueryDataBidiStreamDispatcher extends QueryDataAbstractDispatcher {

    // static variables
    private static final Logger logger = LogManager.getLogger();

    // instance variables
    private MongoCursor<BucketDocument> mongoCursor = null;
    private final Object cursorLock = new Object(); // used for synchronized access to cursor which is not thread safe
    private final AtomicBoolean cursorClosed = new AtomicBoolean(false);
    private QueryDataResponse.QueryData.DataBucket nextBucket = null;
    private int nextBucketSize = 0;

    public QueryDataBidiStreamDispatcher(StreamObserver<QueryDataResponse> responseObserver) {
        super(responseObserver);
    }

    private void sendNextResponse(MongoCursor<BucketDocument> cursor) {

        synchronized (cursorLock) {
            // mongo cursor is not thread safe so synchronize access

            logger.trace("entering sendNextResponse synchronized id: " + getResponseObserver().hashCode());

            if (cursor != null) {
                this.mongoCursor = cursor;
            }

            if (this.mongoCursor == null) {
                // we probably received a "next" request before we finished executing the query and handling initial results
                logger.trace("sending not ready response id: " + getResponseObserver().hashCode());
                final QueryDataResponse statusResponse = QueryServiceImpl.queryDataResponseNotReady();
                getResponseObserver().onNext(statusResponse);
                return;
            }

            // build next response from query result cursor
            final QueryDataResponse.QueryData.Builder queryDataBuilder =
                    QueryDataResponse.QueryData.newBuilder();
            int messageSize = 0;

            // add leftover bucket from previous attempt to send next response where we exceeded response message size limit
            if (this.nextBucket != null) {
                logger.trace("adding leftover bucket details id: " + getResponseObserver().hashCode());
                queryDataBuilder.addDataBuckets(this.nextBucket);
                messageSize = messageSize + this.nextBucketSize;
                this.nextBucket = null;
                this.nextBucketSize = 0;
            }

            boolean isError = false;
            String errorMsg = "";
            while ( ! this.cursorClosed.get() && this.mongoCursor.hasNext()){

                // get next BucketDocument from cursor
                final BucketDocument document = this.mongoCursor.next();

                // build DataBucket from BucketDocument
                QueryDataResponse.QueryData.DataBucket bucket = null;
                try {
                    bucket = BucketDocument.dataBucketFromDocument(document);
                } catch (DpException e) {
                    // exception deserialzing BucketDocument contents, so send error response
                    errorMsg =
                            "exception deserializing protobuf data for BucketDocument id: "
                                    + getResponseObserver().hashCode()
                                    + " exception: " + e.getMessage();
                    isError = true;
                    break;
                }
                Objects.requireNonNull(bucket);

                // determine bucket size
                int bucketSerializedSize = bucket.getSerializedSize();

                // check if bucket is larger than response message size limit
                if (bucketSerializedSize > MongoQueryHandler.getOutgoingMessageSizeLimitBytes()) {
                    errorMsg = "bucket size: " + bucketSerializedSize
                            + " greater than maximum message size: "
                            + MongoQueryHandler.getOutgoingMessageSizeLimitBytes();
                    isError = true;
                    break;
                }

                // save current bucket and break out of cursor handling loop if next bucket might exceed maximum size
                if (messageSize + bucketSerializedSize > MongoQueryHandler.getOutgoingMessageSizeLimitBytes()) {
                    logger.trace("reached response message size limit, saving next bucket details id: "
                            + getResponseObserver().hashCode());
                    this.nextBucket = bucket;
                    this.nextBucketSize = bucketSerializedSize;
                    break;
                }

                // add bucket to result
                queryDataBuilder.addDataBuckets(bucket);
                messageSize = messageSize + bucketSerializedSize;

            }

            if (isError) {
                // send error response
                logger.trace("error generating next response id: "
                        + getResponseObserver().hashCode() + " msg: " + errorMsg);
                QueryServiceImpl.sendQueryDataResponseError(errorMsg, getResponseObserver());
                this.cursorClosed.set(true);
                this.mongoCursor.close();
                return;

            } else {
                if (messageSize > 0) {
                    // send next query result response
                    QueryServiceImpl.sendQueryDataResponse(queryDataBuilder, getResponseObserver());
                }
            }

            // close cursor if we have exhausted it
            if ( ! this.cursorClosed.get()) {
                final boolean cursorHasNext = this.mongoCursor.hasNext();
                if (!cursorHasNext) {
                    logger.trace("closing cursor id: " + getResponseObserver().hashCode());
                    this.cursorClosed.set(true);
                    this.mongoCursor.close();

                    if (nextBucket == null) {
                        // close responses stream since cursor is exhausted and there are no pending buckets
                        logger.trace("closing response stream id: " + getResponseObserver().hashCode());
                        getResponseObserver().onCompleted();
                    }
                }
            }
        }

        logger.trace("exiting sendNextResponse synchronized id: " + getResponseObserver().hashCode());
    }

    @Override
    public void handleResult_(MongoCursor<BucketDocument> cursor) {
        sendNextResponse(cursor);
    }

    public void close() {
        if (cursorClosed.get()) {
            return;
        }
        synchronized (cursorLock) {
            // mongo cursor is not thread safe so synchronize access
            this.cursorClosed.set(true);
            this.mongoCursor.close();
            this.mongoCursor = null;
        }
    }

    public void next() {
        sendNextResponse(null);
    }

}
