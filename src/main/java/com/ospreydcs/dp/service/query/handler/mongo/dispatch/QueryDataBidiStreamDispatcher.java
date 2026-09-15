package com.ospreydcs.dp.service.query.handler.mongo.dispatch;

import com.mongodb.client.MongoCursor;
import com.ospreydcs.dp.grpc.v1.common.DataBucket;
import com.ospreydcs.dp.grpc.v1.query.QueryDataRequest;
import com.ospreydcs.dp.grpc.v1.query.QueryDataResponse;
import com.ospreydcs.dp.service.common.bson.bucket.BucketDocument;
import com.ospreydcs.dp.service.common.exception.DpException;
import com.ospreydcs.dp.service.query.handler.QueryTelemetry;
import com.ospreydcs.dp.service.query.handler.mongo.MongoQueryHandler;
import com.ospreydcs.dp.service.query.service.QueryServiceImpl;
import io.grpc.stub.StreamObserver;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Bidirectional-streaming {@code queryDataBidiStream} formatter: the client pulls one response at a
 * time via {@code next()}, so this dispatcher outlives the job that created it.
 *
 * <p><b>Telemetry caveat (issue #212, out of scope note).</b> The {@code QueryTelemetry} for this
 * request is completed by {@code QueryDataJob}'s {@code finally}, which runs when the first response
 * has been sent -- not when the stream ends. So {@code dp.query.stage.duration} for
 * {@code queryDataBidiStream} measures the time to the first response, and the response counters
 * count only what was sent by then. The later pulls are still measured where it matters: their
 * MongoDB round-trips appear in {@code db.client.operation.duration} (D4) and their job-free work is
 * on the gRPC thread, which {@code grpc.server.call.duration} covers for the whole call.
 *
 * <p>Recording per pull instead would mean either completing the telemetry once per {@code next()}
 * -- which counts one client request as many in {@code dp.query.requests}, inflating the request
 * rate by the client's page count -- or holding the context open until the stream closes, which for
 * an abandoned stream is never. Neither is worth it for a legacy client path; a per-request context
 * keyed to the stream lifecycle is the follow-on if this path ever matters again.
 */
public class QueryDataBidiStreamDispatcher extends QueryDataAbstractDispatcher {

    // static variables
    private static final Logger logger = LogManager.getLogger();

    // instance variables
    private MongoCursor<BucketDocument> mongoCursor = null;
    private final Object cursorLock = new Object(); // used for synchronized access to cursor which is not thread safe
    private final AtomicBoolean cursorClosed = new AtomicBoolean(false);
    private DataBucket nextBucket = null;
    private int nextBucketSize = 0;

    public QueryDataBidiStreamDispatcher(
            StreamObserver<QueryDataResponse> responseObserver,
            QueryDataRequest.QuerySpec querySpec,
            QueryTelemetry telemetry
    ) {
        super(responseObserver, querySpec, telemetry);
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
                telemetry.recordResponse(statusResponse.getSerializedSize());
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
                DataBucket bucket = null;
                try {
                    bucket = BucketDocument.dataBucketFromDocument(document, querySpec);
                } catch (DpException e) {
                    // exception deserializing BucketDocument contents, so send error response.
                    // e.getMessage() identifies the offending document by id and pvName, which is
                    // what an operator needs to locate and repair it.
                    errorMsg = "exception deserializing protobuf data for bucket: " + e.getMessage();
                    logger.error(errorMsg, e);
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
                telemetry.markError();
                QueryServiceImpl.sendQueryDataResponseError(errorMsg, getResponseObserver());
                this.cursorClosed.set(true);
                this.mongoCursor.close();
                recordCursorTime(this.mongoCursor);
                return;

            } else {
                // send next query result response
                logger.trace("sending query result response id: " + getResponseObserver().hashCode());
                telemetry.recordResponse(queryDataBuilder.build().getSerializedSize());
                QueryServiceImpl.sendQueryDataResponse(queryDataBuilder, getResponseObserver());
            }

            // close cursor if we have exhausted it
            if ( ! this.cursorClosed.get()) {
                final boolean cursorHasNext = this.mongoCursor.hasNext();
                if (!cursorHasNext) {
                    logger.trace("closing cursor id: " + getResponseObserver().hashCode());
                    this.cursorClosed.set(true);
                    this.mongoCursor.close();
                    recordCursorTime(this.mongoCursor);

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
