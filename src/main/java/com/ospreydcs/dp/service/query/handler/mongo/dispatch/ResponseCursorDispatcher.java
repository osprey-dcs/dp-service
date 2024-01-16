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

import java.util.concurrent.atomic.AtomicBoolean;

public class ResponseCursorDispatcher extends ResultDispatcher {

    // static variables
    private static final Logger LOGGER = LogManager.getLogger();

    // instance variables
    private MongoCursor<BucketDocument> mongoCursor = null;
    private final Object cursorLock = new Object(); // used for synchronized access to cursor which is not thread safe
    private AtomicBoolean cursorClosed = new AtomicBoolean(false);

    public ResponseCursorDispatcher(StreamObserver<QueryResponse> responseObserver) {
        super(responseObserver);
    }

    private void sendNextResponse(MongoCursor<BucketDocument> cursor) {

        if (this.cursorClosed.get()) {
            return;
        }

        synchronized (cursorLock) {
            // mongo cursor is not thread safe so synchronize access

            if (cursor != null) {
                this.mongoCursor = cursor;
            }

            if (this.mongoCursor == null) {
                // we probably received a "next" request before we finished executing the query and handling initial results
                final QueryResponse statusResponse = QueryServiceImpl.queryResponseNotReady();
                getResponseObserver().onNext(statusResponse);
                return;
            }

            final QueryResponse response = super.nextQueryResponseFromCursor(this.mongoCursor);
            boolean isError = false;
            if (response != null) {
                isError = (response.getResponseType() == ResponseType.ERROR_RESPONSE);
                getResponseObserver().onNext(response);

            } else {
                // this is unexpected so send an error response because it indicates a bug
                final String msg = "unexpected null response from nextQueryResponseFromCursor";
                final QueryResponse errorResponse = QueryServiceImpl.queryResponseError(msg);
                getResponseObserver().onNext(errorResponse);
            }

            // close stream if there is no response to send or we sent an error
            if (response == null || isError) {
                LOGGER.debug("sendNextResponse closing cursor");
                getResponseObserver().onCompleted();
                this.cursorClosed.set(true);
                this.mongoCursor.close();
            }
        }
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
