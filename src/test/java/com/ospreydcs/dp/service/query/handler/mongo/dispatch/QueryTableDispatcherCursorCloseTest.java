package com.ospreydcs.dp.service.query.handler.mongo.dispatch;

import com.mongodb.ServerAddress;
import com.mongodb.ServerCursor;
import com.mongodb.client.MongoCursor;
import com.ospreydcs.dp.grpc.v1.common.Timestamp;
import com.ospreydcs.dp.grpc.v1.query.QueryTableRequest;
import com.ospreydcs.dp.grpc.v1.query.QueryTableResponse;
import com.ospreydcs.dp.service.common.bson.bucket.BucketDocument;
import com.ospreydcs.dp.service.query.handler.QueryTelemetry;
import io.grpc.stub.StreamObserver;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.NoSuchElementException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * {@code QueryTableDispatcher} must close the cursor it is handed on every exit, including the
 * ones that read it to its end (PR #323 review). A request whose PVs fall in more than one span
 * class gets a cursor that owns the explicit session its finds share (#314), and that session
 * ends only when the cursor is closed; an exhausted cursor no longer releases it by itself. The
 * empty result is the exit that used to return without closing.
 *
 * <p>No database: the cursor is a stub that counts closes.
 */
public class QueryTableDispatcherCursorCloseTest {

    private static final class EmptyCursor implements MongoCursor<BucketDocument> {
        int closes = 0;

        @Override public void close() { closes++; }
        @Override public boolean hasNext() { return false; }
        @Override public BucketDocument next() { throw new NoSuchElementException(); }
        @Override public int available() { return 0; }
        @Override public BucketDocument tryNext() { return null; }
        @Override public ServerCursor getServerCursor() { return null; }
        @Override public ServerAddress getServerAddress() { return null; }
    }

    private static final class CapturingObserver implements StreamObserver<QueryTableResponse> {
        final List<QueryTableResponse> responses = new ArrayList<>();
        boolean completed = false;

        @Override public void onNext(QueryTableResponse value) { responses.add(value); }
        @Override public void onError(Throwable t) { }
        @Override public void onCompleted() { completed = true; }
    }

    @Test
    public void testEmptyResultClosesTheCursor() {
        final QueryTableRequest request = QueryTableRequest.newBuilder()
                .setFormat(QueryTableRequest.TableResultFormat.TABLE_FORMAT_COLUMN)
                .setBeginTime(Timestamp.newBuilder().setEpochSeconds(1_700_000_000L))
                .setEndTime(Timestamp.newBuilder().setEpochSeconds(1_700_000_010L))
                .build();
        final CapturingObserver observer = new CapturingObserver();
        final EmptyCursor cursor = new EmptyCursor();

        new QueryTableDispatcher(observer, request, new QueryTelemetry("queryTableTest")).handleResult(cursor);

        assertEquals("the empty result is still answered", 1, observer.responses.size());
        assertTrue(observer.completed);
        assertEquals("the cursor must be closed on the empty-result exit", 1, cursor.closes);
    }
}
