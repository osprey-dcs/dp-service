package com.ospreydcs.dp.service.query.handler.mongo.dispatch;

import com.mongodb.ServerAddress;
import com.mongodb.ServerCursor;
import com.mongodb.client.MongoCursor;
import com.ospreydcs.dp.grpc.v1.common.ExceptionalResult;
import com.ospreydcs.dp.grpc.v1.query.QueryDataRequest;
import com.ospreydcs.dp.grpc.v1.query.QueryDataResponse;
import com.ospreydcs.dp.service.common.bson.bucket.BucketDocument;
import com.ospreydcs.dp.service.common.grpc.FakeServerCallStreamObserver;
import com.ospreydcs.dp.service.common.telemetry.DpMetrics;
import com.ospreydcs.dp.service.query.handler.QueryTelemetry;
import org.junit.Test;

import java.util.Iterator;
import java.util.List;

import static org.junit.Assert.*;

/**
 * Pins what {@code queryDataStream} does with an error when the client has cancelled (#326):
 * nothing is sent, the call is not completed, and the outcome is still {@code error}, because the
 * request failed whoever is left to be told. Each case has a control with a client that is still
 * there, so a test cannot pass because the error path was never reached.
 *
 * <p>No database: the dispatcher is handed a null cursor, or an in-memory cursor holding a bucket
 * that fails deserialization.
 */
public class QueryDataStreamDispatcherCancelledErrorTest {

    private static final class ListCursor implements MongoCursor<BucketDocument> {
        private final Iterator<BucketDocument> iterator;

        ListCursor(List<BucketDocument> buckets) {
            this.iterator = buckets.iterator();
        }

        @Override public void close() { }
        @Override public boolean hasNext() { return iterator.hasNext(); }
        @Override public BucketDocument next() { return iterator.next(); }
        @Override public int available() { return 0; }
        @Override public BucketDocument tryNext() { return hasNext() ? next() : null; }
        @Override public ServerCursor getServerCursor() { return null; }
        @Override public ServerAddress getServerAddress() { return null; }
    }

    /** A bucket with no column or timestamps: deserialization fails as a DpException. */
    private static MongoCursor<BucketDocument> malformedBucketCursor() {
        final BucketDocument bucket = new BucketDocument();
        bucket.setId("malformed");
        bucket.setPvName("pv1");
        return new ListCursor(List.of(bucket));
    }

    private static FakeServerCallStreamObserver<QueryDataResponse> dispatch(
            MongoCursor<BucketDocument> cursor, boolean cancelled, QueryTelemetry telemetry) {
        final FakeServerCallStreamObserver<QueryDataResponse> observer = new FakeServerCallStreamObserver<>();
        observer.cancelled.set(cancelled);
        new QueryDataStreamDispatcher(observer, QueryDataRequest.QuerySpec.getDefaultInstance(), telemetry)
                .handleResult(cursor);
        return observer;
    }

    private static void assertErrorSent(
            FakeServerCallStreamObserver<QueryDataResponse> observer, QueryTelemetry telemetry) {
        assertEquals(1, observer.messages.size());
        assertEquals(
                ExceptionalResult.ExceptionalResultStatus.RESULT_STATUS_ERROR,
                observer.messages.get(0).getExceptionalResult().getExceptionalResultStatus());
        assertTrue(observer.completed);
        assertEquals(DpMetrics.OUTCOME_ERROR, telemetry.getOutcome());
    }

    private static void assertErrorNotSent(
            FakeServerCallStreamObserver<QueryDataResponse> observer, QueryTelemetry telemetry) {
        assertEquals("nothing may be sent to a cancelled client", 0, observer.messages.size());
        assertFalse("a call with no reader must not be completed", observer.completed);
        assertEquals("an error stays an error when the client has gone",
                DpMetrics.OUTCOME_ERROR, telemetry.getOutcome());
    }

    @Test
    public void testNullCursorErrorIsSentToAPresentClient() {
        final QueryTelemetry telemetry = new QueryTelemetry("queryDataStreamTest");
        assertErrorSent(dispatch(null, false, telemetry), telemetry);
    }

    @Test
    public void testNullCursorErrorIsNotSentToACancelledClient() {
        final QueryTelemetry telemetry = new QueryTelemetry("queryDataStreamTest");
        assertErrorNotSent(dispatch(null, true, telemetry), telemetry);
    }

    @Test
    public void testDeserializationErrorIsSentToAPresentClient() {
        final QueryTelemetry telemetry = new QueryTelemetry("queryDataStreamTest");
        assertErrorSent(dispatch(malformedBucketCursor(), false, telemetry), telemetry);
    }

    @Test
    public void testDeserializationErrorIsNotSentToACancelledClient() {
        final QueryTelemetry telemetry = new QueryTelemetry("queryDataStreamTest");
        assertErrorNotSent(dispatch(malformedBucketCursor(), true, telemetry), telemetry);
    }
}
