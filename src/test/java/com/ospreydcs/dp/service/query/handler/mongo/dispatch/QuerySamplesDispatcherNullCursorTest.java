package com.ospreydcs.dp.service.query.handler.mongo.dispatch;

import com.mongodb.client.MongoCursor;
import com.ospreydcs.dp.grpc.v1.common.ExceptionalResult;
import com.ospreydcs.dp.grpc.v1.query.QueryDataRequest;
import com.ospreydcs.dp.grpc.v1.query.QueryProviderStatsRequest;
import com.ospreydcs.dp.grpc.v1.query.QueryProvidersRequest;
import com.ospreydcs.dp.grpc.v1.query.QueryPvStatsRequest;
import com.ospreydcs.dp.grpc.v1.query.QuerySamplesResponse;
import com.ospreydcs.dp.grpc.v1.query.QueryTableRequest;
import com.ospreydcs.dp.service.common.bson.ProviderDocument;
import com.ospreydcs.dp.service.common.bson.ProviderMetadataQueryResultDocument;
import com.ospreydcs.dp.service.common.bson.PvMetadataQueryResultDocument;
import com.ospreydcs.dp.service.common.bson.bucket.BucketDocument;
import com.ospreydcs.dp.service.common.bson.dataset.DataBlockDocument;
import com.ospreydcs.dp.service.query.handler.model.KeysetPosition;
import com.ospreydcs.dp.service.query.handler.model.ResolvedQuery;
import com.ospreydcs.dp.service.query.handler.model.TimeInterval;
import com.ospreydcs.dp.service.query.handler.mongo.client.MongoQueryClientInterface;
import io.grpc.stub.StreamObserver;
import org.bson.conversions.Bson;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.Assert.*;

/**
 * Pins how the two querySamples dispatchers classify a null cursor from
 * {@code executeQuerySamplesV2} (issue #232, task 6). The client returns null both when nothing
 * overlaps the page window and when retrieval fails — a database error, or a failed pvStats span
 * read (plan D8). The dispatchers must screen the empty window themselves, before the database call,
 * and report a null they do receive as an error. Treating it as an empty page would silently return
 * no data, the same failure class as #197.
 *
 * <p>No database: the client is a stub that returns null and counts calls.
 */
public class QuerySamplesDispatcherNullCursorTest {

    private static final long B = 1_700_000_000L;
    private static final long BYTE_BUDGET = Long.MAX_VALUE;

    /** Returns null from every retrieval and records whether the samples retrieval was invoked. */
    private static final class NullCursorClient implements MongoQueryClientInterface {
        int samplesCalls = 0;

        @Override public boolean init() { return true; }
        @Override public boolean fini() { return true; }
        @Override public MongoCursor<BucketDocument> executeDataBlockQuery(DataBlockDocument d) { return null; }
        @Override public MongoCursor<BucketDocument> executeQueryData(QueryDataRequest.QuerySpec q) { return null; }
        @Override public MongoCursor<BucketDocument> executeQueryTable(QueryTableRequest r) { return null; }
        @Override public MongoCursor<PvMetadataQueryResultDocument> executeQueryPvStats(QueryPvStatsRequest r) { return null; }
        @Override public MongoCursor<PvMetadataQueryResultDocument> executeQueryPvStats(Collection<String> l) { return null; }
        @Override public MongoCursor<PvMetadataQueryResultDocument> executeQueryPvStats(String p) { return null; }
        @Override public Collection<String> executeQueryPvExistence(Collection<String> l) { return null; }
        @Override public List<String> resolvePvNamesByPattern(String p) { return List.of(); }
        @Override public List<String> resolvePvNamesByMetadata(List<Bson> f) { return List.of(); }
        @Override public List<TimeInterval> resolveConfigurationIntervals(List<Bson> f) { return List.of(); }
        @Override public MongoCursor<BucketDocument> executeQueryBucketsV2(ResolvedQuery q) { return null; }
        @Override public MongoCursor<BucketDocument> executeQueryBucketsV2Stream(ResolvedQuery q) { return null; }
        @Override public MongoCursor<BucketDocument> executeQuerySamplesV2(ResolvedQuery q, long bs, long bn) {
            samplesCalls++;
            return null;
        }
        @Override public Map<String, Set<Long>> resolveSampleStatusTimestamps(ResolvedQuery q, long bs, long bn) { return Map.of(); }
        @Override public MongoCursor<ProviderDocument> executeQueryProviders(QueryProvidersRequest r) { return null; }
        @Override public MongoCursor<ProviderMetadataQueryResultDocument> executeQueryProviderStats(QueryProviderStatsRequest r) { return null; }
        @Override public MongoCursor<ProviderMetadataQueryResultDocument> executeQueryProviderStats(String id) { return null; }
    }

    private static final class CapturingObserver implements StreamObserver<QuerySamplesResponse> {
        final List<QuerySamplesResponse> responses = new ArrayList<>();
        boolean completed = false;
        Throwable error = null;

        @Override public void onNext(QuerySamplesResponse value) { responses.add(value); }
        @Override public void onError(Throwable t) { error = t; }
        @Override public void onCompleted() { completed = true; }
    }

    private static ResolvedQuery sampleQuery(
            List<TimeInterval> intervals, KeysetPosition pageStart, boolean streaming) {
        return new ResolvedQuery(
                List.of("pv1"), intervals, 10, pageStart, false, false,
                ResolvedQuery.ResultMode.SAMPLE, streaming);
    }

    private static void assertSingleErrorResponse(CapturingObserver observer) {
        assertNull(observer.error);
        assertTrue(observer.completed);
        assertEquals(1, observer.responses.size());
        final QuerySamplesResponse response = observer.responses.get(0);
        assertTrue(response.hasExceptionalResult());
        assertEquals(
                ExceptionalResult.ExceptionalResultStatus.RESULT_STATUS_ERROR,
                response.getExceptionalResult().getExceptionalResultStatus());
        assertTrue(response.getExceptionalResult().getMessage().contains("null cursor"));
    }

    private static void assertSingleEmptyResponse(CapturingObserver observer) {
        assertNull(observer.error);
        assertTrue(observer.completed);
        assertEquals(1, observer.responses.size());
        final QuerySamplesResponse response = observer.responses.get(0);
        assertFalse(response.hasExceptionalResult());
        assertTrue(response.hasSampleQueryResult());
        assertEquals(0, response.getSampleQueryResult().getColumnTable()
                .getTimestampList().getTimestampsCount());
        assertTrue(response.getSampleQueryResult().getNextPageToken().isEmpty());
    }

    // -----------------------------------------------------------------------
    // unary querySamples
    // -----------------------------------------------------------------------

    @Test
    public void testUnaryNullCursorOnNonEmptyWindowIsError() {
        final NullCursorClient client = new NullCursorClient();
        final CapturingObserver observer = new CapturingObserver();
        final ResolvedQuery query = sampleQuery(
                List.of(new TimeInterval(B, 0, B + 10, 0)), null, false);

        new QuerySamplesUnaryDispatcher(observer, BYTE_BUDGET).executeAndDispatch(query, client);

        assertEquals(1, client.samplesCalls);
        assertSingleErrorResponse(observer);
    }

    @Test
    public void testUnaryEmptyWindowIsEmptyPageWithoutRetrieval() {
        // continuation page whose resume timestamp is at the only fragment's end: nothing overlaps
        final NullCursorClient client = new NullCursorClient();
        final CapturingObserver observer = new CapturingObserver();
        final ResolvedQuery query = sampleQuery(
                List.of(new TimeInterval(B, 0, B + 10, 0)), KeysetPosition.ofSample(B + 10, 0), false);

        new QuerySamplesUnaryDispatcher(observer, BYTE_BUDGET).executeAndDispatch(query, client);

        assertEquals("empty window must be screened before the database call", 0, client.samplesCalls);
        assertSingleEmptyResponse(observer);
    }

    // -----------------------------------------------------------------------
    // querySamplesStream
    // -----------------------------------------------------------------------

    @Test
    public void testStreamNullCursorOnNonEmptyWindowIsError() {
        final NullCursorClient client = new NullCursorClient();
        final CapturingObserver observer = new CapturingObserver();
        final ResolvedQuery query = sampleQuery(
                List.of(new TimeInterval(B, 0, B + 10, 0)), null, true);

        new QuerySamplesStreamDispatcher(observer, BYTE_BUDGET).executeAndDispatch(query, client);

        assertEquals(1, client.samplesCalls);
        assertSingleErrorResponse(observer);
    }

    @Test
    public void testStreamEmptyWindowIsSingleEmptyMessageWithoutRetrieval() {
        // an empty fragment (begin == end) is dropped by the clamp, leaving nothing to retrieve
        final NullCursorClient client = new NullCursorClient();
        final CapturingObserver observer = new CapturingObserver();
        final ResolvedQuery query = sampleQuery(
                List.of(new TimeInterval(B, 0, B, 0)), null, true);

        new QuerySamplesStreamDispatcher(observer, BYTE_BUDGET).executeAndDispatch(query, client);

        assertEquals("empty window must be screened before the database call", 0, client.samplesCalls);
        assertSingleEmptyResponse(observer);
    }
}
