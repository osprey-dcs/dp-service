package com.ospreydcs.dp.service.query;

import com.ospreydcs.dp.grpc.v1.common.DataBucket;
import com.ospreydcs.dp.grpc.v1.query.QueryDataResponse;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

/**
 * Pins the ordering the integration tests depend on from
 * {@link QueryTestBase.QueryDataResponseStreamObserver}: once {@code await()} returns for a stream,
 * every response delivered before {@code onCompleted()} has been handled, in the order delivered.
 *
 * <p>The observer handles callbacks off the calling thread. When each callback had a thread of its
 * own, the one for {@code onCompleted()} could release the latch before the one for
 * {@code onNext()} had stored its buckets, and a test then read an empty or partial result from a
 * query the service had answered correctly ({@code QueryAnnotationsIT} on main, 2026-11-25:
 * expected 2 buckets, was 0). A race, so each case is repeated.
 */
public class QueryDataResponseStreamObserverTest {

    private static final int REPETITIONS = 2_000;

    private static QueryDataResponse responseWithBuckets(String... names) {
        final QueryDataResponse.QueryData.Builder queryData = QueryDataResponse.QueryData.newBuilder();
        for (String name : names) {
            queryData.addDataBuckets(DataBucket.newBuilder().setPvName(name));
        }
        return QueryDataResponse.newBuilder().setQueryData(queryData).build();
    }

    private static List<String> names(List<DataBucket> buckets) {
        final List<String> names = new ArrayList<>();
        // copy under the list's lock; the observer's list is a synchronized one
        synchronized (buckets) {
            for (DataBucket bucket : buckets) {
                names.add(bucket.getPvName());
            }
        }
        return names;
    }

    @Test
    public void testAwaitReturnsOnlyAfterEveryResponseIsHandled() {

        for (int i = 0; i < REPETITIONS; i++) {
            final QueryTestBase.QueryDataResponseStreamObserver observer =
                    QueryTestBase.QueryDataResponseStreamObserver.newQueryDataStreamObserver();

            observer.onNext(responseWithBuckets("pv-1", "pv-2"));
            observer.onCompleted();
            observer.await();

            assertFalse(observer.getErrorMessage(), observer.isError());
            assertEquals("repetition " + i, List.of("pv-1", "pv-2"), names(observer.getDataBucketList()));
        }
    }

    @Test
    public void testResponsesAreHandledInTheOrderDelivered() {

        for (int i = 0; i < REPETITIONS; i++) {
            final QueryTestBase.QueryDataResponseStreamObserver observer =
                    QueryTestBase.QueryDataResponseStreamObserver.newQueryDataStreamObserver();

            observer.onNext(responseWithBuckets("pv-1"));
            observer.onNext(responseWithBuckets("pv-2"));
            observer.onNext(responseWithBuckets("pv-3"));
            observer.onCompleted();
            observer.await();

            assertEquals(
                    "repetition " + i,
                    List.of("pv-1", "pv-2", "pv-3"), names(observer.getDataBucketList()));
        }
    }
}
