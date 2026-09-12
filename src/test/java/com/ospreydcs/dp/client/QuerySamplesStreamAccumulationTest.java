package com.ospreydcs.dp.client;

import com.ospreydcs.dp.grpc.v1.common.DataColumn;
import com.ospreydcs.dp.grpc.v1.common.DataValue;
import com.ospreydcs.dp.grpc.v1.common.ExceptionalResult;
import com.ospreydcs.dp.grpc.v1.common.SerializedDataColumn;
import com.ospreydcs.dp.grpc.v1.common.Timestamp;
import com.ospreydcs.dp.grpc.v1.common.TimestampList;
import com.ospreydcs.dp.grpc.v1.query.ColumnTable;
import com.ospreydcs.dp.grpc.v1.query.QuerySamplesResponse;
import com.google.protobuf.ByteString;
import org.junit.Test;

import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Unit coverage for QueryClient.QuerySamplesStreamResponseObserver's page accumulation.
 *
 * <p>Accumulating a streamed ColumnTable is the one piece of genuine logic in the streaming
 * wrapper, and its failure mode is a wrong answer rather than an error: merging columns by
 * position instead of by name would silently mis-align an entire PV's values against the
 * timestamp axis.  These tests drive the observer directly rather than through a service, so the
 * page shapes that are hard to produce from a real server — a reordered column set, a differing
 * column set, a short column — can be exercised.
 */
public class QuerySamplesStreamAccumulationTest {

    private static Timestamp ts(long seconds, long nanos) {
        return Timestamp.newBuilder().setEpochSeconds(seconds).setNanoseconds(nanos).build();
    }

    private static DataColumn column(String name, double... values) {
        final DataColumn.Builder builder = DataColumn.newBuilder().setName(name);
        for (double value : values) {
            builder.addDataValues(DataValue.newBuilder().setDoubleValue(value));
        }
        return builder.build();
    }

    private static QuerySamplesResponse page(List<Timestamp> timestamps, DataColumn... columns) {
        final ColumnTable.Builder tableBuilder = ColumnTable.newBuilder()
                .setTimestampList(TimestampList.newBuilder().addAllTimestamps(timestamps));
        for (DataColumn column : columns) {
            tableBuilder.addDataColumns(column);
        }
        return QuerySamplesResponse.newBuilder()
                .setSampleQueryResult(QuerySamplesResponse.SampleQueryResult.newBuilder()
                        .setColumnTable(tableBuilder))
                .build();
    }

    /**
     * Two pages concatenate along the timestamp axis and append per column.
     */
    @Test
    public void testAccumulatesSuccessivePages() {

        final QueryClient.QuerySamplesStreamResponseObserver observer =
                new QueryClient.QuerySamplesStreamResponseObserver();

        observer.onNext(page(List.of(ts(1, 0), ts(2, 0)), column("pvA", 1.0, 2.0), column("pvB", 10.0, 20.0)));
        observer.onNext(page(List.of(ts(3, 0)), column("pvA", 3.0), column("pvB", 30.0)));
        observer.onCompleted();
        observer.await();

        assertFalse(observer.getErrorMessage(), observer.isError());

        final ColumnTable table = observer.getColumnTable();
        assertEquals(3, table.getTimestampList().getTimestampsCount());
        assertEquals(List.of(ts(1, 0), ts(2, 0), ts(3, 0)), table.getTimestampList().getTimestampsList());
        assertEquals(2, table.getDataColumnsCount());
        assertEquals(column("pvA", 1.0, 2.0, 3.0), table.getDataColumns(0));
        assertEquals(column("pvB", 10.0, 20.0, 30.0), table.getDataColumns(1));
    }

    /**
     * A page whose columns arrive in a different ORDER merges correctly, because the merge is by
     * name.  Merging by index would swap the two PVs' values from this page onward — a wrong
     * answer with no error, which is the whole reason the merge is by name.
     */
    @Test
    public void testMergesByNameNotByPosition() {

        final QueryClient.QuerySamplesStreamResponseObserver observer =
                new QueryClient.QuerySamplesStreamResponseObserver();

        observer.onNext(page(List.of(ts(1, 0)), column("pvA", 1.0), column("pvB", 10.0)));
        // same column set, reversed order
        observer.onNext(page(List.of(ts(2, 0)), column("pvB", 20.0), column("pvA", 2.0)));
        observer.onCompleted();
        observer.await();

        assertFalse(observer.getErrorMessage(), observer.isError());

        final ColumnTable table = observer.getColumnTable();
        // column order follows the FIRST page, and each column carries its own values
        assertEquals(column("pvA", 1.0, 2.0), table.getDataColumns(0));
        assertEquals(column("pvB", 10.0, 20.0), table.getDataColumns(1));
    }

    /**
     * A page whose column SET differs from the first page's fails the call.  The missing column's
     * rows cannot be filled in, so appending the rest would leave that column short against the
     * timestamp axis — reporting a partial table would be worse than reporting nothing.
     */
    @Test
    public void testDifferingColumnSetIsHardFailure() {

        final QueryClient.QuerySamplesStreamResponseObserver observer =
                new QueryClient.QuerySamplesStreamResponseObserver();

        observer.onNext(page(List.of(ts(1, 0)), column("pvA", 1.0), column("pvB", 10.0)));
        observer.onNext(page(List.of(ts(2, 0)), column("pvA", 2.0)));
        observer.await();

        assertTrue(observer.isError());
        assertTrue(observer.getErrorMessage(), observer.getErrorMessage().contains("column set differs"));
    }

    /**
     * An extra column in a later page is equally a failure — the accumulated table has no values
     * for it over the earlier pages.
     */
    @Test
    public void testExtraColumnInLaterPageIsHardFailure() {

        final QueryClient.QuerySamplesStreamResponseObserver observer =
                new QueryClient.QuerySamplesStreamResponseObserver();

        observer.onNext(page(List.of(ts(1, 0)), column("pvA", 1.0)));
        observer.onNext(page(List.of(ts(2, 0)), column("pvA", 2.0), column("pvC", 5.0)));
        observer.await();

        assertTrue(observer.isError());
        assertTrue(observer.getErrorMessage(), observer.getErrorMessage().contains("column set differs"));
    }

    /**
     * A column carrying fewer values than the page has timestamps fails rather than being padded:
     * the missing positions are not knowable, and silently shifting the remaining values against
     * the axis is the mis-alignment this observer exists to prevent.
     */
    @Test
    public void testShortColumnIsHardFailure() {

        final QueryClient.QuerySamplesStreamResponseObserver observer =
                new QueryClient.QuerySamplesStreamResponseObserver();

        observer.onNext(page(List.of(ts(1, 0), ts(2, 0)), column("pvA", 1.0)));
        observer.await();

        assertTrue(observer.isError());
        assertTrue(observer.getErrorMessage(),
                observer.getErrorMessage().contains("holds 1 values for 2 timestamps"));
    }

    /**
     * An exceptional response fails the call with the SERVICE's message verbatim and its status,
     * matching the message contract every other observer follows.
     */
    @Test
    public void testExceptionalResponseSurfacesServiceMessage() {

        final QueryClient.QuerySamplesStreamResponseObserver observer =
                new QueryClient.QuerySamplesStreamResponseObserver();

        observer.onNext(QuerySamplesResponse.newBuilder()
                .setExceptionalResult(ExceptionalResult.newBuilder()
                        .setExceptionalResultStatus(
                                ExceptionalResult.ExceptionalResultStatus.RESULT_STATUS_REJECT)
                        .setMessage("pvSelector is required"))
                .build());
        observer.await();

        assertTrue(observer.isError());
        assertEquals("pvSelector is required", observer.getErrorMessage());
        assertEquals(com.ospreydcs.dp.client.result.ApiResultStatus.REJECT, observer.getApiResultStatus());
    }

    /**
     * A stream carrying no message at all accumulates to an empty table rather than a null one, so
     * that a caller reading the result does not have to null-check the payload of a success.
     */
    @Test
    public void testEmptyStreamProducesEmptyTable() {

        final QueryClient.QuerySamplesStreamResponseObserver observer =
                new QueryClient.QuerySamplesStreamResponseObserver();

        observer.onCompleted();
        observer.await();

        assertFalse(observer.isError());
        assertEquals(0, observer.getColumnTable().getTimestampList().getTimestampsCount());
        assertEquals(0, observer.getColumnTable().getDataColumnsCount());
    }

    /**
     * Serialized columns cannot be merged without deserializing them, so they are concatenated as
     * delivered while the timestamp axis accumulates normally.  Documented on the observer; pinned
     * here so the behavior is not mistaken for the by-name merge.
     */
    @Test
    public void testSerializedColumnsAreConcatenated() {

        final QueryClient.QuerySamplesStreamResponseObserver observer =
                new QueryClient.QuerySamplesStreamResponseObserver();

        final SerializedDataColumn first = SerializedDataColumn.newBuilder()
                .setName("pvA").setPayload(ByteString.copyFromUtf8("one")).build();
        final SerializedDataColumn second = SerializedDataColumn.newBuilder()
                .setName("pvA").setPayload(ByteString.copyFromUtf8("two")).build();

        observer.onNext(QuerySamplesResponse.newBuilder()
                .setSampleQueryResult(QuerySamplesResponse.SampleQueryResult.newBuilder()
                        .setColumnTable(ColumnTable.newBuilder()
                                .setTimestampList(TimestampList.newBuilder().addTimestamps(ts(1, 0)))
                                .addSerializedDataColumns(first)))
                .build());
        observer.onNext(QuerySamplesResponse.newBuilder()
                .setSampleQueryResult(QuerySamplesResponse.SampleQueryResult.newBuilder()
                        .setColumnTable(ColumnTable.newBuilder()
                                .setTimestampList(TimestampList.newBuilder().addTimestamps(ts(2, 0)))
                                .addSerializedDataColumns(second)))
                .build());
        observer.onCompleted();
        observer.await();

        assertFalse(observer.getErrorMessage(), observer.isError());
        final ColumnTable table = observer.getColumnTable();
        assertEquals(2, table.getTimestampList().getTimestampsCount());
        assertEquals(List.of(first, second), table.getSerializedDataColumnsList());
        assertEquals(0, table.getDataColumnsCount());
    }
}
