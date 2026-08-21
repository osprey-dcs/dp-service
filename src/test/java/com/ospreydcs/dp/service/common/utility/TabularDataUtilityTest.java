package com.ospreydcs.dp.service.common.utility;

import com.ospreydcs.dp.grpc.v1.common.DataColumn;
import com.ospreydcs.dp.grpc.v1.common.DataTimestamps;
import com.ospreydcs.dp.grpc.v1.common.DataValue;
import com.ospreydcs.dp.grpc.v1.common.SamplingClock;
import com.ospreydcs.dp.grpc.v1.common.Timestamp;
import com.ospreydcs.dp.service.common.model.TimestampDataMap;
import org.junit.Test;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Unit tests for the sample-level range trimming in {@link TabularDataUtility} — the half-open
 * {@code [begin, end)} filter shared by {@code queryTable}, {@code querySamples}, and the annotation
 * export job.
 *
 * <p>Exercises {@code addColumnsToTable} directly (via reflection — it is private, and the public
 * entry points require a Mongo cursor), which is the single place the trim is applied.
 */
public class TabularDataUtilityTest {

    private static final long BASE = 1_000_000L;

    /** Builds a 1 Hz {@link DataTimestamps} of {@code count} samples starting at {@code startSecond}. */
    private static DataTimestamps clock(long startSecond, int count) {
        return DataTimestamps.newBuilder()
                .setSamplingClock(SamplingClock.newBuilder()
                        .setStartTime(Timestamp.newBuilder().setEpochSeconds(startSecond).setNanoseconds(0))
                        .setPeriodNanos(1_000_000_000L)
                        .setCount(count))
                .build();
    }

    /** A column of {@code count} double values, value i = i. */
    private static DataColumn column(String name, int count) {
        final DataColumn.Builder builder = DataColumn.newBuilder().setName(name);
        for (int i = 0; i < count; i++) {
            builder.addDataValues(DataValue.newBuilder().setDoubleValue(i).build());
        }
        return builder.build();
    }

    private static void addColumns(
            DataTimestamps dataTimestamps,
            List<DataColumn> dataColumns,
            TimestampDataMap tableValueMap,
            List<TabularDataUtility.RetentionInterval> intervals) throws Exception {

        final Method method = TabularDataUtility.class.getDeclaredMethod(
                "addColumnsToTable", DataTimestamps.class, List.class, TimestampDataMap.class, List.class,
                TabularDataUtility.SampleStatusFilter.class);
        method.setAccessible(true);
        try {
            method.invoke(null, dataTimestamps, dataColumns, tableValueMap, intervals, null);
        } catch (InvocationTargetException e) {
            throw (Exception) e.getCause();
        }
    }

    /** Offsets from {@link #BASE} of every timestamp held by the map, in order. */
    private static List<Long> offsets(TimestampDataMap tableValueMap) {
        final List<Long> result = new ArrayList<>();
        final TimestampDataMap.DataRowIterator iterator = tableValueMap.new DataRowIterator();
        while (iterator.hasNext()) {
            result.add(iterator.next().seconds() - BASE);
        }
        return result;
    }

    @Test
    public void testSingleIntervalTrimsToHalfOpenRange() throws Exception {
        final TimestampDataMap map = new TimestampDataMap();
        // 20 samples at BASE..BASE+19, kept range [BASE, BASE+10) -> offsets 0..9
        addColumns(clock(BASE, 20), List.of(column("pv", 20)), map,
                List.of(new TabularDataUtility.RetentionInterval(BASE, 0, BASE + 10, 0)));

        assertEquals(List.of(0L, 1L, 2L, 3L, 4L, 5L, 6L, 7L, 8L, 9L), offsets(map));
    }

    @Test
    public void testSampleExactlyAtEndIsExcluded() throws Exception {
        final TimestampDataMap map = new TimestampDataMap();
        addColumns(clock(BASE, 5), List.of(column("pv", 5)), map,
                List.of(new TabularDataUtility.RetentionInterval(BASE, 0, BASE + 3, 0)));

        // half-open: the sample at exactly BASE+3 is excluded
        assertEquals(List.of(0L, 1L, 2L), offsets(map));
    }

    /**
     * A bucket spanning the gap between two disjoint retention intervals must contribute only its
     * in-interval samples (issue #207). This is the case the per-bucket database filter cannot catch.
     */
    @Test
    public void testMultipleIntervalsExcludeGapSamples() throws Exception {
        final TimestampDataMap map = new TimestampDataMap();
        addColumns(clock(BASE, 10), List.of(column("pv", 10)), map,
                List.of(
                        new TabularDataUtility.RetentionInterval(BASE, 0, BASE + 2, 0),
                        new TabularDataUtility.RetentionInterval(BASE + 8, 0, BASE + 10, 0)));

        assertEquals(List.of(0L, 1L, 8L, 9L), offsets(map));
    }

    @Test
    public void testValuesStayAlignedWithTimestampsAcrossAGap() throws Exception {
        final TimestampDataMap map = new TimestampDataMap();
        addColumns(clock(BASE, 10), List.of(column("pv", 10)), map,
                List.of(
                        new TabularDataUtility.RetentionInterval(BASE, 0, BASE + 2, 0),
                        new TabularDataUtility.RetentionInterval(BASE + 8, 0, BASE + 10, 0)));

        // value i == i, so the retained values must be exactly 0, 1, 8, 9 -- skipping a sample must
        // advance the value index in lockstep with the timestamp iterator.
        final List<Double> values = new ArrayList<>();
        final TimestampDataMap.DataRowIterator iterator = map.new DataRowIterator();
        while (iterator.hasNext()) {
            values.add(iterator.next().dataValues().get(0).getDoubleValue());
        }
        assertEquals(List.of(0.0, 1.0, 8.0, 9.0), values);
    }

    /**
     * A column whose samples all fall outside the retention range must still be registered, yielding
     * an all-empty column rather than a missing one. The pre-#207 code got this incidentally (the
     * range test sat inside the per-column loop, after the registering {@code getColumnIndex} call);
     * registration is now explicit, and this pins the behavior so a future refactor cannot drop it.
     */
    @Test
    public void testFullyOutOfRangeColumnIsStillRegistered() throws Exception {
        final TimestampDataMap map = new TimestampDataMap();
        // every sample (BASE..BASE+4) is outside the kept range [BASE+100, BASE+110)
        addColumns(clock(BASE, 5), List.of(column("pv_absent", 5)), map,
                List.of(new TabularDataUtility.RetentionInterval(BASE + 100, 0, BASE + 110, 0)));

        assertTrue("no rows expected", offsets(map).isEmpty());
        assertEquals("column must still be registered", List.of("pv_absent"), map.getColumnNameList());
    }

    @Test
    public void testMultiColumnRegistrationOrderIsPreserved() throws Exception {
        final TimestampDataMap map = new TimestampDataMap();
        // pv_b has no in-range samples; pv_a and pv_c do. All three must keep their slots and order.
        addColumns(clock(BASE, 4), List.of(column("pv_a", 4), column("pv_b", 4), column("pv_c", 4)), map,
                List.of(new TabularDataUtility.RetentionInterval(BASE, 0, BASE + 2, 0)));

        assertEquals(List.of("pv_a", "pv_b", "pv_c"), map.getColumnNameList());
    }
}
