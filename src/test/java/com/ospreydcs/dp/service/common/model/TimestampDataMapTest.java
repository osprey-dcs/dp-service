package com.ospreydcs.dp.service.common.model;

import com.ospreydcs.dp.grpc.v1.common.DataValue;
import org.junit.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Covers {@link TimestampDataMap} row iteration, in particular the draining mode added for #199.
 *
 * <p>Draining lets a consumer release each row as it is materialized into another representation,
 * so peak memory is not the sum of both copies. The rows produced must be identical either way —
 * the only difference is what remains in the map afterward.
 */
public class TimestampDataMapTest {

    private static DataValue doubleValue(double v) {
        return DataValue.newBuilder().setDoubleValue(v).build();
    }

    /** Builds a map with the given (seconds, nanos) rows, one value per column. */
    private static TimestampDataMap mapWithRows(List<long[]> timestamps, int columnCount) {
        final TimestampDataMap map = new TimestampDataMap();
        for (int c = 0; c < columnCount; c++) {
            map.getColumnIndex("pv_" + c);
        }
        for (int r = 0; r < timestamps.size(); r++) {
            final Map<Integer, DataValue> row = new HashMap<>();
            for (int c = 0; c < columnCount; c++) {
                row.put(c, doubleValue(r * 100 + c));
            }
            map.put(timestamps.get(r)[0], timestamps.get(r)[1], row);
        }
        return map;
    }

    private static List<long[]> timestamps(long... secondsNanosPairs) {
        final List<long[]> result = new ArrayList<>();
        for (int i = 0; i < secondsNanosPairs.length; i += 2) {
            result.add(new long[]{secondsNanosPairs[i], secondsNanosPairs[i + 1]});
        }
        return result;
    }

    private static List<TimestampDataMap.DataRow> drain(TimestampDataMap.DataRowIterator it) {
        final List<TimestampDataMap.DataRow> rows = new ArrayList<>();
        while (it.hasNext()) {
            rows.add(it.next());
        }
        return rows;
    }

    /** Draining must produce exactly the same rows, in the same order, as non-draining. */
    @Test
    public void testDrainingProducesSameRowsAsNonDraining() {
        final List<long[]> ts = timestamps(100L, 0L, 100L, 500L, 101L, 0L, 102L, 250L);

        final List<TimestampDataMap.DataRow> expected =
                drain(mapWithRows(ts, 3).new DataRowIterator(false));
        final List<TimestampDataMap.DataRow> actual =
                drain(mapWithRows(ts, 3).new DataRowIterator(true));

        assertEquals(expected.size(), actual.size());
        for (int i = 0; i < expected.size(); i++) {
            assertEquals(expected.get(i).seconds(), actual.get(i).seconds());
            assertEquals(expected.get(i).nanos(), actual.get(i).nanos());
            assertEquals(expected.get(i).dataValues(), actual.get(i).dataValues());
        }
    }

    /** After a full draining pass the map retains nothing, including empty per-second maps. */
    @Test
    public void testDrainingEmptiesTheMap() {
        final TimestampDataMap map =
                mapWithRows(timestamps(100L, 0L, 100L, 500L, 101L, 0L, 102L, 250L), 2);
        assertEquals(4, map.size());

        drain(map.new DataRowIterator(true));

        assertEquals(0, map.size());
        assertTrue("per-second maps must be dropped, not left empty", map.isEmpty());
    }

    /** Non-draining iteration leaves the map intact, which existing callers rely on. */
    @Test
    public void testNonDrainingLeavesMapIntact() {
        final TimestampDataMap map = mapWithRows(timestamps(100L, 0L, 101L, 0L), 2);

        drain(map.new DataRowIterator(false));

        assertEquals(2, map.size());
        assertFalse(map.isEmpty());
    }

    /**
     * The map shrinks progressively during draining rather than only at the end — the property that
     * makes peak memory lower.
     */
    @Test
    public void testMapShrinksDuringDraining() {
        final TimestampDataMap map =
                mapWithRows(timestamps(100L, 0L, 101L, 0L, 102L, 0L, 103L, 0L), 2);
        final TimestampDataMap.DataRowIterator it = map.new DataRowIterator(true);

        int previousSize = map.size();
        assertEquals(4, previousSize);
        while (it.hasNext()) {
            it.next();
            final int currentSize = map.size();
            assertTrue("map size must not grow while draining", currentSize < previousSize);
            previousSize = currentSize;
        }
        assertEquals(0, previousSize);
    }

    /** hasNext() after exhaustion must stay safe: the final removal happens at most once. */
    @Test
    public void testRepeatedHasNextAfterExhaustionIsSafe() {
        final TimestampDataMap map = mapWithRows(timestamps(100L, 0L), 1);
        final TimestampDataMap.DataRowIterator it = map.new DataRowIterator(true);

        assertTrue(it.hasNext());
        it.next();
        assertFalse(it.hasNext());
        assertFalse(it.hasNext());
        assertFalse(it.hasNext());
        assertEquals(0, map.size());
    }

    /** An empty map iterates to nothing in both modes. */
    @Test
    public void testEmptyMapIterates() {
        assertEquals(0, drain(new TimestampDataMap().new DataRowIterator(true)).size());
        assertEquals(0, drain(new TimestampDataMap().new DataRowIterator(false)).size());
    }

    /** Missing cells are filled with an unset DataValue, in draining mode too. */
    @Test
    public void testSparseRowsFilledWhenDraining() {
        final TimestampDataMap map = new TimestampDataMap();
        map.getColumnIndex("pv_0");
        map.getColumnIndex("pv_1");
        final Map<Integer, DataValue> sparseRow = new HashMap<>();
        sparseRow.put(0, doubleValue(1.0)); // column 1 absent
        map.put(100L, 0L, sparseRow);

        final List<TimestampDataMap.DataRow> rows = drain(map.new DataRowIterator(true));

        assertEquals(1, rows.size());
        assertEquals(2, rows.get(0).dataValues().size());
        assertEquals(doubleValue(1.0), rows.get(0).dataValues().get(0));
        assertEquals(DataValue.newBuilder().build(), rows.get(0).dataValues().get(1));
    }
}
