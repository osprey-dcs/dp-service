package com.ospreydcs.dp.service.query.handler.mongo.dispatch;

import com.ospreydcs.dp.grpc.v1.common.DataValue;
import com.ospreydcs.dp.grpc.v1.query.ColumnTable;
import com.ospreydcs.dp.service.query.handler.mongo.dispatch.AbstractQuerySamplesDispatcher.RowChunk;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * {@link RowChunk}'s byte figure must never be smaller than the serialized table it describes:
 * the dispatchers stop adding rows on it, so an under-count sends a message over the wire limit.
 * The estimate charges the column names once per table (PR #323 review), which removed the slack
 * that charging them to every row used to give, so the bound is pinned here across the shapes
 * that stress each term.
 */
public class RowChunkSizeBoundTest {

    private static List<String> names(int count, int length) {
        final List<String> names = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            final StringBuilder name = new StringBuilder("pv" + i + "_");
            while (name.length() < length) {
                name.append('x');
            }
            names.add(name.toString());
        }
        return names;
    }

    private static DataValue value(int kind, int rowIndex) {
        return switch (kind) {
            case 0 -> DataValue.newBuilder().setDoubleValue(rowIndex + 0.5).build();
            case 1 -> DataValue.newBuilder().setStringValue("s".repeat(256)).build();
            case 2 -> DataValue.newBuilder().setLongValue(Long.MIN_VALUE).build();
            default -> null; // no sample: sent as an unset value
        };
    }

    /** Fills a chunk, checking the bound after every row and for the empty table. */
    private static void assertBound(int columnCount, int nameLength, int rowCount, boolean serialized, int kind) {
        final RowChunk chunk = new RowChunk(names(columnCount, nameLength), serialized);
        final String shape = columnCount + " columns, names of " + nameLength + ", serialized=" + serialized
                + ", kind " + kind;

        final RowChunk empty = new RowChunk(names(columnCount, nameLength), serialized);
        final long emptyEstimate = empty.bytes();
        assertEquals(empty.emptyBytes(), emptyEstimate);
        assertTrue("empty table under-counted: " + shape,
                emptyEstimate >= empty.buildAndReset(serialized).getSerializedSize());

        final SampleRowMerger.Row row = new SampleRowMerger.Row(columnCount);
        for (int rowIndex = 0; rowIndex < rowCount; rowIndex++) {
            // the largest timestamp encodings: 10-byte seconds, 5-byte nanos
            row.seconds = Long.MAX_VALUE - rowCount + rowIndex;
            row.nanos = 999_999_999L;
            for (int column = 0; column < columnCount; column++) {
                row.values[column] = value(kind == 9 ? (column + rowIndex) % 4 : kind, rowIndex);
            }
            chunk.add(row, chunk.rowBytes(row));
        }
        final long estimate = chunk.bytes();
        final ColumnTable table = chunk.buildAndReset(serialized);
        assertEquals(rowCount, table.getTimestampList().getTimestampsCount());
        assertTrue("under-counted (" + estimate + " < " + table.getSerializedSize() + "): " + shape + ", "
                + rowCount + " rows", estimate >= table.getSerializedSize());
        assertEquals("a built chunk starts again at the empty figure", emptyEstimate, chunk.bytes());
    }

    @Test
    public void testEstimateIsAnUpperBoundOnTheSerializedTable() {
        for (boolean serialized : new boolean[]{false, true}) {
            for (int kind : new int[]{0, 1, 2, 3, 9}) {
                for (int[] shape : new int[][]{
                        {1, 1, 1}, {1, 4, 1}, {1, 200, 1}, {2, 5, 3}, {1, 20, 5000},
                        {300, 20, 1}, {300, 20, 40}, {3000, 40, 2}, {40, 300, 600}}) {
                    assertBound(shape[0], shape[1], shape[2], serialized, kind);
                }
            }
        }
    }

    /** The point of charging names once: a wide message is no longer estimated at several times its size. */
    @Test
    public void testAWideTableIsNotGrosslyOverEstimated() {
        final RowChunk chunk = new RowChunk(names(1000, 20), false);
        final SampleRowMerger.Row row = new SampleRowMerger.Row(1000);
        for (int rowIndex = 0; rowIndex < 100; rowIndex++) {
            row.seconds = 1_700_000_000L + rowIndex;
            row.nanos = 0;
            for (int column = 0; column < 1000; column++) {
                row.values[column] = value(0, rowIndex);
            }
            chunk.add(row, chunk.rowBytes(row));
        }
        final long estimate = chunk.bytes();
        final long actual = chunk.buildAndReset(false).getSerializedSize();
        assertTrue("estimate " + estimate + " for " + actual + " bytes", estimate < 2 * actual);
    }
}
