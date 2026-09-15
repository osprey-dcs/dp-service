package com.ospreydcs.dp.service.query.handler.mongo.client;

import org.junit.Test;

import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

/**
 * Pins the span-class assignment behind the partitioned bucket retrieval (issue #274, plan D11):
 * the power-of-two class boundaries, that a class's bound is the largest span actually present
 * rather than the class ceiling, that a PV without a {@code pvStats} entry lands in class 0, and
 * that a request whose PVs share one class yields a single class -- the fast path that keeps the
 * common case's filter identical to the pre-partition query.
 */
public class SpanClassTest {

    @Test
    public void testClassIndexBoundaries() {
        // class 0: span <= 1 s; class k: 2^(k-1) < span <= 2^k
        assertEquals(0, SpanClass.classIndex(0));
        assertEquals(0, SpanClass.classIndex(1));
        assertEquals(1, SpanClass.classIndex(2));
        assertEquals(2, SpanClass.classIndex(3));
        assertEquals(2, SpanClass.classIndex(4));
        assertEquals(3, SpanClass.classIndex(5));
        assertEquals(3, SpanClass.classIndex(8));
        assertEquals(4, SpanClass.classIndex(9));
        assertEquals(17, SpanClass.classIndex(86_400));      // one day: 2^16 < 86400 <= 2^17
        assertEquals(22, SpanClass.classIndex(3_628_800));   // 42 days: 2^21 < 3628800 <= 2^22
    }

    @Test
    public void testPartitionGroupsByClassWithActualMaximum() {
        final Map<String, Long> spans = Map.of("a", 3L, "b", 4L, "c", 60L, "d", 0L, "e", 40L);
        final List<SpanClass> classes = SpanClass.partition(List.of("a", "b", "c", "d", "e"), spans);

        assertEquals(3, classes.size());
        // class 0: d (span 0)
        assertEquals(List.of("d"), classes.get(0).pvNames());
        assertEquals(0L, classes.get(0).maxSpanSeconds());
        // class 2: a (3), b (4) -- bound is the actual max, 4, not the class ceiling
        assertEquals(List.of("a", "b"), classes.get(1).pvNames());
        assertEquals(4L, classes.get(1).maxSpanSeconds());
        // class 6: e (40), c (60) in request order; bound 60, not 64
        assertEquals(List.of("c", "e"), classes.get(2).pvNames());
        assertEquals(60L, classes.get(2).maxSpanSeconds());
    }

    @Test
    public void testPvWithoutStatsIsClassZero() {
        final List<SpanClass> classes = SpanClass.partition(List.of("known", "unknown"), Map.of("known", 300L));
        assertEquals(2, classes.size());
        assertEquals(List.of("unknown"), classes.get(0).pvNames());
        assertEquals(0L, classes.get(0).maxSpanSeconds());
        assertEquals(List.of("known"), classes.get(1).pvNames());
        assertEquals(300L, classes.get(1).maxSpanSeconds());
    }

    @Test
    public void testSharedClassIsSingleClassWithRequestOrderAndDedup() {
        final List<SpanClass> classes = SpanClass.partition(
                List.of("y", "x", "y"), Map.of("x", 7L, "y", 5L));
        assertEquals(1, classes.size());
        assertEquals(List.of("y", "x"), classes.get(0).pvNames());
        assertEquals(7L, classes.get(0).maxSpanSeconds());
    }

    @Test
    public void testNegativeStoredSpanIsClampedToZero() {
        // the client clamps before partitioning; partition itself must not be tripped by one either
        final List<SpanClass> classes = SpanClass.partition(List.of("neg"), Map.of("neg", -5L));
        assertEquals(1, classes.size());
        assertEquals(0L, classes.get(0).maxSpanSeconds());
    }

    @Test
    public void testEmptyNamesYieldEmptyPartition() {
        assertTrue(SpanClass.partition(List.of(), Map.of()).isEmpty());
    }

    @Test
    public void testConstructorRejectsEmptyNamesAndNegativeSpan() {
        assertThrows(IllegalArgumentException.class, () -> new SpanClass(List.of(), 1L));
        assertThrows(IllegalArgumentException.class, () -> new SpanClass(List.of("a"), -1L));
    }
}
