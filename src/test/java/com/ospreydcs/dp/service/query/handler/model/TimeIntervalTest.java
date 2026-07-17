package com.ospreydcs.dp.service.query.handler.model;

import org.junit.Test;

import java.util.List;

import static org.junit.Assert.*;

/**
 * Unit tests for {@link TimeInterval} interval math (union, intersect) — the pure Java that backs
 * Query API V2 configuration-fragment resolution (Q3). Instants are written as {@code (seconds,
 * nanos)}; nanos are used in a few cases to exercise the lexicographic endpoint comparison.
 */
public class TimeIntervalTest {

    private static TimeInterval iv(long bs, long es) {
        return new TimeInterval(bs, 0, es, 0);
    }

    // -----------------------------------------------------------------------
    // isEmpty / compareInstant
    // -----------------------------------------------------------------------

    @Test
    public void testIsEmpty() {
        assertFalse(iv(10, 20).isEmpty());
        assertTrue(iv(20, 20).isEmpty());   // end == begin
        assertTrue(iv(30, 20).isEmpty());   // end < begin
        assertTrue(new TimeInterval(10, 500, 10, 500).isEmpty());
        assertFalse(new TimeInterval(10, 100, 10, 200).isEmpty()); // same secs, nanos differ
    }

    @Test
    public void testCompareInstant() {
        assertTrue(TimeInterval.compareInstant(1, 0, 2, 0) < 0);
        assertTrue(TimeInterval.compareInstant(2, 0, 1, 0) > 0);
        assertEquals(0, TimeInterval.compareInstant(1, 5, 1, 5));
        assertTrue(TimeInterval.compareInstant(1, 5, 1, 6) < 0); // nanos tiebreak
    }

    // -----------------------------------------------------------------------
    // intersect
    // -----------------------------------------------------------------------

    @Test
    public void testIntersect_overlap() {
        assertEquals(iv(15, 20), iv(10, 20).intersect(15, 0, 25, 0));
    }

    @Test
    public void testIntersect_containedReturnsInner() {
        assertEquals(iv(12, 18), iv(10, 20).intersect(12, 0, 18, 0));
    }

    @Test
    public void testIntersect_noOverlapReturnsNull() {
        assertNull(iv(10, 20).intersect(20, 0, 30, 0)); // touching at 20, half-open → empty
        assertNull(iv(10, 20).intersect(30, 0, 40, 0)); // disjoint
    }

    // -----------------------------------------------------------------------
    // union
    // -----------------------------------------------------------------------

    @Test
    public void testUnion_mergesOverlapping() {
        final List<TimeInterval> result = TimeInterval.union(List.of(iv(10, 20), iv(15, 25)));
        assertEquals(List.of(iv(10, 25)), result);
    }

    @Test
    public void testUnion_mergesAdjacent() {
        // half-open [10,20) and [20,30) abut with no gap → merge
        final List<TimeInterval> result = TimeInterval.union(List.of(iv(10, 20), iv(20, 30)));
        assertEquals(List.of(iv(10, 30)), result);
    }

    @Test
    public void testUnion_keepsDisjoint() {
        final List<TimeInterval> result = TimeInterval.union(List.of(iv(30, 40), iv(10, 20)));
        assertEquals(List.of(iv(10, 20), iv(30, 40)), result); // sorted
    }

    @Test
    public void testUnion_dropsEmptyAndContained() {
        final List<TimeInterval> result = TimeInterval.union(
                List.of(iv(10, 30), iv(15, 20), iv(25, 25) /* empty */));
        assertEquals(List.of(iv(10, 30)), result);
    }

    @Test
    public void testUnion_emptyInput() {
        assertTrue(TimeInterval.union(List.of()).isEmpty());
    }

    // -----------------------------------------------------------------------
    // intersectAll
    // -----------------------------------------------------------------------

    @Test
    public void testIntersectAll_clipsToBound() {
        final List<TimeInterval> intervals = List.of(iv(0, 15), iv(25, 100));
        final TimeInterval bound = iv(10, 40);
        // union = [0,15),[25,100); clipped to [10,40) => [10,15),[25,40)
        assertEquals(List.of(iv(10, 15), iv(25, 40)),
                TimeInterval.intersectAll(intervals, bound));
    }

    @Test
    public void testIntersectAll_noOverlapYieldsEmpty() {
        assertTrue(TimeInterval.intersectAll(List.of(iv(0, 5)), iv(10, 20)).isEmpty());
    }

    @Test
    public void testIntersectAll_openEndedSentinelClampedToBound() {
        // open-ended activation modeled as end = Long.MAX_VALUE; clamps to the bound end
        final TimeInterval openEnded = new TimeInterval(50, 0, Long.MAX_VALUE, 0);
        final TimeInterval bound = iv(10, 100);
        assertEquals(List.of(iv(50, 100)),
                TimeInterval.intersectAll(List.of(openEnded), bound));
    }
}
