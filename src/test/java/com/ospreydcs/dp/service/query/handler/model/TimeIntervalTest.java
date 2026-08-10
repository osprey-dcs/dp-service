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

    // -----------------------------------------------------------------------
    // clampToWindowBegin
    //
    // The single source for the querySamples page clamp (#207): both the bucket
    // retrieval filter (MongoSyncQueryClient.executeQuerySamplesV2) and the sample
    // retention trim (AbstractQuerySamplesDispatcher.retentionIntervals) are built
    // from this, so a divergence here is a divergence between what the database
    // returns and what assembly keeps.
    // -----------------------------------------------------------------------

    @Test
    public void testClampToWindowBegin_firstPageLeavesFragmentsUntouched() {
        // window begin == earliest fragment begin (the page-1 case): nothing is clamped or dropped
        final List<TimeInterval> fragments = List.of(iv(10, 20), iv(30, 40));
        assertEquals(fragments, TimeInterval.clampToWindowBegin(fragments, 10, 0));
    }

    @Test
    public void testClampToWindowBegin_clampsStraddlingFragmentAndDropsEarlierOnes() {
        // resume at 35, mid-way through the second fragment: the first is entirely behind the
        // window and drops; the second is clamped up to the resume point; the third is untouched.
        final List<TimeInterval> fragments = List.of(iv(10, 20), iv(30, 40), iv(50, 60));
        assertEquals(List.of(iv(35, 40), iv(50, 60)),
                TimeInterval.clampToWindowBegin(fragments, 35, 0));
    }

    @Test
    public void testClampToWindowBegin_dropsFragmentEndingExactlyAtWindowBegin() {
        // half-open: a fragment ending exactly at the window begin contributes no samples
        assertEquals(List.of(iv(30, 40)),
                TimeInterval.clampToWindowBegin(List.of(iv(10, 20), iv(30, 40)), 20, 0));
    }

    @Test
    public void testClampToWindowBegin_windowPastEverythingYieldsEmpty() {
        // an empty result means nothing overlaps the page; callers must treat this as an empty
        // page rather than an unfiltered query.
        assertTrue(TimeInterval.clampToWindowBegin(List.of(iv(10, 20), iv(30, 40)), 99, 0).isEmpty());
    }

    @Test
    public void testClampToWindowBegin_comparesNanosNotJustSeconds() {
        // fragment begins at 10.500; a window begin of 10.250 is earlier, so no clamp applies
        final TimeInterval fragment = new TimeInterval(10, 500, 20, 0);
        assertEquals(List.of(fragment),
                TimeInterval.clampToWindowBegin(List.of(fragment), 10, 250));
        // a window begin of 10.750 is later, so the fragment is clamped up to it
        assertEquals(List.of(new TimeInterval(10, 750, 20, 0)),
                TimeInterval.clampToWindowBegin(List.of(fragment), 10, 750));
    }
}
