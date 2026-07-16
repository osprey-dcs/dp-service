package com.ospreydcs.dp.service.query.handler.model;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;

/**
 * A half-open time interval {@code [begin, end)} expressed in epoch seconds + nanoseconds, used by
 * the Query API V2 resolver to represent effective retrieval ranges (a single interval for the whole
 * {@code QuerySpec.timeRange}, or a possibly-fragmented set derived from a
 * {@code ConfigurationSelector}).
 *
 * <p>The endpoints are compared lexicographically on {@code (seconds, nanos)}. An interval is empty
 * (and therefore invalid as a retrieval range) when {@code end <= begin}. The union/intersect
 * helpers here are pure functions with no MongoDB dependency, so they are directly unit-testable.
 */
public final class TimeInterval {

    private final long beginSeconds;
    private final long beginNanos;
    private final long endSeconds;
    private final long endNanos;

    public TimeInterval(long beginSeconds, long beginNanos, long endSeconds, long endNanos) {
        this.beginSeconds = beginSeconds;
        this.beginNanos = beginNanos;
        this.endSeconds = endSeconds;
        this.endNanos = endNanos;
    }

    public long getBeginSeconds() {
        return beginSeconds;
    }

    public long getBeginNanos() {
        return beginNanos;
    }

    public long getEndSeconds() {
        return endSeconds;
    }

    public long getEndNanos() {
        return endNanos;
    }

    /** Lexicographic compare of a {@code (seconds, nanos)} instant pair. */
    public static int compareInstant(long aSecs, long aNanos, long bSecs, long bNanos) {
        if (aSecs != bSecs) {
            return Long.compare(aSecs, bSecs);
        }
        return Long.compare(aNanos, bNanos);
    }

    /** True when {@code end <= begin} (an empty / degenerate interval). */
    public boolean isEmpty() {
        return compareInstant(endSeconds, endNanos, beginSeconds, beginNanos) <= 0;
    }

    /**
     * Intersects this interval with {@code [beginSecs,beginNanos) .. [endSecs,endNanos)} and returns
     * the overlap, or {@code null} if the two do not overlap. Result begin = max of begins, result
     * end = min of ends.
     */
    public TimeInterval intersect(long beginSecs, long beginNanos, long endSecs, long endNanos) {
        // begin = max(this.begin, other.begin)
        final long bSecs, bNanos;
        if (compareInstant(this.beginSeconds, this.beginNanos, beginSecs, beginNanos) >= 0) {
            bSecs = this.beginSeconds;
            bNanos = this.beginNanos;
        } else {
            bSecs = beginSecs;
            bNanos = beginNanos;
        }
        // end = min(this.end, other.end)
        final long eSecs, eNanos;
        if (compareInstant(this.endSeconds, this.endNanos, endSecs, endNanos) <= 0) {
            eSecs = this.endSeconds;
            eNanos = this.endNanos;
        } else {
            eSecs = endSecs;
            eNanos = endNanos;
        }
        final TimeInterval result = new TimeInterval(bSecs, bNanos, eSecs, eNanos);
        return result.isEmpty() ? null : result;
    }

    /**
     * Coalesces a list of possibly-overlapping/adjacent intervals into a minimal sorted set of
     * disjoint intervals (the union). Empty inputs and empty intervals are dropped. Adjacent
     * intervals (one's end equal to the next's begin) are merged, since the ranges are half-open and
     * abut with no gap.
     */
    public static List<TimeInterval> union(List<TimeInterval> intervals) {
        final List<TimeInterval> sorted = new ArrayList<>();
        for (TimeInterval iv : intervals) {
            if (iv != null && !iv.isEmpty()) {
                sorted.add(iv);
            }
        }
        sorted.sort(Comparator
                .comparingLong(TimeInterval::getBeginSeconds)
                .thenComparingLong(TimeInterval::getBeginNanos));

        final List<TimeInterval> merged = new ArrayList<>();
        for (TimeInterval iv : sorted) {
            if (merged.isEmpty()) {
                merged.add(iv);
                continue;
            }
            final TimeInterval last = merged.get(merged.size() - 1);
            // merge when iv.begin <= last.end (overlap or adjacency, half-open)
            if (compareInstant(iv.beginSeconds, iv.beginNanos, last.endSeconds, last.endNanos) <= 0) {
                // extend last to max(last.end, iv.end)
                if (compareInstant(iv.endSeconds, iv.endNanos, last.endSeconds, last.endNanos) > 0) {
                    merged.set(merged.size() - 1,
                            new TimeInterval(last.beginSeconds, last.beginNanos, iv.endSeconds, iv.endNanos));
                }
                // else iv is fully contained in last — drop it
            } else {
                merged.add(iv);
            }
        }
        return merged;
    }

    /**
     * Intersects a set of intervals with a single bounding interval, returning the (sorted, disjoint)
     * pieces that fall within the bound. Input is unioned first, so the result is minimal and
     * disjoint. An empty result means the selector matches nothing inside the bound.
     */
    public static List<TimeInterval> intersectAll(List<TimeInterval> intervals, TimeInterval bound) {
        final List<TimeInterval> result = new ArrayList<>();
        for (TimeInterval iv : union(intervals)) {
            final TimeInterval clipped = iv.intersect(
                    bound.beginSeconds, bound.beginNanos, bound.endSeconds, bound.endNanos);
            if (clipped != null) {
                result.add(clipped);
            }
        }
        return result;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof TimeInterval that)) return false;
        return beginSeconds == that.beginSeconds
                && beginNanos == that.beginNanos
                && endSeconds == that.endSeconds
                && endNanos == that.endNanos;
    }

    @Override
    public int hashCode() {
        return Objects.hash(beginSeconds, beginNanos, endSeconds, endNanos);
    }

    @Override
    public String toString() {
        return "TimeInterval[" + beginSeconds + "." + beginNanos + " .. " + endSeconds + "." + endNanos + ")";
    }
}
