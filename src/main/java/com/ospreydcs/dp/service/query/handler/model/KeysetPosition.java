package com.ospreydcs.dp.service.query.handler.model;

import java.util.Objects;

/**
 * A page-continuation position for Query API V2 paging — a pure position marker, never server state
 * (Q1/Q2/Q3: stateless, best-effort, position-only tokens). Two shapes:
 *
 * <ul>
 *   <li><b>Bucket</b> (Q2): the last-emitted {@code (pvName, firstTimeSeconds, firstTimeNanos)}
 *       tuple; the next page seeks strictly after it in the compound bucket sort order.</li>
 *   <li><b>Sample</b> (Q1): the resume timestamp {@code (epochSeconds, nanos)}; the next page
 *       re-runs the overlap query with begin advanced to this timestamp.</li>
 * </ul>
 *
 * <p>The <em>meaning</em> of a position is kept decoupled from its <em>encoding</em> (owned by
 * {@code PageToken}), so a future server-side cached-cursor implementation can still accept a
 * position-only token as a cache-miss fallback without any proto or client change.
 */
public final class KeysetPosition {

    public enum Kind { BUCKET, SAMPLE }

    private final Kind kind;

    // BUCKET fields
    private final String pvName;

    // shared time fields: BUCKET => firstTime; SAMPLE => resume timestamp
    private final long seconds;
    private final long nanos;

    private KeysetPosition(Kind kind, String pvName, long seconds, long nanos) {
        this.kind = kind;
        this.pvName = pvName;
        this.seconds = seconds;
        this.nanos = nanos;
    }

    /** Bucket-path position: seek strictly after {@code (pvName, firstTimeSeconds, firstTimeNanos)}. */
    public static KeysetPosition ofBucket(String pvName, long firstTimeSeconds, long firstTimeNanos) {
        return new KeysetPosition(Kind.BUCKET, pvName, firstTimeSeconds, firstTimeNanos);
    }

    /** Sample-path position: resume at {@code (epochSeconds, nanos)}. */
    public static KeysetPosition ofSample(long epochSeconds, long nanos) {
        return new KeysetPosition(Kind.SAMPLE, null, epochSeconds, nanos);
    }

    public Kind getKind() {
        return kind;
    }

    public String getPvName() {
        return pvName;
    }

    public long getSeconds() {
        return seconds;
    }

    public long getNanos() {
        return nanos;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof KeysetPosition that)) return false;
        return seconds == that.seconds
                && nanos == that.nanos
                && kind == that.kind
                && Objects.equals(pvName, that.pvName);
    }

    @Override
    public int hashCode() {
        return Objects.hash(kind, pvName, seconds, nanos);
    }

    @Override
    public String toString() {
        return kind == Kind.BUCKET
                ? "KeysetPosition[BUCKET " + pvName + " @ " + seconds + "." + nanos + "]"
                : "KeysetPosition[SAMPLE @ " + seconds + "." + nanos + "]";
    }
}
