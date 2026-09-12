package com.ospreydcs.dp.service.common.bson.pvstats;

import org.bson.codecs.pojo.annotations.BsonId;

/**
 * MongoDB document for the pvStats collection: statistics for one PV, maintained on the ingestion
 * write path and read by the query service (#232, epic #257).
 *
 * <p>One document per PV, keyed by the PV name as {@code _id} (plan D2). The default {@code _id}
 * index serves both the ingestion-side {@code $max} upsert and the query-side {@code $in} read, so
 * the collection carries no additional index. A dedicated collection rather than the user-managed
 * pvMetadata records: those can be deleted through the API, and deleting one would silently remove
 * the evidence the query bound relies on.
 *
 * <p>{@code maxBucketSpanSeconds} is the maximum over the PV's buckets of
 * {@code dataTimestamps.lastTime.seconds - dataTimestamps.firstTime.seconds} — the difference of
 * the two seconds fields, not a nanosecond span (plan D3). That is exactly the quantity the bucket
 * overlap filter consumes: for any bucket overlapping {@code [begin, end)},
 * {@code lastTime.seconds >= beginSeconds}, so {@code firstTime.seconds >= beginSeconds - span}.
 * The v5 seed migration computes the same difference with {@code $subtract}, so ingestion and the
 * seed cannot disagree. It only ever grows: ingestion raises it with {@code $max}, and nothing
 * lowers it, so it stays a valid bound over every bucket ever stored for the PV.
 *
 * <p>Further per-PV ingestion statistics (#201) extend this document with additional fields. Every
 * field must have both a getter and a setter — the POJO codec silently skips a field missing
 * either, so the write succeeds and the field is simply never stored.
 */
public class PvStatsDocument {

    // instance variables
    @BsonId
    private String pvName;
    private long maxBucketSpanSeconds;

    public String getPvName() {
        return pvName;
    }

    public void setPvName(String pvName) {
        this.pvName = pvName;
    }

    public long getMaxBucketSpanSeconds() {
        return maxBucketSpanSeconds;
    }

    public void setMaxBucketSpanSeconds(long maxBucketSpanSeconds) {
        this.maxBucketSpanSeconds = maxBucketSpanSeconds;
    }
}
