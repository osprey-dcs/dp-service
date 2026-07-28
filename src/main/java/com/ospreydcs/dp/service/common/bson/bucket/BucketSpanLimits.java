package com.ospreydcs.dp.service.common.bson.bucket;

import com.ospreydcs.dp.service.common.config.ConfigurationManager;

/**
 * Single source for the maximum time span (lastTime - firstTime) a bucket document may cover.
 *
 * <p>This limit is a shared invariant between the ingestion and query services (issue #197):
 * ingestion validation rejects any data frame whose timestamps span more than the limit, which
 * lets the query-side bucket-overlap filter add the lower bound
 * {@code firstTime.seconds >= beginSeconds - maxBucketSpanSeconds}. Without that bound, the
 * overlap predicate ({@code firstTime < end AND lastTime >= begin}) forces an index scan of each
 * PV's entire history up to the query window — 31.6M keys examined to return 32 documents in the
 * incident deployment.
 *
 * <p>IMPORTANT deployment note: the query bound assumes every archived bucket satisfies the
 * configured limit. When enabling a smaller limit on an archive with pre-existing data, the
 * configured value must be at least the largest bucket span already stored, or queries may
 * silently miss buckets ingested before the limit was enforced.
 */
public class BucketSpanLimits {

    public static final String CFG_KEY_MAX_BUCKET_SPAN_SECONDS = "Buckets.maxBucketSpanSeconds";
    public static final long DEFAULT_MAX_BUCKET_SPAN_SECONDS = 86_400L; // 1 day

    public static long getMaxBucketSpanSeconds() {
        return ConfigurationManager.getInstance()
                .getConfigLong(CFG_KEY_MAX_BUCKET_SPAN_SECONDS, DEFAULT_MAX_BUCKET_SPAN_SECONDS);
    }

    public static long getMaxBucketSpanNanos() {
        return getMaxBucketSpanSeconds() * 1_000_000_000L;
    }
}
