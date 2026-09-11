package com.ospreydcs.dp.service.common.bson.bucket;

import com.ospreydcs.dp.service.common.config.ConfigurationManager;
import com.ospreydcs.dp.service.common.exception.DpRuntimeException;

/**
 * Single source for the maximum time span (lastTime - firstTime) a bucket document may cover.
 *
 * <p>The limit binds ingestion only: ingestion validation rejects any data frame whose timestamps
 * span more than it (issue #197). It does not shape queries. The query-side lower bound on the
 * bucket-overlap filter, {@code firstTime.seconds >= beginSeconds - maxBucketSpanSeconds}, takes
 * its span per PV from the {@code pvStats} collection, which records the largest span actually
 * ingested for each PV (issue #232). That makes the bound exact for the data stored, so this
 * configured value neither needs to cover spans ingested before it was introduced nor affects
 * query results: raising or lowering it changes only what ingestion accepts from then on.
 */
public class BucketSpanLimits {

    public static final String CFG_KEY_MAX_BUCKET_SPAN_SECONDS = "Buckets.maxBucketSpanSeconds";
    public static final long DEFAULT_MAX_BUCKET_SPAN_SECONDS = 86_400L; // 1 day

    /**
     * Largest configurable span. Above this, the conversion to nanos in
     * {@link #getMaxBucketSpanNanos()} would overflow a long and invert the ingestion comparison
     * into "reject everything", so the value is rejected at load time instead.
     */
    public static final long MAX_CONFIGURABLE_SPAN_SECONDS = Long.MAX_VALUE / 1_000_000_000L;

    /**
     * Resolved once and cached: ingestion validation reads this on every request, and the config
     * map is immutable after {@code ConfigurationManager.initialize()}. Caching also gives the
     * validation below a single well-defined place to run.
     */
    private static volatile Long cachedMaxBucketSpanSeconds = null;

    public static long getMaxBucketSpanSeconds() {
        Long resolved = cachedMaxBucketSpanSeconds;
        if (resolved == null) {
            synchronized (BucketSpanLimits.class) {
                resolved = cachedMaxBucketSpanSeconds;
                if (resolved == null) {
                    resolved = loadAndValidateMaxBucketSpanSeconds();
                    cachedMaxBucketSpanSeconds = resolved;
                }
            }
        }
        return resolved;
    }

    /**
     * Reads the configured limit and rejects values that would corrupt ingestion validation: a
     * non-positive limit rejects nearly every data frame, and an oversized limit overflows the
     * nanos conversion into a negative bound that rejects everything. Either would refuse valid
     * data on every request while looking like a validation outcome, so an out-of-range value
     * fails loudly here instead.
     *
     * @throws DpRuntimeException if the configured value is outside the supported range
     */
    private static long loadAndValidateMaxBucketSpanSeconds() {
        final long configuredValue = ConfigurationManager.getInstance()
                .getConfigLong(CFG_KEY_MAX_BUCKET_SPAN_SECONDS, DEFAULT_MAX_BUCKET_SPAN_SECONDS);

        if (configuredValue <= 0) {
            throw new DpRuntimeException(
                    "invalid configuration " + CFG_KEY_MAX_BUCKET_SPAN_SECONDS + "=" + configuredValue
                            + ": must be positive, since a non-positive bucket span limit causes "
                            + "ingestion to reject valid data");
        }

        if (configuredValue > MAX_CONFIGURABLE_SPAN_SECONDS) {
            throw new DpRuntimeException(
                    "invalid configuration " + CFG_KEY_MAX_BUCKET_SPAN_SECONDS + "=" + configuredValue
                            + ": must not exceed " + MAX_CONFIGURABLE_SPAN_SECONDS
                            + ", above which the conversion to nanoseconds overflows");
        }

        return configuredValue;
    }

    public static long getMaxBucketSpanNanos() {
        // Safe: getMaxBucketSpanSeconds() rejects anything above MAX_CONFIGURABLE_SPAN_SECONDS.
        return getMaxBucketSpanSeconds() * 1_000_000_000L;
    }

    /**
     * Resets the cached limit so a test can exercise a different configured value. Not for
     * production use; the limit is a fixed deployment setting.
     */
    public static void resetCachedLimitForTesting() {
        synchronized (BucketSpanLimits.class) {
            cachedMaxBucketSpanSeconds = null;
        }
    }
}
