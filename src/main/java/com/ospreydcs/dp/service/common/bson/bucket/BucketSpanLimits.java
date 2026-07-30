package com.ospreydcs.dp.service.common.bson.bucket;

import com.ospreydcs.dp.service.common.config.ConfigurationManager;
import com.ospreydcs.dp.service.common.exception.DpRuntimeException;

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

    /**
     * Largest configurable span. Above this, the conversion to nanos in
     * {@link #getMaxBucketSpanNanos()} would overflow a long and invert the ingestion comparison
     * into "reject everything", so the value is rejected at load time instead.
     */
    public static final long MAX_CONFIGURABLE_SPAN_SECONDS = Long.MAX_VALUE / 1_000_000_000L;

    /**
     * Resolved once and cached: the filter builder reads this per retrieval interval, and the
     * config map is immutable after {@code ConfigurationManager.initialize()}. Caching also gives
     * the validation below a single well-defined place to run.
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
     * Reads the configured limit and rejects values that would silently corrupt either side of the
     * invariant: a non-positive limit makes ingestion reject nearly everything while narrowing the
     * query bound enough to drop buckets that start before the query window, and an oversized limit
     * overflows the nanos conversion. Both failures are silent wrong answers rather than errors,
     * which is exactly what this invariant exists to prevent.
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
                            + "ingestion to reject valid data and causes time-range queries to "
                            + "silently miss buckets");
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
     * Whether the query-side time-range lower bound may be applied. Defaults to true so the bound
     * is active unless something proves it unsafe: the query service clears this when startup
     * verification finds a stored bucket exceeding the limit, or cannot complete the check.
     *
     * <p>Disabling degrades queries to the pre-#197 behavior — an unbounded index scan, slow but
     * correct — which is strictly preferable to applying a bound that silently drops buckets.
     */
    private static volatile boolean queryLowerBoundEnabled = true;

    public static boolean isQueryLowerBoundEnabled() {
        return queryLowerBoundEnabled;
    }

    /**
     * Disables the query lower bound for this process. Called when archive verification fails; the
     * setting is per-process and is re-evaluated on the next startup.
     */
    public static void disableQueryLowerBound() {
        queryLowerBoundEnabled = false;
    }

    /**
     * Resets the cached limit so a test can exercise a different configured value. Not for
     * production use; the limit is a fixed deployment invariant.
     */
    public static void resetCachedLimitForTesting() {
        synchronized (BucketSpanLimits.class) {
            cachedMaxBucketSpanSeconds = null;
            queryLowerBoundEnabled = true;
        }
    }
}
