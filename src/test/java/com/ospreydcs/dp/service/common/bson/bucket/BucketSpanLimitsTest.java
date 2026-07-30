package com.ospreydcs.dp.service.common.bson.bucket;

import com.ospreydcs.dp.service.common.config.ConfigurationManagerTestBase;
import com.ospreydcs.dp.service.common.exception.DpRuntimeException;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.Properties;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Covers loading and validation of the max bucket span limit (#197). The limit is a shared
 * invariant between ingestion and query, and both of its failure modes are silent wrong answers
 * rather than errors, so the out-of-range cases below must fail loudly.
 */
public class BucketSpanLimitsTest extends ConfigurationManagerTestBase {

    private static final String OVERRIDE_PROPERTY_KEY = "dp.Buckets.maxBucketSpanSeconds";

    @Before
    public void setUp() {
        BucketSpanLimits.resetCachedLimitForTesting();
    }

    @After
    public void tearDown() {
        ConfigurationManagerDerived.resetInstance();
        BucketSpanLimits.resetCachedLimitForTesting();
    }

    /** Installs a ConfigurationManager whose config map carries the given limit value. */
    private void setConfiguredSpanSeconds(String value) {
        final Properties overrideProperties = new Properties();
        overrideProperties.setProperty(OVERRIDE_PROPERTY_KEY, value);
        ConfigurationManagerDerived.setInstance(null, overrideProperties);
        BucketSpanLimits.resetCachedLimitForTesting();
    }

    @Test
    public void testConfiguredValueIsUsed() {
        setConfiguredSpanSeconds("3600");
        assertEquals(3600L, BucketSpanLimits.getMaxBucketSpanSeconds());
        assertEquals(3_600_000_000_000L, BucketSpanLimits.getMaxBucketSpanNanos());
    }

    @Test
    public void testDefaultUsedWhenUnconfigured() {
        // no Buckets key in the config map -> documented default of one day
        ConfigurationManagerDerived.setInstance(null, new Properties());
        BucketSpanLimits.resetCachedLimitForTesting();
        assertEquals(
                BucketSpanLimits.DEFAULT_MAX_BUCKET_SPAN_SECONDS,
                BucketSpanLimits.getMaxBucketSpanSeconds());
    }

    /**
     * A zero limit would make ingestion reject any frame with more than one distinct timestamp,
     * while narrowing the query lower bound to beginSeconds and dropping every bucket that starts
     * before the query window.
     */
    @Test
    public void testZeroSpanRejected() {
        setConfiguredSpanSeconds("0");
        try {
            BucketSpanLimits.getMaxBucketSpanSeconds();
            fail("expected DpRuntimeException for zero bucket span limit");
        } catch (DpRuntimeException ex) {
            assertTrue(ex.getMessage().contains("must be positive"));
        }
    }

    @Test
    public void testNegativeSpanRejected() {
        setConfiguredSpanSeconds("-1");
        try {
            BucketSpanLimits.getMaxBucketSpanSeconds();
            fail("expected DpRuntimeException for negative bucket span limit");
        } catch (DpRuntimeException ex) {
            assertTrue(ex.getMessage().contains("must be positive"));
        }
    }

    /**
     * Without the range check, this value overflows the nanos conversion to a negative number,
     * making the ingestion comparison "spanNanos > maxSpanNanos" true for every request and
     * rejecting all ingestion.
     */
    @Test
    public void testOverflowingSpanRejected() {
        setConfiguredSpanSeconds(String.valueOf(BucketSpanLimits.MAX_CONFIGURABLE_SPAN_SECONDS + 1));
        try {
            BucketSpanLimits.getMaxBucketSpanSeconds();
            fail("expected DpRuntimeException for oversized bucket span limit");
        } catch (DpRuntimeException ex) {
            assertTrue(ex.getMessage().contains("overflows"));
        }
    }

    /** The largest value that still converts to nanos without overflowing is accepted. */
    @Test
    public void testMaxConfigurableSpanAccepted() {
        setConfiguredSpanSeconds(String.valueOf(BucketSpanLimits.MAX_CONFIGURABLE_SPAN_SECONDS));
        assertEquals(
                BucketSpanLimits.MAX_CONFIGURABLE_SPAN_SECONDS,
                BucketSpanLimits.getMaxBucketSpanSeconds());
        assertTrue(BucketSpanLimits.getMaxBucketSpanNanos() > 0);
    }
}
