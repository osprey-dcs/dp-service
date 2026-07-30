package com.ospreydcs.dp.service.common.bson.bucket;

import com.mongodb.client.MongoCollection;
import com.ospreydcs.dp.service.common.bson.BsonConstants;
import com.ospreydcs.dp.service.common.mongo.MongoTestClient;
import org.bson.Document;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

/**
 * Covers the startup archive verification for the max bucket span invariant (#197). The scenarios
 * that matter are the ones where the archive predates the limit, since applying the query lower
 * bound to such an archive silently drops buckets rather than raising an error.
 */
public class BucketSpanVerifierTest {

    private static final long LIMIT_SECONDS = 86_400L;

    private VerifierTestClient testClient;
    private MongoCollection<Document> bucketsCollection;
    private MongoCollection<Document> verificationCollection;

    /** Exposes raw Document-typed collection handles, which is what the verifier operates on. */
    private static class VerifierTestClient extends MongoTestClient {

        MongoCollection<Document> bucketsAsDocuments() {
            return mongoDatabase.getCollection(getCollectionNameBuckets());
        }

        MongoCollection<Document> verificationAsDocuments() {
            return mongoDatabase.getCollection(
                    BucketSpanVerifier.COLLECTION_NAME_BUCKET_SPAN_VERIFICATION);
        }
    }

    @Before
    public void setUp() {
        testClient = new VerifierTestClient();
        testClient.init();
        bucketsCollection = testClient.bucketsAsDocuments();
        verificationCollection = testClient.verificationAsDocuments();
        bucketsCollection.deleteMany(new Document());
        verificationCollection.deleteMany(new Document());
        BucketSpanLimits.resetCachedLimitForTesting();
    }

    @After
    public void tearDown() {
        bucketsCollection.deleteMany(new Document());
        verificationCollection.deleteMany(new Document());
        testClient.fini();
        BucketSpanLimits.resetCachedLimitForTesting();
    }

    /** Inserts a well-formed bucket document carrying the fields the verifier reads. */
    private void insertBucket(String pvName, long firstSeconds, long spanSeconds) {
        bucketsCollection.insertOne(new Document()
                .append("_id", pvName + "-" + firstSeconds)
                .append(BsonConstants.BSON_KEY_PV_NAME, pvName)
                .append("dataColumn", new Document("_t", "DataColumnDocument"))
                .append("dataTimestamps", new Document()
                        .append("firstTime", new Document()
                                .append("seconds", firstSeconds).append("nanos", 0))
                        .append("lastTime", new Document()
                                .append("seconds", firstSeconds + spanSeconds).append("nanos", 0))));
    }

    /** Inserts a bucket missing dataColumn, which deserialization requires. */
    private void insertBucketMissingDataColumn(String pvName, long firstSeconds) {
        bucketsCollection.insertOne(new Document()
                .append("_id", pvName + "-no-column")
                .append(BsonConstants.BSON_KEY_PV_NAME, pvName)
                .append("dataTimestamps", new Document()
                        .append("firstTime", new Document()
                                .append("seconds", firstSeconds).append("nanos", 0))
                        .append("lastTime", new Document()
                                .append("seconds", firstSeconds + 1).append("nanos", 0))));
    }

    /** Inserts a bucket missing dataTimestamps, which deserialization requires. */
    private void insertBucketMissingDataTimestamps(String pvName) {
        bucketsCollection.insertOne(new Document()
                .append("_id", pvName + "-no-timestamps")
                .append(BsonConstants.BSON_KEY_PV_NAME, pvName)
                .append("dataColumn", new Document("_t", "DataColumnDocument")));
    }

    private BucketSpanVerifier.VerificationResult verify() {
        return BucketSpanVerifier.verify(bucketsCollection, verificationCollection, LIMIT_SECONDS);
    }

    @Test
    public void testEmptyArchiveVerifiesClean() {
        final BucketSpanVerifier.VerificationResult result = verify();
        assertEquals(BucketSpanVerifier.VerificationOutcome.VERIFIED_CLEAN, result.outcome());
        assertTrue(result.boundIsSafe());
    }

    @Test
    public void testCompliantArchiveVerifiesClean() {
        insertBucket("pv_1", 1_700_000_000L, 1);
        insertBucket("pv_2", 1_700_000_100L, 3_600);
        insertBucket("pv_3", 1_700_000_200L, LIMIT_SECONDS); // exactly at the limit is compliant

        final BucketSpanVerifier.VerificationResult result = verify();
        assertEquals(BucketSpanVerifier.VerificationOutcome.VERIFIED_CLEAN, result.outcome());
        assertTrue(result.boundIsSafe());
        assertNull(result.violatingPvName());
    }

    /**
     * The case the check exists for: a bucket ingested before the limit was enforced, which the
     * query lower bound would exclude from results without reporting anything.
     */
    @Test
    public void testOverlongBucketDetected() {
        insertBucket("pv_compliant", 1_700_000_000L, 60);
        insertBucket("pv_overlong", 1_700_000_000L, LIMIT_SECONDS + 1);

        final BucketSpanVerifier.VerificationResult result = verify();
        assertEquals(BucketSpanVerifier.VerificationOutcome.VIOLATION_FOUND, result.outcome());
        assertFalse(result.boundIsSafe());
        assertEquals("pv_overlong", result.violatingPvName());
        assertEquals(LIMIT_SECONDS + 1, result.violatingSpanSeconds());
    }

    /** A violation must not be recorded as verified, so the next startup re-checks. */
    @Test
    public void testViolationDoesNotRecordMarker() {
        insertBucket("pv_overlong", 1_700_000_000L, LIMIT_SECONDS * 2);
        verify();
        assertEquals(0, verificationCollection.countDocuments());
    }

    @Test
    public void testSecondRunSkipsScanAfterCleanVerification() {
        insertBucket("pv_1", 1_700_000_000L, 1);

        assertEquals(
                BucketSpanVerifier.VerificationOutcome.VERIFIED_CLEAN, verify().outcome());
        assertEquals(1, verificationCollection.countDocuments());

        final BucketSpanVerifier.VerificationResult second = verify();
        assertEquals(
                BucketSpanVerifier.VerificationOutcome.SKIPPED_ALREADY_VERIFIED, second.outcome());
        assertTrue(second.boundIsSafe());
    }

    /**
     * A marker recorded at a smaller limit still implies compliance with a larger one, since every
     * bucket fitting the smaller span also fits the larger.
     */
    @Test
    public void testMarkerFromSmallerLimitSatisfiesLargerLimit() {
        insertBucket("pv_1", 1_700_000_000L, 1);
        BucketSpanVerifier.verify(bucketsCollection, verificationCollection, 3_600L);

        final BucketSpanVerifier.VerificationResult result =
                BucketSpanVerifier.verify(bucketsCollection, verificationCollection, LIMIT_SECONDS);
        assertEquals(
                BucketSpanVerifier.VerificationOutcome.SKIPPED_ALREADY_VERIFIED, result.outcome());
    }

    /**
     * A bucket missing dataColumn cannot be deserialized, so every query covering it fails. The
     * scan reports it for repair but leaves the span bound enabled, since corruption and the span
     * invariant are unrelated.
     */
    @Test
    public void testMissingDataColumnReported() {
        insertBucket("pv_ok", 1_700_000_000L, 1);
        insertBucketMissingDataColumn("pv_broken", 1_700_000_100L);

        final BucketSpanVerifier.VerificationResult result = verify();
        assertTrue(result.foundCorruptBucket());
        assertEquals("pv_broken", result.corruptBucket().pvName());
        assertEquals("pv_broken-no-column", result.corruptBucket().bucketId());
        assertEquals("dataColumn", result.corruptBucket().missingField());

        // Corruption does not make the span bound unsafe.
        assertTrue(result.boundIsSafe());
    }

    @Test
    public void testMissingDataTimestampsReported() {
        insertBucketMissingDataTimestamps("pv_broken");

        final BucketSpanVerifier.VerificationResult result = verify();
        assertTrue(result.foundCorruptBucket());
        assertEquals("dataTimestamps", result.corruptBucket().missingField());
        assertTrue(result.boundIsSafe());
    }

    /**
     * An unrepaired bucket must keep being reported: recording the marker would silence the scan on
     * the next startup while the problem persists.
     */
    @Test
    public void testCorruptBucketDoesNotRecordMarker() {
        insertBucketMissingDataColumn("pv_broken", 1_700_000_000L);

        assertTrue(verify().foundCorruptBucket());
        assertEquals(0, verificationCollection.countDocuments());

        // Still reported on a subsequent run rather than skipped as already verified.
        final BucketSpanVerifier.VerificationResult second = verify();
        assertEquals(BucketSpanVerifier.VerificationOutcome.VERIFIED_CLEAN, second.outcome());
        assertTrue(second.foundCorruptBucket());
    }

    /** A clean archive reports no corruption, so well-formed buckets are not false positives. */
    @Test
    public void testWellFormedArchiveReportsNoCorruption() {
        insertBucket("pv_1", 1_700_000_000L, 1);
        insertBucket("pv_2", 1_700_000_100L, 3_600);

        final BucketSpanVerifier.VerificationResult result = verify();
        assertEquals(BucketSpanVerifier.VerificationOutcome.VERIFIED_CLEAN, result.outcome());
        assertFalse(result.foundCorruptBucket());
        assertNull(result.corruptBucket());
    }

    /**
     * Lowering the limit is a stronger claim than the recorded one, so the archive must be
     * re-scanned rather than trusting the existing marker.
     */
    @Test
    public void testMarkerFromLargerLimitTriggersRescan() {
        insertBucket("pv_overlong", 1_700_000_000L, 7_200);
        BucketSpanVerifier.verify(bucketsCollection, verificationCollection, LIMIT_SECONDS);
        assertEquals(1, verificationCollection.countDocuments());

        // Re-verify at a limit the stored bucket violates; the old marker must not mask it.
        final BucketSpanVerifier.VerificationResult result =
                BucketSpanVerifier.verify(bucketsCollection, verificationCollection, 3_600L);
        assertEquals(BucketSpanVerifier.VerificationOutcome.VIOLATION_FOUND, result.outcome());
        assertEquals("pv_overlong", result.violatingPvName());
    }
}
