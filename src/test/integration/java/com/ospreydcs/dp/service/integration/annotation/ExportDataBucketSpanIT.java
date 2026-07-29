package com.ospreydcs.dp.service.integration.annotation;

import com.ospreydcs.dp.grpc.v1.common.DataColumn;
import com.ospreydcs.dp.grpc.v1.common.DataTimestamps;
import com.ospreydcs.dp.grpc.v1.common.DataValue;
import com.ospreydcs.dp.grpc.v1.common.SamplingClock;
import com.ospreydcs.dp.service.common.bson.DataTimestampsDocument;
import com.ospreydcs.dp.service.common.bson.bucket.BucketDocument;
import com.ospreydcs.dp.service.common.bson.bucket.BucketSpanLimits;
import com.ospreydcs.dp.service.common.bson.column.DataColumnDocument;
import com.ospreydcs.dp.service.common.bson.dataset.DataSetDocument;
import com.ospreydcs.dp.service.common.protobuf.TimestampUtility;
import com.ospreydcs.dp.service.integration.ingest.GrpcIntegrationIngestionServiceWrapper;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.time.Instant;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/**
 * Demonstrates, against running services and real ingested data, that the query-side time-range
 * lower bound (#197) silently drops an over-long bucket from the dataset retrieval path that the
 * annotation service uses for export.
 *
 * <p>The export jobs reach the bucket overlap filter through {@code executeDataBlockQuery}, the
 * same method exercised here. The annotation handler therefore has to run the same startup
 * verification the query service does; before it did, a legacy archive would export incomplete data
 * with no error and no log line.
 *
 * <p>The over-long bucket is inserted directly into MongoDB rather than through the ingestion
 * service, because ingestion validation rejects it. That is precisely how such a bucket comes to
 * exist in a real archive: it was ingested before the limit was introduced.
 */
public class ExportDataBucketSpanIT extends AnnotationIntegrationTestIntermediate {

    /** A PV belonging to the "first half" dataset built by createDataSetScenario(). */
    private static final String DATASET_PV_NAME = "S01-GCC01";

    /** 2 PVs x 5 one-second data blocks, as ingested by annotationIngestionScenario(). */
    private static final int EXPECTED_COMPLIANT_BUCKETS = 10;

    /** Identifies the deliberately over-long bucket in retrieval results. */
    private static final String OVERLONG_BUCKET_ID = DATASET_PV_NAME + "-legacy-overlong";

    @Before
    public void setUp() throws Exception {
        // Reset before starting services: handler init reads the limit and runs verification.
        BucketSpanLimits.resetCachedLimitForTesting();
        super.setUp();
    }

    /**
     * This test disables the query lower bound, which is a process-wide static. Restoring it must
     * happen even if service teardown throws, or every later test in the JVM would run against an
     * unbounded filter.
     */
    @After
    public void tearDown() {
        try {
            super.tearDown();
        } finally {
            BucketSpanLimits.resetCachedLimitForTesting();
        }
    }

    /**
     * Inserts a bucket that overlaps the dataset's time range but begins far enough before it to
     * fall outside the lower bound. Written straight to the collection so ingestion validation does
     * not reject it, standing in for data ingested before the limit existed.
     *
     * <p>Built as a fully-populated BucketDocument rather than a hand-rolled BSON document: a
     * bucket missing its data column or timestamps would NPE in
     * {@code BucketDocument.dataBucketFromDocument()} as soon as anything converted it to protobuf,
     * which would be a defect in the fixture rather than a property of the archive being modelled.
     */
    private void insertOverlongBucket(long datasetStartSeconds) {

        final long limitSeconds = BucketSpanLimits.getMaxBucketSpanSeconds();

        // Begins two limits before the dataset window and extends past it, so the overlap predicate
        // (firstTime < end AND lastTime >= begin) matches while the lower bound
        // (firstTime.seconds >= beginSeconds - maxBucketSpanSeconds) excludes it.
        final long firstTimeSeconds = datasetStartSeconds - (limitSeconds * 2);
        final long spanSeconds = (limitSeconds * 2) + 10;

        final int sampleCount = 10;
        final long samplePeriodNanos = (spanSeconds * 1_000_000_000L) / (sampleCount - 1);

        final BucketDocument bucket = new BucketDocument();
        bucket.setId(OVERLONG_BUCKET_ID);
        bucket.setPvName(DATASET_PV_NAME);

        final DataColumn.Builder dataColumnBuilder = DataColumn.newBuilder();
        dataColumnBuilder.setName(DATASET_PV_NAME);
        for (int i = 0; i < sampleCount; i++) {
            dataColumnBuilder.addDataValues(DataValue.newBuilder().setDoubleValue(i).build());
        }
        bucket.setDataColumn(DataColumnDocument.fromDataColumn(dataColumnBuilder.build()));

        final SamplingClock samplingClock = SamplingClock.newBuilder()
                .setStartTime(TimestampUtility.timestampFromSeconds(firstTimeSeconds, 0L))
                .setPeriodNanos(samplePeriodNanos)
                .setCount(sampleCount)
                .build();
        bucket.setDataTimestamps(DataTimestampsDocument.fromDataTimestamps(
                DataTimestamps.newBuilder().setSamplingClock(samplingClock).build()));

        mongoClient.insertBucketDocument(bucket);
    }

    /** Retrieves the dataset's buckets through the same path the export jobs use. */
    private List<BucketDocument> retrieveDataSetBuckets(String dataSetId) {
        final DataSetDocument dataSetDocument = mongoClient.findDataSet(dataSetId);
        assertNotNull(dataSetDocument);
        return mongoClient.findDataSetBuckets(dataSetDocument);
    }

    /**
     * Distinct bucket count. findDataSetBuckets queries each data block separately, so a bucket
     * spanning several blocks is returned once per block.
     */
    private static long distinctBucketCount(List<BucketDocument> buckets) {
        return buckets.stream().map(BucketDocument::getId).distinct().count();
    }

    /**
     * The core demonstration: with the bound active, an over-long bucket that genuinely overlaps
     * the dataset window is excluded from retrieval, and disabling the bound — the state the
     * annotation handler now enters when startup verification finds a violation — brings it back.
     *
     * <p>Both halves run in one test so the two counts are compared against identical ingested
     * data, which is what makes the difference attributable to the bound alone.
     */
    @Test
    public void testOverlongBucketDroppedByBoundAndRecoveredWhenDisabled() {

        final long startSeconds = Instant.now().getEpochSecond();

        final Map<String, GrpcIntegrationIngestionServiceWrapper.IngestionStreamInfo> validationMap =
                annotationIngestionScenario(startSeconds);
        final CreateDataSetScenarioResult dataSetResult = createDataSetScenario(startSeconds);
        final String dataSetId = dataSetResult.firstHalfDataSetId();

        // Baseline: only compliant buckets exist, so the bound changes nothing.
        assertTrue(BucketSpanLimits.isQueryLowerBoundEnabled());
        assertEquals(
                EXPECTED_COMPLIANT_BUCKETS, distinctBucketCount(retrieveDataSetBuckets(dataSetId)));

        insertOverlongBucket(startSeconds);

        // With the bound enabled the extra bucket is silently excluded: the count is unchanged even
        // though the archive now holds a bucket that overlaps the requested range.
        final List<BucketDocument> withBound = retrieveDataSetBuckets(dataSetId);
        assertEquals(EXPECTED_COMPLIANT_BUCKETS, distinctBucketCount(withBound));
        assertTrue(withBound.stream().noneMatch(b -> OVERLONG_BUCKET_ID.equals(b.getId())));

        // Disabling the bound is the fallback taken when verification fails. The same query now
        // returns the previously-dropped bucket, confirming it was the bound that excluded it and
        // not the overlap predicate.
        //
        // findDataSetBuckets queries each of the dataset's 5 one-second data blocks separately, and
        // the inserted bucket spans all of them, so it appears once per block. Compare distinct ids
        // rather than raw cursor rows.
        BucketSpanLimits.disableQueryLowerBound();
        final List<BucketDocument> withoutBound = retrieveDataSetBuckets(dataSetId);
        assertEquals(EXPECTED_COMPLIANT_BUCKETS + 1, distinctBucketCount(withoutBound));
        assertTrue(withoutBound.stream().anyMatch(b -> OVERLONG_BUCKET_ID.equals(b.getId())));
    }
}
