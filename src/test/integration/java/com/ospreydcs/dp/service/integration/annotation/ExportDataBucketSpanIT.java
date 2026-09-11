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
import org.junit.Test;

import java.time.Instant;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/**
 * Demonstrates, against running services and real ingested data, how the query-side time-range
 * lower bound treats a bucket longer than the ingestion limit on the dataset retrieval path the
 * annotation service uses for export.
 *
 * <p>The export jobs reach the bucket overlap filter through {@code executeDataBlockQuery}, the
 * same method exercised here. Since #232 the bound's span comes per PV from the {@code pvStats}
 * collection rather than from the configured limit: a bucket is retrieved when its PV's recorded
 * max span reaches back to it, and excluded — silently, as an ordinary short result — when not.
 * The two tests pin both outcomes.
 *
 * <p>The over-long bucket is inserted directly into MongoDB rather than through the ingestion
 * service, because ingestion validation rejects it. That is precisely how such a bucket comes to
 * exist in a real archive: it was ingested before the limit was introduced. Migration v5 records
 * the span of every stored bucket in {@code pvStats}, so a migrated archive is in the state the
 * first test seeds by hand; the second test models a bucket that bypassed both ingestion and the
 * migration.
 */
public class ExportDataBucketSpanIT extends AnnotationIntegrationTestIntermediate {

    /** A PV belonging to the "first half" dataset built by createDataSetScenario(). */
    private static final String DATASET_PV_NAME = "S01-GCC01";

    /** 2 PVs x 5 one-second data blocks, as ingested by annotationIngestionScenario(). */
    private static final int EXPECTED_COMPLIANT_BUCKETS = 10;

    /** Identifies the deliberately over-long bucket in retrieval results. */
    private static final String OVERLONG_BUCKET_ID = DATASET_PV_NAME + "-legacy-overlong";

    /**
     * Inserts a bucket that overlaps the dataset's time range but begins far enough before it that
     * only a pvStats span covering it lets the lower bound admit it. Written straight to the
     * collection so ingestion validation does not reject it, standing in for data ingested before
     * the limit existed. Returns the bucket's span in seconds, for seeding pvStats.
     *
     * <p>Built as a fully-populated BucketDocument rather than a hand-rolled BSON document: a
     * bucket missing its data column or timestamps would NPE in
     * {@code BucketDocument.dataBucketFromDocument()} as soon as anything converted it to protobuf,
     * which would be a defect in the fixture rather than a property of the archive being modelled.
     */
    private long insertOverlongBucket(long datasetStartSeconds) {

        final long limitSeconds = BucketSpanLimits.getMaxBucketSpanSeconds();

        // Begins two ingestion limits before the dataset window and extends past it, so the overlap
        // predicate (firstTime < end AND lastTime >= begin) matches, while the lower bound
        // (firstTime.seconds >= beginSeconds - span) admits it only for a recorded span of at least
        // two limits — far beyond what this PV's ingested one-second buckets record.
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
        return spanSeconds;
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
     * With the over-long bucket's span recorded in pvStats — the state migration v5 leaves a legacy
     * archive in — the per-PV bound reaches back to it and the export retrieval returns it alongside
     * the compliant buckets. Before #232 the same bucket was silently dropped by a bound derived
     * from the configured limit.
     *
     * <p>findDataSetBuckets queries each of the dataset's 5 one-second data blocks separately, and
     * the inserted bucket spans all of them, so it appears once per block. Compare distinct ids
     * rather than raw cursor rows.
     */
    @Test
    public void testOverlongBucketReturnedWhenPvStatsRecordsItsSpan() {

        final long startSeconds = Instant.now().getEpochSecond();

        annotationIngestionScenario(startSeconds);
        final CreateDataSetScenarioResult dataSetResult = createDataSetScenario(startSeconds);
        final String dataSetId = dataSetResult.firstHalfDataSetId();

        // Baseline: only compliant buckets exist.
        assertEquals(
                EXPECTED_COMPLIANT_BUCKETS, distinctBucketCount(retrieveDataSetBuckets(dataSetId)));

        final long spanSeconds = insertOverlongBucket(startSeconds);
        mongoClient.upsertPvStatsMaxSpan(DATASET_PV_NAME, spanSeconds);

        final List<BucketDocument> buckets = retrieveDataSetBuckets(dataSetId);
        assertEquals(EXPECTED_COMPLIANT_BUCKETS + 1, distinctBucketCount(buckets));
        assertTrue(buckets.stream().anyMatch(b -> OVERLONG_BUCKET_ID.equals(b.getId())));
    }

    /**
     * The limitation the per-PV bound carries (plan D6/D7): a bucket that reached the collection
     * without passing through ingestion or migration v5 has no pvStats span behind it, so the bound
     * derived from this PV's ingested one-second buckets excludes it — no error, no log line, the
     * result is simply short by one bucket. The recourse is to record the span, as the first test
     * does; the configured ingestion limit has no part in the outcome.
     */
    @Test
    public void testOverlongBucketWithoutPvStatsSpanIsNotReturned() {

        final long startSeconds = Instant.now().getEpochSecond();

        annotationIngestionScenario(startSeconds);
        final CreateDataSetScenarioResult dataSetResult = createDataSetScenario(startSeconds);
        final String dataSetId = dataSetResult.firstHalfDataSetId();

        insertOverlongBucket(startSeconds);

        final List<BucketDocument> buckets = retrieveDataSetBuckets(dataSetId);
        assertEquals(EXPECTED_COMPLIANT_BUCKETS, distinctBucketCount(buckets));
        assertTrue(buckets.stream().noneMatch(b -> OVERLONG_BUCKET_ID.equals(b.getId())));
    }
}
