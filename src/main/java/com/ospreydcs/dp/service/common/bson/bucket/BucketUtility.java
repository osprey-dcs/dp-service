package com.ospreydcs.dp.service.common.bson.bucket;

import com.ospreydcs.dp.service.common.grpc.GrpcUtility;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import static java.lang.Math.round;

public class BucketUtility {
    public static List<BucketDocument> createBucketDocuments(
            long firstSeconds,
            int numSamplesPerSecond,
            int numSecondsPerBucket,
            String columnNameBase,
            int numColumns,
            int numBucketsPerColumn
    ) {
        final List<BucketDocument> bucketList = new ArrayList<>();

        final double sampleRate = 1.0 / numSamplesPerSecond;
        final int numSamplesPerBucket = numSamplesPerSecond * numSecondsPerBucket;
        final long sampleFrequency = round(sampleRate * 1_000_000_000L);

        long firstTimeSeconds = firstSeconds;
        long firstTimeNanos = 0L;

        long lastTimeSeconds = firstTimeSeconds + numSecondsPerBucket - 1;
        long lastTimeNanos = round((1L - sampleRate) * 1_000_000_000L);

        for (int batchIndex = 0 ; batchIndex < numBucketsPerColumn ; batchIndex++) {

            Date firstTimeDate =
                    GrpcUtility.dateFromTimestamp(GrpcUtility.timestampFromSeconds(firstTimeSeconds, firstTimeNanos));
            Date lastTimeDate =
                    GrpcUtility.dateFromTimestamp(GrpcUtility.timestampFromSeconds(lastTimeSeconds, lastTimeNanos));

            // create a bucket document for each column in the batch
            for (int columnIndex = 1 ; columnIndex <= numColumns ; columnIndex++) {
                String columnName = columnNameBase + columnIndex;
                String documentId = columnName + "-" + firstTimeSeconds + "-" + firstTimeNanos;
                DoubleBucketDocument bucket = new DoubleBucketDocument();
                bucket.initColumnDataList();
                bucket.setId(documentId);
                bucket.setColumnName(columnName);
                bucket.setFirstTime(firstTimeDate);
                bucket.setFirstSeconds(firstTimeSeconds);
                bucket.setFirstNanos(firstTimeNanos);
                bucket.setLastTime(lastTimeDate);
                bucket.setLastSeconds(lastTimeSeconds);
                bucket.setLastNanos(lastTimeNanos);
                bucket.setSampleFrequency(sampleFrequency);
                bucket.setNumSamples(numSamplesPerBucket);
                // fill bucket with specified number of data values
                for (int samplesIndex = 0 ; samplesIndex < numSamplesPerBucket ; samplesIndex++) {
                    double dataValue = samplesIndex;
                    bucket.addColumnData(dataValue);
                }
                bucketList.add(bucket);
            }

            // update first/last time for next batch
            firstTimeSeconds = lastTimeSeconds + 1;
            firstTimeNanos = 0L;
            lastTimeSeconds = firstTimeSeconds + numSecondsPerBucket - 1;
            lastTimeNanos = round((1L - sampleRate) * 1_000_000_000L);
        }

        return bucketList;
    }

}
