package com.ospreydcs.dp.service.annotation.utility;

import com.ospreydcs.dp.grpc.v1.common.*;
import com.ospreydcs.dp.service.common.bson.bucket.BucketDocument;
import com.ospreydcs.dp.service.common.bson.bucket.EventMetadataDocument;
import org.junit.Test;

import java.io.IOException;
import java.time.Instant;
import java.util.Date;
import java.util.Map;
import java.util.Objects;

import static org.junit.Assert.fail;

public class DatasetExportHdf5FileTest {

    @Test
    public void testCreateExportFile() {

        // create export file and top-level group index structure
        final String exportFilePathString = "/tmp/testCreateExportFile.h5";
        DatasetExportHdf5File exportHdf5File = null;
        try {
            exportHdf5File = new DatasetExportHdf5File(exportFilePathString);
        } catch (IOException e) {
            fail("exception creating " + exportFilePathString);
        }
        Objects.requireNonNull(exportHdf5File);

        final Instant instantNow = Instant.now();
        final Instant firstInstant = instantNow.minusNanos(instantNow.getNano());
        final long firstSeconds = instantNow.getEpochSecond();
        final long firstNanos = 0;
        final Date firstTime = Date.from(firstInstant);
        final long lastSeconds = firstSeconds;
        final long lastNanos = 900000000L;
        final Instant lastInstant = firstInstant.plusNanos(lastNanos);
        final Date lastTime = Date.from(lastInstant);

        final Timestamp samplingClockStartTime = Timestamp.newBuilder()
                .setEpochSeconds(firstSeconds)
                .setNanoseconds(firstNanos)
                .build();
        final SamplingClock samplingClock =
                SamplingClock.newBuilder()
                        .setStartTime(samplingClockStartTime)
                        .setCount(10)
                        .setPeriodNanos(100000000L)
                        .build();
        final DataTimestamps dataTimestamps = DataTimestamps.newBuilder().setSamplingClock(samplingClock).build();

        final Map<String, String> attributeMap = Map.of("sector", "S01", "subsystem", "vacuum");

        final EventMetadataDocument eventMetadata = new EventMetadataDocument();
        eventMetadata.setDescription("S01 vacuum test");
        eventMetadata.setStartSeconds(firstSeconds);
        eventMetadata.setStartNanos(firstNanos);
        eventMetadata.setStopSeconds(lastSeconds);
        eventMetadata.setStopNanos(lastNanos);

        final String providerId = "S01 vacuum provider";

        // create first BucketDocument for S01-GCC01
        {
            final BucketDocument bucketDocument = new BucketDocument();
            bucketDocument.setPvName("S01-GCC01");
            bucketDocument.setFirstSeconds(firstSeconds);
            bucketDocument.setFirstNanos(firstNanos);
            bucketDocument.setFirstTime(firstTime);
            bucketDocument.setLastSeconds(lastSeconds);
            bucketDocument.setLastNanos(lastNanos);
            bucketDocument.setLastTime(lastTime);
            bucketDocument.setSampleCount(10);
            bucketDocument.setSamplePeriod(100000000L);
            final DataColumn.Builder dataColumnBuilder = DataColumn.newBuilder();
            dataColumnBuilder.setName(bucketDocument.getPvName());
            for (int i = 0; i < 10; ++i) {
                DataValue dataValue = DataValue.newBuilder().setIntValue(i).build();
                dataColumnBuilder.addDataValues(dataValue);
            }
            final DataColumn dataColumn = dataColumnBuilder.build();
            bucketDocument.setDataColumnBytes(dataColumn.toByteArray());
            bucketDocument.setDataTimestampsBytes(dataTimestamps.toByteArray());
            bucketDocument.setAttributeMap(attributeMap);
            bucketDocument.setEventMetadata(eventMetadata);
            bucketDocument.setProviderId(providerId);
            exportHdf5File.writeBucketData(bucketDocument);
        }

        // create second BucketDocument for S01-GCC02
        {
            final BucketDocument bucketDocument = new BucketDocument();
            bucketDocument.setPvName("S01-GCC02");
            bucketDocument.setFirstSeconds(firstSeconds);
            bucketDocument.setFirstNanos(firstNanos);
            bucketDocument.setFirstTime(firstTime);
            bucketDocument.setLastSeconds(lastSeconds);
            bucketDocument.setLastNanos(lastNanos);
            bucketDocument.setLastTime(lastTime);
            bucketDocument.setSampleCount(10);
            bucketDocument.setSamplePeriod(100000000L);
            final DataColumn.Builder dataColumnBuilder = DataColumn.newBuilder();
            dataColumnBuilder.setName(bucketDocument.getPvName());
            for (int i = 10; i < 20; ++i) {
                DataValue dataValue = DataValue.newBuilder().setIntValue(i).build();
                dataColumnBuilder.addDataValues(dataValue);
            }
            final DataColumn dataColumn = dataColumnBuilder.build();
            bucketDocument.setDataColumnBytes(dataColumn.toByteArray());
            bucketDocument.setDataTimestampsBytes(dataTimestamps.toByteArray());
            bucketDocument.setAttributeMap(attributeMap);
            bucketDocument.setEventMetadata(eventMetadata);
            bucketDocument.setProviderId(providerId);
            exportHdf5File.writeBucketData(bucketDocument);
        }

        exportHdf5File.close();
    }
}
