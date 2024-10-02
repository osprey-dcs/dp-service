package com.ospreydcs.dp.service.annotation.utility;

import ch.systemsx.cisd.hdf5.HDF5Factory;
import ch.systemsx.cisd.hdf5.IHDF5Reader;
import com.ospreydcs.dp.grpc.v1.common.*;
import com.ospreydcs.dp.service.annotation.AnnotationTestBase;
import com.ospreydcs.dp.service.common.bson.bucket.BucketDocument;
import com.ospreydcs.dp.service.common.bson.bucket.EventMetadataDocument;
import org.junit.Test;

import java.io.IOException;
import java.time.Instant;
import java.util.Date;
import java.util.Map;
import java.util.Objects;

import static org.junit.Assert.*;

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
        final int sampleCount = 10;
        final long samplePeriod = 100000000L;
        final SamplingClock samplingClock =
                SamplingClock.newBuilder()
                        .setStartTime(samplingClockStartTime)
                        .setCount(sampleCount)
                        .setPeriodNanos(samplePeriod)
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
        BucketDocument pv1BucketDocument = null;
        {
            pv1BucketDocument = new BucketDocument();
            pv1BucketDocument.setPvName("S01-GCC01");
            pv1BucketDocument.setFirstSeconds(firstSeconds);
            pv1BucketDocument.setFirstNanos(firstNanos);
            pv1BucketDocument.setFirstTime(firstTime);
            pv1BucketDocument.setLastSeconds(lastSeconds);
            pv1BucketDocument.setLastNanos(lastNanos);
            pv1BucketDocument.setLastTime(lastTime);
            pv1BucketDocument.setSampleCount(sampleCount);
            pv1BucketDocument.setSamplePeriod(samplePeriod);
            final DataColumn.Builder dataColumnBuilder = DataColumn.newBuilder();
            dataColumnBuilder.setName(pv1BucketDocument.getPvName());
            for (int i = 0; i < sampleCount; ++i) {
                DataValue dataValue = DataValue.newBuilder().setIntValue(i).build();
                dataColumnBuilder.addDataValues(dataValue);
            }
            final DataColumn dataColumn = dataColumnBuilder.build();
            pv1BucketDocument.setDataColumnBytes(dataColumn.toByteArray());
            pv1BucketDocument.setDataTimestampsBytes(dataTimestamps.toByteArray());
            pv1BucketDocument.setAttributeMap(attributeMap);
            pv1BucketDocument.setEventMetadata(eventMetadata);
            pv1BucketDocument.setProviderId(providerId);
            exportHdf5File.writeBucketData(pv1BucketDocument);
        }

        // create second BucketDocument for S01-GCC02
        BucketDocument pv2BucketDocument = null;
        {
            pv2BucketDocument = new BucketDocument();
            pv2BucketDocument.setPvName("S01-GCC02");
            pv2BucketDocument.setFirstSeconds(firstSeconds);
            pv2BucketDocument.setFirstNanos(firstNanos);
            pv2BucketDocument.setFirstTime(firstTime);
            pv2BucketDocument.setLastSeconds(lastSeconds);
            pv2BucketDocument.setLastNanos(lastNanos);
            pv2BucketDocument.setLastTime(lastTime);
            pv2BucketDocument.setSampleCount(sampleCount);
            pv2BucketDocument.setSamplePeriod(samplePeriod);
            final DataColumn.Builder dataColumnBuilder = DataColumn.newBuilder();
            dataColumnBuilder.setName(pv2BucketDocument.getPvName());
            for (int i = sampleCount; i < 20; ++i) {
                DataValue dataValue = DataValue.newBuilder().setIntValue(i).build();
                dataColumnBuilder.addDataValues(dataValue);
            }
            DataColumn dataColumn = dataColumnBuilder.build();
            pv2BucketDocument.setDataColumnBytes(dataColumn.toByteArray());
            pv2BucketDocument.setDataTimestampsBytes(dataTimestamps.toByteArray());
            pv2BucketDocument.setAttributeMap(attributeMap);
            pv2BucketDocument.setEventMetadata(eventMetadata);
            pv2BucketDocument.setProviderId(providerId);
            exportHdf5File.writeBucketData(pv2BucketDocument);
        }

        exportHdf5File.close();

        // verify file content
        final IHDF5Reader reader = HDF5Factory.openForReading("/tmp/testCreateExportFile.h5");
        AnnotationTestBase.verifyBucketDocumentHdf5Content(reader, pv1BucketDocument);
        AnnotationTestBase.verifyBucketDocumentHdf5Content(reader, pv2BucketDocument);

        // close reader
        reader.close();
    }
}
