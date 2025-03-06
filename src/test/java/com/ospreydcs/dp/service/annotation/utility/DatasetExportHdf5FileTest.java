package com.ospreydcs.dp.service.annotation.utility;

import ch.systemsx.cisd.hdf5.HDF5Factory;
import ch.systemsx.cisd.hdf5.IHDF5Reader;
import com.ospreydcs.dp.grpc.v1.common.*;
import com.ospreydcs.dp.service.annotation.AnnotationTestBase;
import com.ospreydcs.dp.service.annotation.handler.mongo.export.DatasetExportHdf5File;
import com.ospreydcs.dp.service.common.bson.DataColumnDocument;
import com.ospreydcs.dp.service.common.bson.DataTimestampsDocument;
import com.ospreydcs.dp.service.common.bson.bucket.BucketDocument;
import com.ospreydcs.dp.service.common.bson.EventMetadataDocument;
import com.ospreydcs.dp.service.common.bson.dataset.DataBlockDocument;
import com.ospreydcs.dp.service.common.bson.dataset.DataSetDocument;
import com.ospreydcs.dp.service.common.exception.DpException;
import org.bson.types.ObjectId;
import org.junit.Test;

import java.time.Instant;
import java.util.*;

import static org.junit.Assert.*;

public class DatasetExportHdf5FileTest {

    @Test
    public void testCreateExportFile() {

        final Instant instantNow = Instant.now();
        final Instant firstInstant = instantNow.minusNanos(instantNow.getNano());
        final long firstSeconds = instantNow.getEpochSecond();
        final long firstNanos = 0;
        final Date firstTime = Date.from(firstInstant);
        final long lastSeconds = firstSeconds;
        final long lastNanos = 900000000L;
        final Instant lastInstant = firstInstant.plusNanos(lastNanos);
        final Date lastTime = Date.from(lastInstant);

        // create dataset for export
        final DataSetDocument dataset = new DataSetDocument();
        dataset.setName("export dataset");
        dataset.setDescription("test coverage for export to hdf5 file");
        dataset.setId(new ObjectId("1234abcd1234abcd1234abcd")); // must be 24 digit hex
        final List<DataBlockDocument> dataBlocks = new ArrayList<>();
        DataBlockDocument dataBlock1 = new DataBlockDocument();
        dataBlock1.setPvNames(Set.of("S01-GCC01", "S01-GCC02"));
        dataBlock1.setBeginTimeSeconds(firstSeconds);
        dataBlock1.setBeginTimeNanos(firstNanos);
        dataBlock1.setEndTimeSeconds(lastSeconds);
        dataBlock1.setEndTimeNanos(lastNanos);
        dataBlocks.add(dataBlock1);
        dataset.setDataBlocks(dataBlocks);

        // create export file and top-level group index structure
        final String exportFilePathString = "/tmp/testCreateExportFile.h5";
        DatasetExportHdf5File exportHdf5File = null;
        try {
            exportHdf5File = new DatasetExportHdf5File(dataset, exportFilePathString);
        } catch (DpException e) {
            fail("exception creating " + exportFilePathString);
        }
        Objects.requireNonNull(exportHdf5File);

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
        final DataTimestampsDocument dataTimestampsDocument = DataTimestampsDocument.fromDataTimestamps(dataTimestamps);

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
            pv1BucketDocument.setDataTimestamps(dataTimestampsDocument);
            final DataColumn.Builder dataColumnBuilder = DataColumn.newBuilder();
            dataColumnBuilder.setName(pv1BucketDocument.getPvName());
            for (int i = 0; i < sampleCount; ++i) {
                DataValue dataValue = DataValue.newBuilder().setIntValue(i).build();
                dataColumnBuilder.addDataValues(dataValue);
            }
            final DataColumn dataColumn = dataColumnBuilder.build();
            final DataColumnDocument dataColumnDocument = DataColumnDocument.fromDataColumn(dataColumn);
            pv1BucketDocument.setDataColumn(dataColumnDocument);
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
            pv2BucketDocument.setDataTimestamps(dataTimestampsDocument);
            final DataColumn.Builder dataColumnBuilder = DataColumn.newBuilder();
            dataColumnBuilder.setName(pv2BucketDocument.getPvName());
            for (int i = sampleCount; i < 20; ++i) {
                DataValue dataValue = DataValue.newBuilder().setIntValue(i).build();
                dataColumnBuilder.addDataValues(dataValue);
            }
            DataColumn dataColumn = dataColumnBuilder.build();
            final DataColumnDocument dataColumnDocument = DataColumnDocument.fromDataColumn(dataColumn);
            pv2BucketDocument.setDataColumn(dataColumnDocument);
            pv2BucketDocument.setAttributeMap(attributeMap);
            pv2BucketDocument.setEventMetadata(eventMetadata);
            pv2BucketDocument.setProviderId(providerId);
            exportHdf5File.writeBucketData(pv2BucketDocument);
        }

        exportHdf5File.close();

        // verify file content
        final IHDF5Reader reader = HDF5Factory.openForReading("/tmp/testCreateExportFile.h5");
        AnnotationTestBase.verifyDatasetHdf5Content(reader, dataset);
        AnnotationTestBase.verifyBucketDocumentHdf5Content(reader, pv1BucketDocument);
        AnnotationTestBase.verifyBucketDocumentHdf5Content(reader, pv2BucketDocument);

        // close reader
        reader.close();
    }
}
