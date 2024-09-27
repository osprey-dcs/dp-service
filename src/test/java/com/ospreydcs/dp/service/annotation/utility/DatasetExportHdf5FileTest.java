package com.ospreydcs.dp.service.annotation.utility;

import com.ospreydcs.dp.grpc.v1.common.DataColumn;
import com.ospreydcs.dp.grpc.v1.common.DataValue;
import com.ospreydcs.dp.service.common.bson.bucket.BucketDocument;
import org.junit.Test;

import java.io.IOException;
import java.time.Instant;
import java.util.Date;
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

        // create first BucketDocument for S01-GCC01
        {
            final BucketDocument bucketDocument = new BucketDocument();
            bucketDocument.setPvName("S01-GCC01");
            bucketDocument.setFirstSeconds(instantNow.getEpochSecond());
            bucketDocument.setFirstNanos(instantNow.getNano());
            DataColumn.Builder dataColumnBuilder = DataColumn.newBuilder();
            dataColumnBuilder.setName(bucketDocument.getPvName());
            for (int i = 0; i < 10; ++i) {
                DataValue dataValue = DataValue.newBuilder().setIntValue(i).build();
                dataColumnBuilder.addDataValues(dataValue);
            }
            DataColumn dataColumn = dataColumnBuilder.build();
            bucketDocument.setDataColumnBytes(dataColumn.toByteArray());
            exportHdf5File.writeBucketData(bucketDocument);
        }

        // create second BucketDocument for S01-GCC02
        {
            final BucketDocument bucketDocument = new BucketDocument();
            bucketDocument.setPvName("S01-GCC02");
            bucketDocument.setFirstSeconds(instantNow.getEpochSecond());
            bucketDocument.setFirstNanos(instantNow.getNano());
            DataColumn.Builder dataColumnBuilder = DataColumn.newBuilder();
            dataColumnBuilder.setName(bucketDocument.getPvName());
            for (int i = 10; i < 20; ++i) {
                DataValue dataValue = DataValue.newBuilder().setIntValue(i).build();
                dataColumnBuilder.addDataValues(dataValue);
            }
            DataColumn dataColumn = dataColumnBuilder.build();
            bucketDocument.setDataColumnBytes(dataColumn.toByteArray());
            exportHdf5File.writeBucketData(bucketDocument);
        }

        exportHdf5File.close();
    }
}
