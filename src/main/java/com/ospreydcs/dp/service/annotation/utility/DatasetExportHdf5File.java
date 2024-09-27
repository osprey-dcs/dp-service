package com.ospreydcs.dp.service.annotation.utility;

import ch.systemsx.cisd.hdf5.HDF5Factory;
import ch.systemsx.cisd.hdf5.IHDF5Writer;
import com.ospreydcs.dp.service.common.bson.bucket.BucketDocument;

import java.io.File;
import java.io.IOException;

public class DatasetExportHdf5File {

    private final IHDF5Writer writer;

    public DatasetExportHdf5File(String filePathString) throws IOException {
        // create hdf5 file with specified path
        File hdf5File = new File(filePathString);
//        if (hdf5File.canWrite()) {
//            throw new IOException("unable to write to hdf5 file: " + filePathString);
//        }
        writer = HDF5Factory.configure(hdf5File).overwrite().writer();
        this.initialize();
    }

    private void initialize() {
        this.createGroups();
    }

    public void createGroups() {
        // create top-level groups for file organization
        // TODO: use constants
        writer.object().createGroup("dataset");
        writer.object().createGroup("pvs");
        writer.object().createGroup("times");
    }

    public void writeBucketData(BucketDocument bucketDocument) {

//        - /root/dataset/[list of data block pojo]
//        - where each data block pojo contains list of PVs and time range begin/end times, ordered by time
//        - /root/pvs/[S01_GCC01, …]/times/[T01, T02, T03, …]/bucket
//                - bucket for S01_GCC01 starting at time T01
//                - /root/times/[T01, T02, T03, …]/pvs/[S01_GCC01, …]/bucket
//                -  bucket for S01_GCC01 starting at time T01
//                - points to same object as /root/pvs/… entry

        // create pv-specific groups in hdf5 file
        final String pvNameGroup = "pvs/" + bucketDocument.getPvName();
        if (! writer.object().isGroup(pvNameGroup)) {
            writer.object().createGroup(pvNameGroup);
        }
        final String pvTimesGroup = pvNameGroup + "/times";
        if (! writer.object().isGroup(pvTimesGroup)) {
            writer.object().createGroup(pvTimesGroup);
        }
        final String firstSecondsString = String.format("%012d", bucketDocument.getFirstSeconds());
        final String pvTimesSecondsGroup = pvTimesGroup + "/" + firstSecondsString;
        if (! writer.object().isGroup(pvTimesSecondsGroup)) {
            writer.object().createGroup(pvTimesSecondsGroup);
        }
        final String firstNanosString = String.format("%012d", bucketDocument.getFirstNanos());
        final String pvTimesSecondsNanosGroup = pvTimesSecondsGroup + "/" + firstNanosString;
        if (! writer.object().isGroup(pvTimesSecondsNanosGroup)) {
            writer.object().createGroup(pvTimesSecondsNanosGroup);
        }

        // write fields from bucket document (including column data values) document under pv-specific path
        final String bucketDataPath = pvTimesSecondsNanosGroup + "/data";
        writer.writeByteArray(bucketDataPath, bucketDocument.getDataColumnBytes());

        // create timestamp-specific group in hdf5 file
        final String timesSecondsGroup = "times/" + firstSecondsString;
        if (! writer.object().isGroup(timesSecondsGroup)) {
            writer.object().createGroup(timesSecondsGroup);
        }
        final String timesSecondsNanosGroup = timesSecondsGroup + "/" + firstNanosString;
        if (! writer.object().isGroup(timesSecondsNanosGroup)) {
            writer.object().createGroup(timesSecondsNanosGroup);
        }
        final String timesSecondsNanosPvsGroup = timesSecondsNanosGroup + "/pvs";
        if (! writer.object().isGroup(timesSecondsNanosPvsGroup)) {
            writer.object().createGroup(timesSecondsNanosPvsGroup);
        }

        // create soft link to bucket document under pvs path from times path
        final String timesSecondsNanosPvsPvPath = timesSecondsNanosPvsGroup + "/" + bucketDocument.getPvName();
        if (! writer.object().exists(timesSecondsNanosPvsPvPath)) {
            writer.object().createSoftLink("/" + pvTimesSecondsNanosGroup, timesSecondsNanosPvsPvPath);
        }

//            writer.string().write("/groupA/string", "Just some random string.");
//            writer.int32().writeArray("/groupB/inarr", new int[]
//                    { 17, 42, -1 });
//            writer.float64().writeMatrix("/groupB/dmat", new double[][]
//                    {
//                            { 1.1, 2.2, 3.3 },
//                            { 4.4, 5.5, 6.6 },
//                            { 7.7, 8.8, 9.9 }, });
//            writer.object().createSoftLink("/groupA/groupC", "/groupB/groupC");
//            writer.time().write("/groupA/date", new Date());
//            writer.close();

    }

    public void close() {
        writer.close();
    }
}