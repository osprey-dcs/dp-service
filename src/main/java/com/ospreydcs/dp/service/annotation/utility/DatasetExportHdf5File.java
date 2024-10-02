package com.ospreydcs.dp.service.annotation.utility;

import ch.systemsx.cisd.hdf5.HDF5Factory;
import ch.systemsx.cisd.hdf5.IHDF5Writer;
import com.ospreydcs.dp.service.common.bson.bucket.BucketDocument;

import java.io.File;
import java.io.IOException;
import java.util.Objects;

/*
 * Export file directory structure (using hdf5 groups):
 *
 * /root/dataset : Contains details about the dataset exported to this file, with list of pvs and begin/end times
 * for each data block (TODO).
 *
 * /root/pvs : Contains index structure by pv name and bucket first time (seconds and nanos), where each leaf directory
 * contains the bucket document fields for the bucket whose first time matches the directory path's seconds and nanos.
 * E.g., "/pvs/S01-GCC01/times/001727467265/000353987280" contains fields for the S01-GCC01 bucket document whose first
 * time seconds is 001727467265 and nanos 000353987280.
 *
 * /root/times : Contains index structure by 1) the first time seconds and nanos fields of the dataset's buckets and
 * 2) the pv names with buckets whose first time matches the path's seconds and nanos. The entry for each pv name is a
 * soft link to the directory containing the bucket document fields for the bucket for that pv whose first time matches
 * the path name's seconds and nanos.  E.g., "/root/times/001727467265/000353987280/pvs/S01-GCC01" is linked to
 * "/pvs/S01-GCC01/times/001727467265/000353987280", so the bucket fields described above are also available
 * by navigating the soft link.
 */

public class DatasetExportHdf5File {

    // constants
    public final static String GROUP_DATASET = "dataset";
    public final static String GROUP_PVS = "pvs";
    public final static String GROUP_TIMES = "times";
    public final static String DATASET_FIRST_SECONDS = "firstSeconds";
    public final static String DATASET_FIRST_NANOS = "firstNanos";
    public final static String DATASET_FIRST_TIME = "firstTime";
    public final static String DATASET_LAST_SECONDS = "lastSeconds";
    public final static String DATASET_LAST_NANOS = "lastNanos";
    public final static String DATASET_LAST_TIME = "lastTime";
    public final static String DATASET_SAMPLE_COUNT = "sampleCount";
    public final static String DATASET_SAMPLE_PERIOD = "samplePeriod";
    public final static String DATASET_DATA_COLUMN_BYTES = "dataColumnBytes";
    public final static String DATASET_DATA_TIMESTAMPS_BYTES = "dataTimestampsBytes";
    public final static String DATASET_ATTRIBUTE_MAP_KEYS = "attributeMapKeys";
    public final static String DATASET_ATTRIBUTE_MAP_VALUES = "attributeMapValues";
    public final static String DATASET_EVENT_METADATA_DESCRIPTION = "eventMetadataDescription";
    public final static String DATASET_EVENT_METADATA_START_SECONDS = "eventMetadataStartSeconds";
    public final static String DATASET_EVENT_METADATA_START_NANOS = "eventMetadataStartNanos";
    public final static String DATASET_EVENT_METADATA_STOP_SECONDS = "eventMetadataStopSeconds";
    public final static String DATASET_EVENT_METADATA_STOP_NANOS = "eventMetadataStopNanos";
    public final static String DATASET_PROVIDER_ID = "providerId";
    public final static String PATH_SEPARATOR = "/";

    // instance variables
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
        writer.object().createGroup(GROUP_DATASET);
        writer.object().createGroup(GROUP_PVS);
        writer.object().createGroup(GROUP_TIMES);
    }

    public void writeBucketData(BucketDocument bucketDocument) {

        // create groups for indexing by pv and time
        Objects.requireNonNull(bucketDocument.getPvName());
        final String pvNameGroup = GROUP_PVS + PATH_SEPARATOR + bucketDocument.getPvName();
        if (! writer.object().isGroup(pvNameGroup)) {
            writer.object().createGroup(pvNameGroup);
        }
        final String pvTimesGroup = pvNameGroup + PATH_SEPARATOR + GROUP_TIMES;
        if (! writer.object().isGroup(pvTimesGroup)) {
            writer.object().createGroup(pvTimesGroup);
        }
        Objects.requireNonNull(bucketDocument.getFirstSeconds());
        final String firstSecondsString = String.format("%012d", bucketDocument.getFirstSeconds());
        final String pvTimesSecondsGroup = pvTimesGroup + PATH_SEPARATOR + firstSecondsString;
        if (! writer.object().isGroup(pvTimesSecondsGroup)) {
            writer.object().createGroup(pvTimesSecondsGroup);
        }
        Objects.requireNonNull(bucketDocument.getFirstNanos());
        final String firstNanosString = String.format("%012d", bucketDocument.getFirstNanos());
        final String pvTimesSecondsNanosGroup = pvTimesSecondsGroup + PATH_SEPARATOR + firstNanosString;
        if (! writer.object().isGroup(pvTimesSecondsNanosGroup)) {
            writer.object().createGroup(pvTimesSecondsNanosGroup);
        }

        // write fields from bucket document (including column data values) document under pv index

        // first seconds/nanos/time
        Objects.requireNonNull(bucketDocument.getFirstTime());
        final String firstSecondsPath = pvTimesSecondsNanosGroup + PATH_SEPARATOR + DATASET_FIRST_SECONDS;
        writer.writeLong(firstSecondsPath, bucketDocument.getFirstSeconds());
        final String firstNanosPath = pvTimesSecondsNanosGroup + PATH_SEPARATOR + DATASET_FIRST_NANOS;
        writer.writeLong(firstNanosPath, bucketDocument.getFirstNanos());
        final String firstTimePath = pvTimesSecondsNanosGroup + PATH_SEPARATOR + DATASET_FIRST_TIME;
        writer.time().write(firstTimePath, bucketDocument.getFirstTime());

        // last seconds/nanos/time
        Objects.requireNonNull(bucketDocument.getLastSeconds());
        Objects.requireNonNull(bucketDocument.getLastNanos());
        Objects.requireNonNull(bucketDocument.getLastTime());
        final String lastSecondsPath = pvTimesSecondsNanosGroup + PATH_SEPARATOR + DATASET_LAST_SECONDS;
        writer.writeLong(lastSecondsPath, bucketDocument.getLastSeconds());
        final String lastNanosPath = pvTimesSecondsNanosGroup + PATH_SEPARATOR + DATASET_LAST_NANOS;
        writer.writeLong(lastNanosPath, bucketDocument.getLastNanos());
        final String lastTimePath = pvTimesSecondsNanosGroup + PATH_SEPARATOR + DATASET_LAST_TIME;
        writer.time().write(lastTimePath, bucketDocument.getLastTime());

        // sample period and count
        Objects.requireNonNull(bucketDocument.getSampleCount());
        Objects.requireNonNull(bucketDocument.getSamplePeriod());
        final String sampleCountPath = pvTimesSecondsNanosGroup + PATH_SEPARATOR + DATASET_SAMPLE_COUNT;
        writer.writeInt(sampleCountPath, bucketDocument.getSampleCount());
        final String samplePeriodPath = pvTimesSecondsNanosGroup + PATH_SEPARATOR + DATASET_SAMPLE_PERIOD;
        writer.writeLong(samplePeriodPath, bucketDocument.getSamplePeriod());

        // dataColumnBytes
        Objects.requireNonNull(bucketDocument.getDataColumnBytes());
        final String columnDataPath = pvTimesSecondsNanosGroup + PATH_SEPARATOR + DATASET_DATA_COLUMN_BYTES;
        writer.writeByteArray(columnDataPath, bucketDocument.getDataColumnBytes());

        // dataTimestampsBytes
        Objects.requireNonNull(bucketDocument.getDataTimestampsBytes());
        final String dataTimestampsPath = pvTimesSecondsNanosGroup + PATH_SEPARATOR + DATASET_DATA_TIMESTAMPS_BYTES;
        writer.writeByteArray(dataTimestampsPath, bucketDocument.getDataTimestampsBytes());

        // attributeMap - write keys to one array and values to another
        Objects.requireNonNull(bucketDocument.getAttributeMap());
        final String attributeMapKeysPath = pvTimesSecondsNanosGroup + PATH_SEPARATOR + DATASET_ATTRIBUTE_MAP_KEYS;
        writer.writeStringArray(attributeMapKeysPath, bucketDocument.getAttributeMap().keySet().toArray(new String[0]));
        final String attributeMapValuesPath = pvTimesSecondsNanosGroup + PATH_SEPARATOR + DATASET_ATTRIBUTE_MAP_VALUES;
        writer.writeStringArray(attributeMapValuesPath, bucketDocument.getAttributeMap().values().toArray(new String[0]));

        // eventMetadata - description, start/stop times
        Objects.requireNonNull(bucketDocument.getEventMetadata());
        final String eventMetadataDescriptionPath = 
                pvTimesSecondsNanosGroup + PATH_SEPARATOR + DATASET_EVENT_METADATA_DESCRIPTION;
        writer.writeString(eventMetadataDescriptionPath, bucketDocument.getEventMetadata().getDescription());
        final String eventMetadataStartSecondsPath = 
                pvTimesSecondsNanosGroup + PATH_SEPARATOR + DATASET_EVENT_METADATA_START_SECONDS;
        writer.writeLong(eventMetadataStartSecondsPath, bucketDocument.getEventMetadata().getStartSeconds());
        final String eventMetadataStartNanosPath = 
                pvTimesSecondsNanosGroup + PATH_SEPARATOR + DATASET_EVENT_METADATA_START_NANOS;
        writer.writeLong(eventMetadataStartNanosPath, bucketDocument.getEventMetadata().getStartNanos());
        final String eventMetadataStopSecondsPath =
                pvTimesSecondsNanosGroup + PATH_SEPARATOR + DATASET_EVENT_METADATA_STOP_SECONDS;
        writer.writeLong(eventMetadataStopSecondsPath, bucketDocument.getEventMetadata().getStopSeconds());
        final String eventMetadataStopNanosPath =
                pvTimesSecondsNanosGroup + PATH_SEPARATOR + DATASET_EVENT_METADATA_STOP_NANOS;
        writer.writeLong(eventMetadataStopNanosPath, bucketDocument.getEventMetadata().getStopNanos());

        // providerId
        Objects.requireNonNull(bucketDocument.getProviderId());
        final String providerIdPath = pvTimesSecondsNanosGroup + PATH_SEPARATOR + DATASET_PROVIDER_ID;
        writer.writeString(providerIdPath, bucketDocument.getProviderId());
        
        // create groups for indexing by time and pv
        final String timesSecondsGroup = GROUP_TIMES + PATH_SEPARATOR + firstSecondsString;
        if (! writer.object().isGroup(timesSecondsGroup)) {
            writer.object().createGroup(timesSecondsGroup);
        }
        final String timesSecondsNanosGroup = timesSecondsGroup + PATH_SEPARATOR + firstNanosString;
        if (! writer.object().isGroup(timesSecondsNanosGroup)) {
            writer.object().createGroup(timesSecondsNanosGroup);
        }
        final String timesSecondsNanosPvsGroup = timesSecondsNanosGroup + PATH_SEPARATOR + GROUP_PVS;
        if (! writer.object().isGroup(timesSecondsNanosPvsGroup)) {
            writer.object().createGroup(timesSecondsNanosPvsGroup);
        }

        // create soft link to bucket document under pvs path from times path
        final String timesSecondsNanosPvsPvPath =
                timesSecondsNanosPvsGroup + PATH_SEPARATOR + bucketDocument.getPvName();
        if (! writer.object().exists(timesSecondsNanosPvsPvPath)) {
            writer.object().createSoftLink(PATH_SEPARATOR + pvTimesSecondsNanosGroup, timesSecondsNanosPvsPvPath);
        }
    }

    public void close() {
        writer.close();
    }
}