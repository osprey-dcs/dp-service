package com.ospreydcs.dp.service.common.utility;

import com.mongodb.client.MongoCursor;
import com.ospreydcs.dp.grpc.v1.common.*;
import com.ospreydcs.dp.service.common.bson.column.ColumnDocumentBase;
import com.ospreydcs.dp.service.common.bson.column.DataColumnDocument;
import com.ospreydcs.dp.service.common.bson.column.ScalarColumnDocumentBase;
import com.ospreydcs.dp.service.common.bson.bucket.BucketDocument;
import com.ospreydcs.dp.service.common.bson.calculations.CalculationsDataFrameDocument;
import com.ospreydcs.dp.service.common.bson.calculations.CalculationsDocument;
import com.ospreydcs.dp.service.common.exception.DpException;
import com.ospreydcs.dp.service.common.exception.NonScalarColumnException;
import com.ospreydcs.dp.service.common.protobuf.DataTimestampsUtility;
import com.ospreydcs.dp.service.common.model.TimestampDataMap;

import java.time.Instant;
import java.util.*;

public class TabularDataUtility {

    public static record TimestampDataMapSizeStats(int currentDataSize, boolean sizeLimitExceeded) {}

    public static TimestampDataMapSizeStats addBucketsToTable(
            TimestampDataMap tableValueMap,
            MongoCursor<BucketDocument> cursor,
            int previousDataSize,
            Integer sizeLimit, // if null, no limit is applied
            long beginSeconds,
            long beginNanos,
            long endSeconds,
            long endNanos
    ) throws DpException {

        int currentDataSize = previousDataSize;
        while (cursor.hasNext()) {
            // add buckets to table data structure
            final BucketDocument bucket = cursor.next();
            int columnIndex = tableValueMap.getColumnIndex(bucket.getPvName());
            int bucketDataSize = addBucketToTable(
                    bucket, tableValueMap, beginSeconds, beginNanos, endSeconds, endNanos);
            currentDataSize = currentDataSize + bucketDataSize;
            // Size accounting is per-BUCKET, not per-timestamp: a whole bucket is added to the table
            // before the limit is checked, so currentDataSize can overshoot sizeLimit by up to one
            // bucket's worth of data. This is intentional and benign for the callers that pass a
            // sizeLimit as a HARD ceiling (queryTable and the export job both discard the result and
            // return an error when sizeLimitExceeded() is true — overshooting by one bucket vs. one
            // byte yields the same error outcome). The V2 querySamples unary dispatcher instead uses
            // the flag as a PAGING boundary (drain-then-truncate) and owns the zero-progress guard for
            // the case where the overshoot collapses to a single indivisible timestamp. Do NOT tighten
            // this to per-timestamp granularity without revisiting those callers — for the hard-ceiling
            // callers it would change nothing observable, and V2's boundary handling already accounts
            // for the overshoot.
            if (sizeLimit != null && currentDataSize > sizeLimit) {
                cursor.close();
                return new TimestampDataMapSizeStats(currentDataSize, true);
            }
        }
        cursor.close();

        return new TimestampDataMapSizeStats(currentDataSize, false);
    }

    private static int addBucketToTable(
            BucketDocument bucket,
            TimestampDataMap tableValueMap,
            long beginSeconds,
            long beginNanos,
            long endSeconds,
            long endNanos
    ) throws DpException {

        final DataTimestamps bucketDataTimestamps = bucket.getDataTimestamps().toDataTimestamps();

        // Only scalar column types and DataColumnDocument can be converted to DataColumn for tabular export
        final ColumnDocumentBase columnDocument = bucket.getDataColumn();
        final DataColumn bucketColumn;
        
        if (columnDocument instanceof ScalarColumnDocumentBase) {
            // Scalar columns can be converted to legacy DataColumn format
            bucketColumn = ((ScalarColumnDocumentBase<?>) columnDocument).toDataColumn();
        } else if (columnDocument instanceof DataColumnDocument) {
            // Legacy DataColumn documents can be converted directly
            bucketColumn = ((DataColumnDocument) columnDocument).toDataColumn();
        } else {
            // Non-scalar columns (arrays, images, structs) have no tabular (row/column) representation.
            // Throw a neutral, PV-named exception; each caller phrases its own guidance (the export
            // framework and querySamples both catch this and translate for their context, Q4).
            throw new NonScalarColumnException(bucket.getPvName(), columnDocument.getClass().getSimpleName());
        }

        return addColumnsToTable(
                bucketDataTimestamps,
                List.of(bucketColumn),
                tableValueMap,
                beginSeconds,
                beginNanos,
                endSeconds,
                endNanos);
    }

    private static int addColumnsToTable(
            DataTimestamps dataTimestamps,
            List<DataColumn> dataColumns,
            TimestampDataMap tableValueMap,
            long beginSeconds,
            long beginNanos,
            long endSeconds,
            long endNanos
    ) throws DpException {

        int dataValueSize = 0;
        final DataTimestampsUtility.DataTimestampsIterator dataTimestampsIterator =
                DataTimestampsUtility.dataTimestampsIterator(dataTimestamps);


        // derserialize DataColumn content from document and the iterate DataValues in column
        int valueIndex = 0;
        while (dataTimestampsIterator.hasNext()) {

            final Timestamp timestamp = dataTimestampsIterator.next();
            final long second = timestamp.getEpochSeconds();
            final long nano = timestamp.getNanoseconds();

            // add next value for each column to tableValueMap
            for (DataColumn dataColumn : dataColumns) {
                final DataValue dataValue = dataColumn.getDataValues(valueIndex);
                final int columnIndex = tableValueMap.getColumnIndex(dataColumn.getName());

                // skip values outside query time range
                if (second < beginSeconds || second > endSeconds) {
                    continue;
                } else if ((second == beginSeconds && nano < beginNanos) || (second == endSeconds && nano >= endNanos)) {
                    continue;
                }

                // keep track of data size
                dataValueSize = dataValueSize + dataValue.getSerializedSize();

                // add value to tableValueMap
                Map<Integer, DataValue> nanoValueMap = tableValueMap.get(second, nano);
                if (nanoValueMap == null) {
                    nanoValueMap = new TreeMap<>();
                    tableValueMap.put(second, nano, nanoValueMap);
                }
                nanoValueMap.put(columnIndex, dataValue);
            }

            valueIndex = valueIndex + 1;
        }

        return dataValueSize;
    }

    public static TimestampDataMapSizeStats addCalculationsToTable(
            TimestampDataMap tableValueMap,
            CalculationsDocument calculationsDocument,
            Map<String, CalculationsSpec.ColumnNameList> frameColumnNamesMap,
            Instant exportBeginInstant,
            Instant exportEndInstant,
            int previousDataSize,
            Integer sizeLimit // if null, no limit is applied
    ) throws DpException {

        int currentDataSize = previousDataSize;

        // add columns for each CalculationsDataFrame to table
        for (CalculationsDataFrameDocument frameDocument : calculationsDocument.getDataFrames()) {

            final String frameName = frameDocument.getName();

            // create a model for accessing frame's begin/end times
            final DataTimestamps frameDataTimestamps = frameDocument.getDataTimestamps().toDataTimestamps();
            final DataTimestampsUtility.DataTimestampsModel frameTimestampsModel =
                    new DataTimestampsUtility.DataTimestampsModel(frameDataTimestamps);

            // Determine time range for truncating values.
            // We include all values in the frame if exportBeginInstant and exportEndInstant are not specified,
            // otherwise we truncate values outside that range (e.g., if we are exporting a dataset with calculations).
            long beginSeconds;
            long beginNanos;
            long endSeconds;
            long endNanos;
            if (exportBeginInstant == null || exportEndInstant == null) {
                beginSeconds = frameTimestampsModel.getFirstTimestamp().getEpochSeconds();
                beginNanos = frameTimestampsModel.getFirstTimestamp().getNanoseconds();
                endSeconds = frameTimestampsModel.getLastTimestamp().getEpochSeconds();
                endNanos = frameTimestampsModel.getLastTimestamp().getNanoseconds() + 1; // we add one so last value is not truncated
            } else {
                beginSeconds = exportBeginInstant.getEpochSecond();
                beginNanos = exportBeginInstant.getNano();
                endSeconds = exportEndInstant.getEpochSecond();
                endNanos = exportEndInstant.getNano();
            }

            // make list of columns for frame
            List<DataColumn> frameColumns = new ArrayList<>();
            for (DataColumnDocument frameColumnDocument : frameDocument.getDataColumns()) {
                if (frameColumnNamesMap != null) {
                    // only include columns specified in map if one is provided
                    final CalculationsSpec.ColumnNameList frameColumnNamesList = frameColumnNamesMap.get(frameName);
                    if (frameColumnNamesList != null &&
                            frameColumnNamesList.getColumnNamesList().contains(frameColumnDocument.getName())) {
                        frameColumns.add(frameColumnDocument.toDataColumn());
                    }
                } else {
                    frameColumns.add(frameColumnDocument.toDataColumn());
                }
            }

            // add list of columns to tableValueMap
            int frameDataSize = addColumnsToTable(
                    frameDataTimestamps, frameColumns, tableValueMap, beginSeconds, beginNanos, endSeconds, endNanos);

            // update and check export data size against limit
            currentDataSize = currentDataSize + frameDataSize;
            if (sizeLimit != null && currentDataSize > sizeLimit) {
                return new TimestampDataMapSizeStats(currentDataSize, true);
            }
        }

        return new TimestampDataMapSizeStats(currentDataSize, false);
    }

}
