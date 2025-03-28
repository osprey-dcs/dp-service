package com.ospreydcs.dp.service.common.utility;

import com.mongodb.client.MongoCursor;
import com.ospreydcs.dp.grpc.v1.common.DataTimestamps;
import com.ospreydcs.dp.grpc.v1.common.DataValue;
import com.ospreydcs.dp.grpc.v1.common.Timestamp;
import com.ospreydcs.dp.service.common.bson.bucket.BucketDocument;
import com.ospreydcs.dp.service.common.exception.DpException;
import com.ospreydcs.dp.service.common.protobuf.DataTimestampsUtility;
import com.ospreydcs.dp.service.common.model.TimestampDataMap;
import com.ospreydcs.dp.service.common.model.TimestampMap;

import java.util.Iterator;
import java.util.Map;
import java.util.TreeMap;

public class TabularDataUtility {

    public static record TimestampDataMapSizeStats(int currentDataSize, boolean sizeLimitExceeded) {}

    public static TimestampDataMapSizeStats updateTimestampMapFromBucketCursor(
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
                    columnIndex, bucket, tableValueMap, beginSeconds, beginNanos, endSeconds, endNanos);
            currentDataSize = currentDataSize + bucketDataSize;
            if (sizeLimit != null && currentDataSize > sizeLimit) {
                cursor.close();
                return new TimestampDataMapSizeStats(currentDataSize, true);
            }
        }
        cursor.close();

        return new TimestampDataMapSizeStats(currentDataSize, false);
    }

    private static int addBucketToTable(
            int columnIndex,
            BucketDocument bucket,
            TimestampMap<Map<Integer, DataValue>> tableValueMap,
            long beginSeconds,
            long beginNanos,
            long endSeconds,
            long endNanos
    ) throws DpException {

        int dataValueSize = 0;
        final DataTimestamps bucketDataTimestamps = bucket.getDataTimestamps().toDataTimestamps();
        final DataTimestampsUtility.DataTimestampsIterator dataTimestampsIterator =
                DataTimestampsUtility.dataTimestampsIterator(bucketDataTimestamps);

        // derserialize DataColumn content from document and the iterate DataValues in column
        final Iterator<DataValue> dataValueIterator = bucket.getDataColumn().toDataColumn().getDataValuesList().iterator();
        while (dataTimestampsIterator.hasNext() && dataValueIterator.hasNext()) {

            final Timestamp timestamp = dataTimestampsIterator.next();
            final long second = timestamp.getEpochSeconds();
            final long nano = timestamp.getNanoseconds();
            final DataValue dataValue = dataValueIterator.next();

            // skip values outside query time range
            if (second < beginSeconds || second > endSeconds) {
                continue;
            } else if ((second == beginSeconds && nano < beginNanos) || (second == endSeconds && nano >= endNanos)) {
                continue;
            }

            // generate DataValue object from column data value
            dataValueSize = dataValueSize + dataValue.getSerializedSize();

            // add to table data structure
            Map<Integer, DataValue> nanoValueMap = tableValueMap.get(second, nano);
            if (nanoValueMap == null) {
                nanoValueMap = new TreeMap<>();
                tableValueMap.put(second, nano, nanoValueMap);
            }
            nanoValueMap.put(columnIndex, dataValue);
        }

        return dataValueSize;
    }

}
