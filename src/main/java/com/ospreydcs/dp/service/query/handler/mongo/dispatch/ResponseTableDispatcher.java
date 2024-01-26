package com.ospreydcs.dp.service.query.handler.mongo.dispatch;

import com.mongodb.client.MongoCursor;
import com.ospreydcs.dp.grpc.v1.common.*;
import com.ospreydcs.dp.grpc.v1.query.QueryResponse;
import com.ospreydcs.dp.service.common.bson.BucketDocument;
import com.ospreydcs.dp.service.query.service.QueryServiceImpl;
import io.grpc.stub.StreamObserver;

import java.util.List;
import java.util.Map;
import java.util.TreeMap;

public class ResponseTableDispatcher extends BucketCursorResponseDispatcher {

    public ResponseTableDispatcher(StreamObserver<QueryResponse> responseObserver) {
        super(responseObserver);
    }

    private static <T> void addBucketToTable(
            int columnIndex, BucketDocument<T> bucket, Map<Long, Map<Long, Map<Integer, DataValue>>> tableValueMap) {
        long second = bucket.getFirstSeconds();
        long nano = bucket.getFirstNanos();
        final long delta = bucket.getSampleFrequency();
        for (T value : bucket.getColumnDataList()) {

            // generate DataValue object from column data value
            final DataValue.Builder valueBuilder = DataValue.newBuilder();
            bucket.addColumnDataValue(value, valueBuilder);
            final DataValue dataValue = valueBuilder.build();

            // add to table data structure
            if (!tableValueMap.containsKey(second)) {
                tableValueMap.put(second, new TreeMap<>());
            }
            final Map<Long, Map<Integer, DataValue>> secondValueMap = tableValueMap.get(second);
            if (!secondValueMap.containsKey(nano)) {
                secondValueMap.put(nano, new TreeMap<>());
            }
            final Map<Integer, DataValue> nanoValueMap = secondValueMap.get(nano);
            nanoValueMap.put(columnIndex, dataValue);

            // increment nanos, and increment seconds if nanos rolled over one billion
            nano = nano + delta;
            if (nano >= 1_000_000_000) {
                second = second + 1;
                nano = nano - 1_000_000_000;
            }
        }
    }

    private static DataTable dataTableFromMap(int numColumns, Map<Long, Map<Long, Map<Integer, DataValue>>> tableValueMap) {

        // create builders for table and columns, and list of timestamps
        final DataTable.Builder dataTableBuilder = DataTable.newBuilder();
        final Map<Integer, DataColumn.Builder> columnBuilderMap = new TreeMap<>();
        for (int i = 1 ; i <= numColumns ; ++i) {
            columnBuilderMap.put(i, DataColumn.newBuilder());
        }
        final TimestampList.Builder timestampListBuilder = TimestampList.newBuilder();

        // add data values to column builders
        for (var secondEntry : tableValueMap.entrySet()) {
            final long second = secondEntry.getKey();
            final Map<Long, Map<Integer, DataValue>> secondValueMap = secondEntry.getValue();
            for (var nanoEntry : secondValueMap.entrySet()) {
                final long nano = nanoEntry.getKey();
                final Map<Integer, DataValue> nanoValueMap = nanoEntry.getValue();
                final Timestamp timestamp = Timestamp.newBuilder().setEpochSeconds(second).setNanoseconds(nano).build();
                timestampListBuilder.addTimestamps(timestamp);
                for (var columnBuilderMapEntry : columnBuilderMap.entrySet()) {
                    final int columnIndex = columnBuilderMapEntry.getKey();
                    final DataColumn.Builder dataColumnBuilder = columnBuilderMapEntry.getValue();
                    DataValue columnDataValue = nanoValueMap.get(columnIndex);
                    dataColumnBuilder.addDataValues(columnDataValue);
                }
            }
        }

        // build timestamp list, columns, and table
        timestampListBuilder.build();
        DataTimeSpec.Builder dataTimeSpecBuilder = DataTimeSpec.newBuilder();
        dataTimeSpecBuilder.setTimestampList(timestampListBuilder).build();
        dataTableBuilder.setDataTimeSpec(dataTimeSpecBuilder);
        for (DataColumn.Builder dataColumnBuilder : columnBuilderMap.values()) {
            dataColumnBuilder.build();
            dataTableBuilder.addDataColumns(dataColumnBuilder);
        }
        return dataTableBuilder.build();
    }

    public void handleResult_(MongoCursor<BucketDocument> cursor) {

        // we have a non-empty cursor in call from handleResult()

        // create data structure for creating table
        final Map<Long, Map<Long, Map<Integer, DataValue>>> tableValueMap = new TreeMap<>();

        int messageSize = 0;
        int columnIndex = 0;
        while (cursor.hasNext()) {
            // add buckets to table data structure
            columnIndex = columnIndex + 1;
            final BucketDocument bucket = cursor.next();
            addBucketToTable(columnIndex, bucket, tableValueMap);
        }
        cursor.close();

        // create DataTable for response from temporary data structure
        final DataTable dataTable = dataTableFromMap(columnIndex, tableValueMap);

        // create and send response, close response stream
        QueryResponse response = QueryServiceImpl.queryResponseWithTable(dataTable);
        getResponseObserver().onNext(response);
        getResponseObserver().onCompleted();
    }
}
