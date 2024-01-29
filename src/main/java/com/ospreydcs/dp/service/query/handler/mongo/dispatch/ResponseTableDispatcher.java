package com.ospreydcs.dp.service.query.handler.mongo.dispatch;

import com.mongodb.client.MongoCursor;
import com.ospreydcs.dp.grpc.v1.common.*;
import com.ospreydcs.dp.grpc.v1.query.QueryResponse;
import com.ospreydcs.dp.service.common.bson.BucketDocument;
import com.ospreydcs.dp.service.query.service.QueryServiceImpl;
import io.grpc.stub.StreamObserver;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

public class ResponseTableDispatcher extends BucketCursorResponseDispatcher {

    // static variables
    private static final Logger logger = LogManager.getLogger();

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

    private static DataTable dataTableFromMap(List<String> columnNames, Map<Long, Map<Long, Map<Integer, DataValue>>> tableValueMap) {

        // create builders for table and columns, and list of timestamps
        final DataTable.Builder dataTableBuilder = DataTable.newBuilder();
        final Map<Integer, DataColumn.Builder> columnBuilderMap = new TreeMap<>();
        for (int i = 0 ; i < columnNames.size() ; ++i) {
            final DataColumn.Builder dataColumnBuilder = DataColumn.newBuilder();
            dataColumnBuilder.setName(columnNames.get(i));
            columnBuilderMap.put(i, dataColumnBuilder);
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
                    if (columnDataValue == null) {
                        columnDataValue = DataValue.newBuilder().build();
                    }
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

        // data structure for getting column index
        final List<String> columnNameList = new ArrayList<>();

        int messageSize = 0;
        while (cursor.hasNext()) {
            // add buckets to table data structure
            final BucketDocument bucket = cursor.next();
            int columnIndex = columnNameList.indexOf(bucket.getColumnName());
            if (columnIndex == -1) {
                // add column to list and get index
                columnNameList.add(bucket.getColumnName());
                columnIndex = columnNameList.size() - 1;
            }
            addBucketToTable(columnIndex, bucket, tableValueMap);
        }
        cursor.close();

        // create DataTable for response from temporary data structure
        final DataTable dataTable = dataTableFromMap(columnNameList, tableValueMap);

        // create and send response, close response stream
        QueryResponse response = QueryServiceImpl.queryResponseWithTable(dataTable);
        getResponseObserver().onNext(response);
        getResponseObserver().onCompleted();
    }
}
