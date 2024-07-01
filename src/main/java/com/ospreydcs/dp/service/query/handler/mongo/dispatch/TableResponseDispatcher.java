package com.ospreydcs.dp.service.query.handler.mongo.dispatch;

import com.mongodb.client.MongoCursor;
import com.ospreydcs.dp.grpc.v1.common.*;
import com.ospreydcs.dp.grpc.v1.query.QueryTableRequest;
import com.ospreydcs.dp.grpc.v1.query.QueryTableResponse;
import com.ospreydcs.dp.service.common.bson.bucket.BucketDocument;
import com.ospreydcs.dp.service.common.grpc.DataTimestampsUtility;
import com.ospreydcs.dp.service.common.grpc.TimestampUtility;
import com.ospreydcs.dp.service.common.handler.Dispatcher;
import com.ospreydcs.dp.service.common.model.TimestampMap;
import com.ospreydcs.dp.service.query.service.QueryServiceImpl;
import io.grpc.stub.StreamObserver;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.*;

public class TableResponseDispatcher extends Dispatcher {

    // static variables
    private static final Logger logger = LogManager.getLogger();

    // instance variables
    private final StreamObserver<QueryTableResponse> responseObserver;
    private final QueryTableRequest request;

    // constants
    public static final String TABLE_RESULT_TIMESTAMP_COLUMN_NAME = "timestamp";

    public TableResponseDispatcher(
            StreamObserver<QueryTableResponse> responseObserver,
            QueryTableRequest request
    ) {
        this.responseObserver = responseObserver;
        this.request = request;
    }

    private int addBucketToTable(
            int columnIndex, BucketDocument bucket, TimestampMap<Map<Integer, DataValue>> tableValueMap
    ) {
        final long beginSeconds = this.request.getBeginTime().getEpochSeconds();
        final long beginNanos = this.request.getBeginTime().getNanoseconds();
        final long endSeconds = this.request.getEndTime().getEpochSeconds();
        final long endNanos = this.request.getEndTime().getNanoseconds();

        int dataValueSize = 0;
        final DataTimestamps bucketDataTimestamps = bucket.readDataTimestampsContent();
        final DataTimestampsUtility.DataTimestampsIterator dataTimestampsIterator =
                DataTimestampsUtility.dataTimestampsIterator(bucketDataTimestamps);
        final Iterator<DataValue> dataValueIterator = bucket.readDataColumnContent().getDataValuesList().iterator();
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

    private QueryTableResponse.TableResult columnTableResultFromMap(
            List<String> columnNames, TimestampMap<Map<Integer, DataValue>> tableValueMap) {

        // create builders for table and columns, and list of timestamps
        final QueryTableResponse.TableResult.Builder tableResultBuilder =
                QueryTableResponse.TableResult.newBuilder();

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
        final DataTimestamps.Builder dataTimestampsBuilder = DataTimestamps.newBuilder();
        dataTimestampsBuilder.setTimestampList(timestampListBuilder).build();
        QueryTableResponse.ColumnTable.Builder columnTableBuilder = QueryTableResponse.ColumnTable.newBuilder();
        columnTableBuilder.setDataTimestamps(dataTimestampsBuilder);
        for (DataColumn.Builder dataColumnBuilder : columnBuilderMap.values()) {
            dataColumnBuilder.build();
            columnTableBuilder.addDataColumns(dataColumnBuilder);
        }
        tableResultBuilder.setColumnTable(columnTableBuilder.build());
        return tableResultBuilder.build();
    }

    private QueryTableResponse.TableResult rowMapTableResultFromMap(
            List<String> columnNames, TimestampMap<Map<Integer, DataValue>> tableValueMap) {

        final QueryTableResponse.TableResult.Builder tableResultBuilder = QueryTableResponse.TableResult.newBuilder();
        final QueryTableResponse.RowMapTable.Builder rowMapTableBuilder = QueryTableResponse.RowMapTable.newBuilder();

        final List<String> columnNamesWithTimestamp = new ArrayList<>();
        columnNamesWithTimestamp.add(TABLE_RESULT_TIMESTAMP_COLUMN_NAME);
        columnNamesWithTimestamp.addAll(columnNames);
        rowMapTableBuilder.addAllColumnNames(columnNamesWithTimestamp);

        for (var secondEntry : tableValueMap.entrySet()) {

            final long second = secondEntry.getKey();

            final Map<Long, Map<Integer, DataValue>> secondValueMap = secondEntry.getValue();
            for (var nanoEntry : secondValueMap.entrySet()) {

                final long nano = nanoEntry.getKey();

                final Map<Integer, DataValue> nanoValueMap = nanoEntry.getValue();

                // create map for row's data, keys are column names, values are column values
                final QueryTableResponse.RowMapTable.DataRow.Builder dataRowBuilder =
                        QueryTableResponse.RowMapTable.DataRow.newBuilder();
                final Map<String, DataValue> rowDataValueMap = new TreeMap<>();

                // set value for timestamp column
                final Timestamp timestamp = Timestamp.newBuilder().setEpochSeconds(second).setNanoseconds(nano).build();
                final DataValue timestampDataValue = DataValue.newBuilder()
                        .setTimestampValue(timestamp)
                        .build();
                rowDataValueMap.put(TABLE_RESULT_TIMESTAMP_COLUMN_NAME, timestampDataValue);

                // add map entry for each data column value
                int columnIndex = 0;
                for (String columnName : columnNames) {
                    DataValue columnDataValue = nanoValueMap.get(columnIndex);
                    if (columnDataValue == null) {
                        columnDataValue = DataValue.newBuilder().build();
                    }
                    rowDataValueMap.put(columnName, columnDataValue);

                    columnIndex = columnIndex + 1;
                }

                // add value map to row, add row to result
                dataRowBuilder.putAllColumnValues(rowDataValueMap);
                rowMapTableBuilder.addRows(dataRowBuilder.build());
            }
        }

        tableResultBuilder.setRowMapTable(rowMapTableBuilder.build());
        return tableResultBuilder.build();
    }

    public void handleResult(MongoCursor<BucketDocument> cursor) {

        // send error response and close response stream if cursor is null
        if (cursor == null) {
            final String msg = "executeQuery returned null cursor";
            logger.error(msg);
            QueryServiceImpl.sendQueryTableResponseError(msg, this.responseObserver);
            return;
        }

        // send empty QueryStatus and close response stream if query matched no data
        if (!cursor.hasNext()) {
            logger.trace("processQueryRequest: query matched no data, cursor is empty");
            QueryServiceImpl.sendQueryTableResponseEmpty(this.responseObserver);
            return;
        }

        // create data structure for creating table
        final TimestampMap<Map<Integer, DataValue>> tableValueMap = new TimestampMap<>();

        // data structure for getting column index
        final List<String> columnNameList = new ArrayList<>();

        int responseMessageSize = 0;
        while (cursor.hasNext()) {
            // add buckets to table data structure
            final BucketDocument bucket = cursor.next();
            int columnIndex = columnNameList.indexOf(bucket.getPvName());
            if (columnIndex == -1) {
                // add column to list and get index
                columnNameList.add(bucket.getPvName());
                columnIndex = columnNameList.size() - 1;
            }
            int bucketDataSize = addBucketToTable(columnIndex, bucket, tableValueMap);
            responseMessageSize = responseMessageSize + bucketDataSize;
            if (responseMessageSize > TimestampUtility.MAX_GRPC_MESSAGE_SIZE) {
                final String msg = "result exceeds gRPC message size limit";
                logger.error(msg);
                QueryServiceImpl.sendQueryTableResponseError(msg, this.responseObserver);
                return;
            }
        }
        cursor.close();

        // create column or row-oriented table result from map as specified in request
        QueryTableResponse.TableResult tableResult = null;
        switch (request.getFormat()) {

            case TABLE_FORMAT_COLUMN -> {
                tableResult = columnTableResultFromMap(columnNameList, tableValueMap);
            }
            case TABLE_FORMAT_ROW_MAP -> {
                tableResult = rowMapTableResultFromMap(columnNameList, tableValueMap);
            }
            case UNRECOGNIZED -> {
                QueryServiceImpl.sendQueryTableResponseError(
                        "QueryTableRequest.format must be specified", this.responseObserver);
                return;
            }
        }

        // create and send response, close response stream
        QueryTableResponse response = QueryServiceImpl.queryTableResponse(tableResult);
        responseObserver.onNext(response);
        responseObserver.onCompleted();
    }
}
