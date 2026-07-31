package com.ospreydcs.dp.service.query.handler.mongo.dispatch;

import com.mongodb.client.MongoCursor;
import com.ospreydcs.dp.grpc.v1.common.*;
import com.ospreydcs.dp.grpc.v1.query.QueryTableRequest;
import com.ospreydcs.dp.grpc.v1.query.QueryTableResponse;
import com.ospreydcs.dp.service.common.bson.bucket.BucketDocument;
import com.ospreydcs.dp.service.common.exception.DpException;
import com.ospreydcs.dp.service.common.handler.Dispatcher;
import com.ospreydcs.dp.service.common.model.TimestampDataMap;
import com.ospreydcs.dp.service.common.utility.TabularDataUtility;
import com.ospreydcs.dp.service.query.handler.mongo.MongoQueryHandler;
import com.ospreydcs.dp.service.query.service.QueryServiceImpl;
import io.grpc.stub.StreamObserver;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.*;

public class QueryTableDispatcher extends Dispatcher {

    // static variables
    private static final Logger logger = LogManager.getLogger();

    // instance variables
    private final StreamObserver<QueryTableResponse> responseObserver;
    private final QueryTableRequest request;

    // constants
    public static final String TABLE_RESULT_TIMESTAMP_COLUMN_NAME = "timestamp";

    public QueryTableDispatcher(
            StreamObserver<QueryTableResponse> responseObserver,
            QueryTableRequest request
    ) {
        this.responseObserver = responseObserver;
        this.request = request;
    }

    private QueryTableResponse.TableResult columnTableResultFromMap(
            List<String> columnNames, TimestampDataMap tableValueMap) {

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

        // Add data values to column builders, draining the map as we go (#199): each row is released
        // once its values are held by the builders, so peak memory is not both representations at
        // full size. tableValueMap is not used after this point.
        //
        // DataRow already sparse-fills missing cells with an unset DataValue, matching the previous
        // inline behavior here.
        final TimestampDataMap.DataRowIterator dataRowIterator = tableValueMap.drainingDataRowIterator();
        while (dataRowIterator.hasNext()) {
            final TimestampDataMap.DataRow dataRow = dataRowIterator.next();
            timestampListBuilder.addTimestamps(Timestamp.newBuilder()
                    .setEpochSeconds(dataRow.seconds())
                    .setNanoseconds(dataRow.nanos())
                    .build());
            final List<DataValue> rowDataValues = dataRow.dataValues();
            for (var columnBuilderMapEntry : columnBuilderMap.entrySet()) {
                final int columnIndex = columnBuilderMapEntry.getKey();
                columnBuilderMapEntry.getValue().addDataValues(rowDataValues.get(columnIndex));
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
            List<String> columnNames, TimestampDataMap tableValueMap) {

        final QueryTableResponse.TableResult.Builder tableResultBuilder = QueryTableResponse.TableResult.newBuilder();
        final QueryTableResponse.RowMapTable.Builder rowMapTableBuilder = QueryTableResponse.RowMapTable.newBuilder();

        final List<String> columnNamesWithTimestamp = new ArrayList<>();
        columnNamesWithTimestamp.add(TABLE_RESULT_TIMESTAMP_COLUMN_NAME);
        columnNamesWithTimestamp.addAll(columnNames);
        rowMapTableBuilder.addAllColumnNames(columnNamesWithTimestamp);

        // Draining iteration (#199): each row is released from the map as it is copied into the
        // response builder, so peak memory is not both representations at full size. tableValueMap
        // is not used after this point.
        final TimestampDataMap.DataRowIterator dataRowIterator = tableValueMap.drainingDataRowIterator();
        while (dataRowIterator.hasNext()) {

            // read next data row
            final TimestampDataMap.DataRow dataRow = dataRowIterator.next();

            // create builder for response data row and map for column values
            final QueryTableResponse.RowMapTable.DataRow.Builder dataRowBuilder =
                    QueryTableResponse.RowMapTable.DataRow.newBuilder();
            final Map<String, DataValue> rowDataValueMap = new TreeMap<>();

            // set value for timestamp column
            final Timestamp timestamp = Timestamp.newBuilder()
                    .setEpochSeconds(dataRow.seconds())
                    .setNanoseconds(dataRow.nanos())
                    .build();
            final DataValue timestampDataValue = DataValue.newBuilder()
                    .setTimestampValue(timestamp)
                    .build();
            rowDataValueMap.put(TABLE_RESULT_TIMESTAMP_COLUMN_NAME, timestampDataValue);

            // add map entry for each data column value
            int columnIndex = 0;
            for (String columnName : columnNames) {
                DataValue columnDataValue = dataRow.dataValues().get(columnIndex);
                rowDataValueMap.put(columnName, columnDataValue);
                columnIndex = columnIndex + 1;
            }

            // add value map to row, add row to result
            dataRowBuilder.putAllColumnValues(rowDataValueMap);
            rowMapTableBuilder.addRows(dataRowBuilder.build());
        }

        tableResultBuilder.setRowMapTable(rowMapTableBuilder.build());
        return tableResultBuilder.build();
    }

    public void handleResult(MongoCursor<BucketDocument> cursor) {

        // send error response and close response stream if cursor is null
        if (cursor == null) {
            final String msg = "executeQuery returned null cursor";
            logger.error(msg + " id: " + this.responseObserver.hashCode());
            QueryServiceImpl.sendQueryTableResponseError(msg, this.responseObserver);
            return;
        }

        // send empty result response if query matched no data
        if (!cursor.hasNext()) {
            logger.trace(
                    "processQueryRequest: query matched no data, cursor is empty id: "
                            + this.responseObserver.hashCode());
            QueryServiceImpl.sendQueryTableResponseEmpty(request.getFormat(), this.responseObserver);
            return;
        }

        // create data structure for creating table
        final TimestampDataMap tableValueMap = new TimestampDataMap();

        // build temporary tabular data structure from cursor
        final long beginSeconds = this.request.getBeginTime().getEpochSeconds();
        final long beginNanos = this.request.getBeginTime().getNanoseconds();
        final long endSeconds = this.request.getEndTime().getEpochSeconds();
        final long endNanos = this.request.getEndTime().getNanoseconds();
        TabularDataUtility.TimestampDataMapSizeStats sizeStats = null;

        try {
            sizeStats = TabularDataUtility.addBucketsToTable(
                    tableValueMap,
                    cursor,
                    0,
                    MongoQueryHandler.getOutgoingMessageSizeLimitBytes(),
                    beginSeconds,
                    beginNanos,
                    endSeconds,
                    endNanos
            );

        } catch (DpException e) {
            final String msg = "exception building tabular result: " + e.getMessage();
            logger.error(msg, e);
            QueryServiceImpl.sendQueryTableResponseError(msg, this.responseObserver);
            return;
        }

        if (sizeStats.sizeLimitExceeded()) {
            final String msg = "result exceeds gRPC message size limit";
            logger.error(msg);
            QueryServiceImpl.sendQueryTableResponseError(msg, this.responseObserver);
            return;
        }

        // create column or row-oriented table result from map as specified in request
        QueryTableResponse.TableResult tableResult = null;
        final List<String> columnNameList = tableValueMap.getColumnNameList();
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
