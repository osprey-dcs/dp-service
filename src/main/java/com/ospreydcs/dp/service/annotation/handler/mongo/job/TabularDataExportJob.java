package com.ospreydcs.dp.service.annotation.handler.mongo.job;

import com.mongodb.client.MongoCursor;
import com.ospreydcs.dp.grpc.v1.common.DataValue;
import com.ospreydcs.dp.service.annotation.handler.model.HandlerExportDataSetRequest;
import com.ospreydcs.dp.service.annotation.handler.mongo.client.MongoAnnotationClientInterface;
import com.ospreydcs.dp.service.annotation.handler.mongo.export.TabularDataExportFileInterface;
import com.ospreydcs.dp.service.common.bson.bucket.BucketDocument;
import com.ospreydcs.dp.service.common.bson.dataset.DataBlockDocument;
import com.ospreydcs.dp.service.common.bson.dataset.DataSetDocument;
import com.ospreydcs.dp.service.common.model.TimestampMap;
import com.ospreydcs.dp.service.common.utility.TabularDataUtility;
import com.ospreydcs.dp.service.query.handler.mongo.client.MongoQueryClientInterface;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public abstract class TabularDataExportJob extends ExportDataSetJob {

    // constants
    private static final String COLUMN_HEADER_SECONDS = "seconds";
    private static final String COLUMN_HEADER_NANOS = "nanos";

    // instance variables
    private TabularDataExportFileInterface exportFile;

    public TabularDataExportJob(
            HandlerExportDataSetRequest handlerRequest,
            MongoAnnotationClientInterface mongoAnnotationClient,
            MongoQueryClientInterface mongoQueryClient
    ) {
        super(handlerRequest, mongoAnnotationClient, mongoQueryClient);
    }

    protected abstract TabularDataExportFileInterface createExportFile_(
            DataSetDocument dataset, String serverFilePath) throws IOException;

    @Override
    protected ExportDatasetStatus exportDataset_(DataSetDocument dataset, String serverFilePath) {

        // create file for export
        try {
            exportFile = createExportFile_(dataset, serverFilePath);
        } catch (IOException e) {
            final String errorMsg = "error writing to export file: " + serverFilePath;
            return new ExportDatasetStatus(true, errorMsg);
        }

        // execute query for each data block in dataset and write data to file
        final TimestampMap<Map<Integer, DataValue>> tableValueMap = new TimestampMap<>(); // temp tabular data structure
        final List<String> columnNameList = new ArrayList<>(); // data structure for getting column index by PV name
        for (DataBlockDocument dataBlock : dataset.getDataBlocks()) {

            final MongoCursor<BucketDocument> cursor =
                    this.mongoQueryClient.executeDataBlockQuery(dataBlock);

            if (cursor == null) {
                final String errorMsg = "unknown error executing data block query";
                return new ExportDatasetStatus(true, errorMsg);
            }

            if (!cursor.hasNext()) {
                final String errorMsg = "data block query returned no data";
                return new ExportDatasetStatus(true, errorMsg);
            }

            // build temporary tabular data structure from cursor
            final long beginSeconds = dataBlock.getBeginTimeSeconds();
            final long beginNanos = dataBlock.getBeginTimeNanos();
            final long endSeconds = dataBlock.getEndTimeSeconds();
            final long endNanos = dataBlock.getEndTimeNanos();
            TabularDataUtility.TimestampMapDataSizeStats sizeStats =
                    TabularDataUtility.updateTimestampMapFromBucketCursor(
                            tableValueMap,
                            columnNameList,
                            cursor,
                            0,
                            null,
                            beginSeconds,
                            beginNanos,
                            endSeconds,
                            endNanos
                    );

//            if (sizeStats.sizeLimitExceeded()) {
//                  // TODO: not sure if we want to add a size limit here yet?
//            }
//            tableDataSize = tableDataSize + sizeStats.currentDataSize();
        }

        // write data to tabular formatted file

        // write column headers
        final List<String> columnHeaders = List.of(COLUMN_HEADER_SECONDS, COLUMN_HEADER_NANOS);
        columnHeaders.addAll(columnNameList);
        exportFile.writeHeaderRow(columnHeaders);

        for (var secondEntry : tableValueMap.entrySet()) {

            final long second = secondEntry.getKey();

            final Map<Long, Map<Integer, DataValue>> secondValueMap = secondEntry.getValue();
            for (var nanoEntry : secondValueMap.entrySet()) {

                final long nano = nanoEntry.getKey();

                // get map of column data values for row (key is column index, row is data value for that column)
                final Map<Integer, DataValue> nanoValueMap = nanoEntry.getValue();

                // create list for row data values
                final List<String> rowDataValues = new ArrayList<>();

                // add values to row for seconds and nanos columns
                rowDataValues.add(String.valueOf(second));
                rowDataValues.add(String.valueOf(nano));

                // add values to row for each column data value
                int columnIndex = 0;
                for (String columnName : columnNameList) {
                    DataValue columnDataValue = nanoValueMap.get(columnIndex);
                    if (columnDataValue == null) {
                        columnDataValue = DataValue.newBuilder().build();
                    }
                    rowDataValues.add(columnDataValue.toString());

                    columnIndex = columnIndex + 1;
                }

                // write data row to file
                exportFile.writeDataRow(rowDataValues);
            }
        }

        exportFile.close();

        return new ExportDatasetStatus(false, "");
    }

}
