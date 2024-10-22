package com.ospreydcs.dp.service.annotation.handler.mongo.job;

import com.mongodb.client.MongoCursor;
import com.ospreydcs.dp.service.annotation.handler.model.HandlerExportDataSetRequest;
import com.ospreydcs.dp.service.annotation.handler.mongo.client.MongoAnnotationClientInterface;
import com.ospreydcs.dp.service.annotation.handler.mongo.export.TabularDataExportFileInterface;
import com.ospreydcs.dp.service.common.bson.bucket.BucketDocument;
import com.ospreydcs.dp.service.common.bson.dataset.DataBlockDocument;
import com.ospreydcs.dp.service.common.bson.dataset.DataSetDocument;
import com.ospreydcs.dp.service.common.exception.DpException;
import com.ospreydcs.dp.service.common.model.TimestampDataMap;
import com.ospreydcs.dp.service.common.utility.TabularDataUtility;
import com.ospreydcs.dp.service.query.handler.mongo.client.MongoQueryClientInterface;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public abstract class TabularDataExportJob extends ExportDataSetJob {

    // constants
    public static final String COLUMN_HEADER_SECONDS = "seconds";
    public static final String COLUMN_HEADER_NANOS = "nanos";

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
            DataSetDocument dataset, String serverFilePath) throws DpException;

    @Override
    protected ExportDatasetStatus exportDataset_(DataSetDocument dataset, String serverFilePath) {

        // create file for export
        try {
            exportFile = createExportFile_(dataset, serverFilePath);
        } catch (DpException e) {
            final String errorMsg = "exception opening export file " + serverFilePath + ": " + e.getMessage();
            return new ExportDatasetStatus(true, errorMsg);
        }

        // execute query for each data block in dataset and write data to file
        final TimestampDataMap tableValueMap = new TimestampDataMap();
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
            TabularDataUtility.TimestampDataMapSizeStats sizeStats =
                    TabularDataUtility.updateTimestampMapFromBucketCursor(
                            tableValueMap,
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
        final List<String> columnHeaders = new ArrayList<>();
        columnHeaders.add(COLUMN_HEADER_SECONDS);
        columnHeaders.add(COLUMN_HEADER_NANOS);
        columnHeaders.addAll(tableValueMap.getColumnNameList());
        try {
            exportFile.writeHeaderRow(columnHeaders);
        } catch (DpException e) {
            final String errorMsg = "exception writing header to export file " + serverFilePath + ": " + e.getMessage();
            return new ExportDatasetStatus(true, errorMsg);
        }

        // write data
        try {
            exportFile.writeData(tableValueMap);
        } catch (DpException e) {
            final String errorMsg = "exception writing data to export file " + serverFilePath + ": " + e.getMessage();
            return new ExportDatasetStatus(true, errorMsg);
        }

        // close file
        try {
            exportFile.close();
        } catch (DpException e) {
            final String errorMsg = "exception closing export file " + serverFilePath + ": " + e.getMessage();
            return new ExportDatasetStatus(true, errorMsg);
        }

        return new ExportDatasetStatus(false, "");
    }

}
