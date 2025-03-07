package com.ospreydcs.dp.service.annotation.handler.mongo.job;

import com.mongodb.client.MongoCursor;
import com.ospreydcs.dp.service.annotation.handler.model.ExportConfiguration;
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

import java.util.ArrayList;
import java.util.List;

public abstract class ExportDataSetJobAbstractTabular extends ExportDataSetJobBase {

    // constants
    public static final String COLUMN_HEADER_SECONDS = "seconds";
    public static final String COLUMN_HEADER_NANOS = "nanos";

    // instance variables
    private TabularDataExportFileInterface exportFile;

    public ExportDataSetJobAbstractTabular(
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
        int tableDataSize = 0;
        for (DataBlockDocument dataBlock : dataset.getDataBlocks()) {

            final MongoCursor<BucketDocument> cursor =
                    this.mongoQueryClient.executeDataBlockQuery(dataBlock);

            if (cursor == null) {
                final String errorMsg = "unknown error executing data block query for export file: " + serverFilePath;
                logger.error(errorMsg);
                return new ExportDatasetStatus(true, errorMsg);
            }

            if (!cursor.hasNext()) {
                final String errorMsg = "data block query returned no data";
                return new ExportDatasetStatus(true, errorMsg);
            }

            // build temporary tabular data structure from cursor
            final long beginSeconds = dataBlock.getBeginTime().getSeconds();
            final long beginNanos = dataBlock.getBeginTime().getNanos();
            final long endSeconds = dataBlock.getEndTime().getSeconds();
            final long endNanos = dataBlock.getEndTime().getNanos();
            TabularDataUtility.TimestampDataMapSizeStats sizeStats = null;
            try {
                sizeStats = TabularDataUtility.updateTimestampMapFromBucketCursor(
                        tableValueMap,
                        cursor,
                        tableDataSize,
                        ExportConfiguration.getExportFileSizeLimitBytes(),
                        beginSeconds,
                        beginNanos,
                        endSeconds,
                        endNanos
                );
            } catch (DpException e) {
                final String errorMsg = "exception deserializing BucketDocument fields: " + e.getMessage();
                logger.error(errorMsg);
                return new ExportDatasetStatus(true, errorMsg);
            }

            // check if export output file size limit exceeded
            if (sizeStats.sizeLimitExceeded()) {
                final String errorMsg = "export file size limit "
                        + ExportConfiguration.getExportFileSizeLimitBytes()
                        + " exceeded for: " + serverFilePath;
                return new ExportDatasetStatus(true, errorMsg);
            }

            tableDataSize = tableDataSize + sizeStats.currentDataSize();
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
            logger.error(errorMsg);
            return new ExportDatasetStatus(true, errorMsg);
        }

        // write data
        try {
            exportFile.writeData(tableValueMap);
        } catch (DpException e) {
            final String errorMsg = "exception writing data to export file " + serverFilePath + ": " + e.getMessage();
            logger.error(errorMsg);
            return new ExportDatasetStatus(true, errorMsg);
        }

        // close file
        try {
            exportFile.close();
        } catch (DpException e) {
            final String errorMsg = "exception closing export file " + serverFilePath + ": " + e.getMessage();
            logger.error(errorMsg);
            return new ExportDatasetStatus(true, errorMsg);
        }

        return new ExportDatasetStatus(false, "");
    }

}
