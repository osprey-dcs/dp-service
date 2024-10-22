package com.ospreydcs.dp.service.annotation.handler.mongo.job;

import com.mongodb.client.MongoCursor;
import com.ospreydcs.dp.service.annotation.handler.model.HandlerExportDataSetRequest;
import com.ospreydcs.dp.service.annotation.handler.mongo.client.MongoAnnotationClientInterface;
import com.ospreydcs.dp.service.annotation.handler.mongo.export.BucketedDataExportFileInterface;
import com.ospreydcs.dp.service.common.bson.bucket.BucketDocument;
import com.ospreydcs.dp.service.common.bson.dataset.DataBlockDocument;
import com.ospreydcs.dp.service.common.bson.dataset.DataSetDocument;
import com.ospreydcs.dp.service.common.exception.DpException;
import com.ospreydcs.dp.service.query.handler.mongo.client.MongoQueryClientInterface;

import java.io.IOException;

public abstract class BucketedDataExportJob extends ExportDataSetJob {

    // instance variables
    private BucketedDataExportFileInterface exportFile;

    public BucketedDataExportJob(
            HandlerExportDataSetRequest handlerRequest,
            MongoAnnotationClientInterface mongoAnnotationClient,
            MongoQueryClientInterface mongoQueryClient
    ) {
        super(handlerRequest, mongoAnnotationClient, mongoQueryClient);
    }

    protected abstract BucketedDataExportFileInterface createExportFile_(
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

        // execute query for each data block in dataset and write data to hdf5 file
        for (DataBlockDocument dataBlock : dataset.getDataBlocks()) {

            final MongoCursor<BucketDocument> cursor =
                    this.mongoQueryClient.executeDataBlockQuery(dataBlock);

            if (cursor == null) {
                final String errorMsg = "unknown error executing data block query";
                return new ExportDatasetStatus(true, errorMsg);
            }

            while (cursor.hasNext()) {
                final BucketDocument bucketDocument = cursor.next();
                try {
                    exportFile.writeBucketData(bucketDocument);
                } catch (DpException e) {
                    final String errorMsg =
                            "exception writing data to export file " + serverFilePath + ": " + e.getMessage();
                    return new ExportDatasetStatus(true, errorMsg);
                }
            }
        }

        try {
            exportFile.close();
        } catch (DpException e) {
            final String errorMsg = "exception closing export file " + serverFilePath + ": " + e.getMessage();
            return new ExportDatasetStatus(true, errorMsg);
        }

        return new ExportDatasetStatus(false, "");
    }
}
