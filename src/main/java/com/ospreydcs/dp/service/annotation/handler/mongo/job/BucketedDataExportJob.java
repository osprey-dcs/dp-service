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

            // We could enforce an export output file size limit here, as in TabularDataExportJob.exportDataset_(),
            // but at this point I'm not going to do so.  In the case of tabular data, we build a large table data
            // structure before we write the data to file.  But with bucketed data (at least for HDF5 format), we write
            // each individual bucket to the file as we read it from the database cursor.  As HDF5 is designed to handle
            // large amounts of data, and we are considering its use as a long term archive format, I've decided not
            // to add constraints for the file size.  If we wanted to do so, we would probably change the signature of
            // writeBucketData() to include parameters for the previous file size and size limit, and return a structure
            // with the new file size and a flag indicating if the size limit is exceeded.  In terms of
            // the implementation in DatasetExportHdf5File.writeBucketData(), we could use
            // dataColumnBytes.getSerializedSize() to get the size of the protobuf vector of data values written to the
            // file for each bucket.

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
