package com.ospreydcs.dp.service.annotation.handler.mongo.job;

import com.mongodb.client.MongoCursor;
import com.ospreydcs.dp.service.annotation.handler.model.ExportConfiguration;
import com.ospreydcs.dp.service.annotation.handler.model.HandlerExportDataSetRequest;
import com.ospreydcs.dp.service.annotation.handler.mongo.client.MongoAnnotationClientInterface;
import com.ospreydcs.dp.service.annotation.handler.mongo.dispatch.ExportDataSetDispatcher;
import com.ospreydcs.dp.service.annotation.utility.DatasetExportHdf5File;
import com.ospreydcs.dp.service.common.bson.bucket.BucketDocument;
import com.ospreydcs.dp.service.common.bson.dataset.DataBlockDocument;
import com.ospreydcs.dp.service.common.bson.dataset.DataSetDocument;
import com.ospreydcs.dp.service.common.handler.HandlerJob;
import com.ospreydcs.dp.service.query.handler.mongo.client.MongoQueryClientInterface;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;

public class ExportDataSetJob extends HandlerJob {

    // static variables
    private static final Logger logger = LogManager.getLogger();

    // instance variables
    private final HandlerExportDataSetRequest handlerRequest;
    private final ExportDataSetDispatcher dispatcher;
    private final MongoAnnotationClientInterface mongoAnnotationClient;
    private final MongoQueryClientInterface mongoQueryClient;
    private final ExportConfiguration exportConfiguration;

    public ExportDataSetJob(
            HandlerExportDataSetRequest handlerRequest,
            MongoAnnotationClientInterface mongoAnnotationClient,
            MongoQueryClientInterface mongoQueryClient
    ) {
        this.handlerRequest = handlerRequest;
        this.mongoAnnotationClient = mongoAnnotationClient;
        this.mongoQueryClient = mongoQueryClient;
        this.dispatcher = new ExportDataSetDispatcher(handlerRequest, mongoAnnotationClient);
        this.exportConfiguration = new ExportConfiguration();
    }

    @Override
    public void execute() {

        logger.debug("executing ExportDataSetJob id: {}", this.handlerRequest.responseObserver.hashCode());

        // get dataset for id specified in request
        String datasetId = this.handlerRequest.exportDataSetRequest.getDataSetId();
        DataSetDocument dataset = mongoAnnotationClient.findDataSet(datasetId);
        if (dataset == null) {
            final String errorMsg = "Dataset with id " + datasetId + " not found";
            this.dispatcher.handleError(errorMsg);
            return;
        }

        // generate server output file path for export
        final ExportConfiguration.ExportFilePaths exportFilePaths =
                exportConfiguration.getExportFilePaths(datasetId, ExportConfiguration.FILE_EXTENSION_HDF5);
        if (! exportFilePaths.valid) {
            final String errorMsg =
                    "Export mechanism is not properly configured (e.g., see resources/application.yml file)";
            this.dispatcher.handleError(errorMsg);
            return;
        }

        // create hdf5 file for export
        final String serverFilePath = exportFilePaths.serverFilePath;
        DatasetExportHdf5File exportFile = null;
        try {
            exportFile = new DatasetExportHdf5File(dataset, serverFilePath);
        } catch (IOException e) {
            final String errorMsg = "error writing to export file: " + serverFilePath;
            logger.error(errorMsg);
            this.dispatcher.handleError(errorMsg);
            return;
        }

        // execute query for each data block in dataset and write data to hdf5 file
        for (DataBlockDocument dataBlock : dataset.getDataBlocks()) {

            final MongoCursor<BucketDocument> cursor =
                    this.mongoQueryClient.executeDataBlockQuery(dataBlock);

            if (cursor == null) {
                final String errorMsg = "unknown error executing data block query";
                logger.error(errorMsg);
                this.dispatcher.handleError(errorMsg);
                return;
            }

            while (cursor.hasNext()) {
                final BucketDocument bucketDocument = cursor.next();
                exportFile.writeBucketData(bucketDocument);
            }
        }

        exportFile.close();
//
//        logger.debug("dispatching QueryAnnotationsJob id: {}", this.responseObserver.hashCode());
//        dispatcher.handleResult(cursor);
    }

}
