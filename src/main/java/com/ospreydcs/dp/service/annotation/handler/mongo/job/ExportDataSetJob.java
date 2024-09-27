package com.ospreydcs.dp.service.annotation.handler.mongo.job;

import ch.systemsx.cisd.hdf5.HDF5Factory;
import ch.systemsx.cisd.hdf5.IHDF5Writer;
import com.mongodb.client.MongoCursor;
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

    public ExportDataSetJob(
            HandlerExportDataSetRequest handlerRequest,
            MongoAnnotationClientInterface mongoAnnotationClient,
            MongoQueryClientInterface mongoQueryClient
    ) {
        this.handlerRequest = handlerRequest;
        this.mongoAnnotationClient = mongoAnnotationClient;
        this.mongoQueryClient = mongoQueryClient;
        this.dispatcher = new ExportDataSetDispatcher(handlerRequest, mongoAnnotationClient);
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

        // TODO: generate path (url?) for exported file
        final String exportFileString = "/tmp/dataset-export.h5";

        // create hdf5 file for export
        DatasetExportHdf5File exportFile = null;
        try {
            exportFile = new DatasetExportHdf5File(exportFileString);
        } catch (IOException e) {
            final String errorMsg = "error writing to export file: " + exportFileString;
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
