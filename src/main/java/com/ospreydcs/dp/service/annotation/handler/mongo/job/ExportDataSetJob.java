package com.ospreydcs.dp.service.annotation.handler.mongo.job;

import com.ospreydcs.dp.service.annotation.handler.model.ExportConfiguration;
import com.ospreydcs.dp.service.annotation.handler.model.HandlerExportDataSetRequest;
import com.ospreydcs.dp.service.annotation.handler.mongo.client.MongoAnnotationClientInterface;
import com.ospreydcs.dp.service.annotation.handler.mongo.dispatch.ExportDataSetDispatcher;
import com.ospreydcs.dp.service.common.bson.dataset.DataSetDocument;
import com.ospreydcs.dp.service.common.handler.HandlerJob;
import com.ospreydcs.dp.service.query.handler.mongo.client.MongoQueryClientInterface;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

public abstract class ExportDataSetJob extends HandlerJob {

    protected static record ExportDatasetStatus (boolean isError, String errorMessage) {}

    // static variables
    protected static final Logger logger = LogManager.getLogger();

    // instance variables
    private final HandlerExportDataSetRequest handlerRequest;
    private final ExportDataSetDispatcher dispatcher;
    private final MongoAnnotationClientInterface mongoAnnotationClient;
    protected final MongoQueryClientInterface mongoQueryClient;
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

    protected abstract String getFileExtension_();
    protected abstract ExportDatasetStatus exportDataset_(DataSetDocument dataset, String serverFilePath);

    @Override
    public void execute() {

        logger.debug(
                "executing {} id: {}",
                this.getClass().getSimpleName(),
                this.handlerRequest.responseObserver.hashCode());

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
                exportConfiguration.getExportFilePaths(datasetId, getFileExtension_());
        if (! exportFilePaths.valid) {
            final String errorMsg =
                    "Export mechanism is not properly configured (e.g., see resources/application.yml file)";
            this.dispatcher.handleError(errorMsg);
            return;
        }
        final String serverDirectoryPathString = exportFilePaths.serverDirectoryPath;
        final String filename = exportFilePaths.filename;

        // create directories in server file path
        Path serverDirectoryPath = Paths.get(serverDirectoryPathString);
        try {
            Files.createDirectories(serverDirectoryPath);
        } catch (IOException e) {
            final String errorMsg =
                    "IOException creating directories in path " + serverDirectoryPathString + ": " + e.getMessage();
            logger.error(errorMsg);
            this.dispatcher.handleError(errorMsg);
            return;
        }

        // export data to file
        ExportDatasetStatus status = exportDataset_(dataset, serverDirectoryPathString + filename);
        if (status.isError) {
            logger.error(status.errorMessage);
            this.dispatcher.handleError(status.errorMessage);
            return;
        }

        logger.debug(
                "dispatching {} id: {}",
                this.getClass().getSimpleName(),
                this.handlerRequest.responseObserver.hashCode());
        dispatcher.handleResult(exportFilePaths);
    }

}
