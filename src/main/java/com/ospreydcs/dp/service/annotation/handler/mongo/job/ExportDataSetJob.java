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
import java.nio.file.*;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.concurrent.TimeUnit;

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
        final Path serverDirectoryPath = Paths.get(serverDirectoryPathString);
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
        final String serverFilePathString = serverDirectoryPathString + filename;
        final Path serverFilePath = Paths.get(serverFilePathString);
        final ExportDatasetStatus status = exportDataset_(dataset, serverFilePathString);
        if (status.isError) {
            logger.error(status.errorMessage);
            this.dispatcher.handleError(status.errorMessage);
            return;
        }

        // check that file is readable before sending API response
        // even though Java calls like write() and close() are synchronous, the OS handling is async
        // we encountered race conditions if not checking that file writing completes before sending api response
        // TODO: enable this
//        try {
//            final BasicFileAttributes exportFileAttributes = awaitFile(serverFilePath, 60*1000 /* 60 seconds */);
//        } catch (IOException | InterruptedException e) {
//            final String errorMsg = "exception waiting for export file " + serverFilePathString + ": " + e.getMessage();
//            logger.error(errorMsg);
//            this.dispatcher.handleError(errorMsg);
//            return;
//        }

        logger.debug(
                "dispatching {} id: {}",
                this.getClass().getSimpleName(),
                this.handlerRequest.responseObserver.hashCode());
        dispatcher.handleResult(exportFilePaths);
    }

    public static BasicFileAttributes awaitFile(Path target, long timeout)
            throws IOException, InterruptedException
    {
        final Path name = target.getFileName();
        final Path targetDir = target.getParent();

        // If path already exists, return early
        try {
            logger.trace("ExportDataSetJob.awaitFile path " + target.toString() + " already exists");
            return Files.readAttributes(target, BasicFileAttributes.class);
        } catch (NoSuchFileException ex) {}

        logger.trace("ExportDataSetJob.awaitFile using WatchService to wait for file " + target.toString());
        final WatchService watchService = FileSystems.getDefault().newWatchService();
        try {
            final WatchKey watchKey = targetDir.register(watchService, StandardWatchEventKinds.ENTRY_CREATE);
            // The file could have been created in the window between Files.readAttributes and Path.register
            try {
                return Files.readAttributes(target, BasicFileAttributes.class);
            } catch (NoSuchFileException ex) {}
            // The file is absent: watch events in parent directory
            WatchKey watchKey1 = null;
            boolean valid = true;
            do {
                long t0 = System.currentTimeMillis();
                watchKey1 = watchService.poll(timeout, TimeUnit.MILLISECONDS);
                if (watchKey1 == null) {
                    return null; // timed out
                }
                // Examine events associated with key
                for (WatchEvent<?> event: watchKey1.pollEvents()) {
                    Path path1 = (Path) event.context();
                    if (path1.getFileName().equals(name)) {
                        return Files.readAttributes(target, BasicFileAttributes.class);
                    }
                }
                // Did not receive an interesting event; re-register key to queue
                long elapsed = System.currentTimeMillis() - t0;
                timeout = elapsed < timeout? (timeout - elapsed) : 0L;
                valid = watchKey1.reset();
            } while (valid);
        } finally {
            watchService.close();
        }

        return null;
    }

}
