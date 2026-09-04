package com.ospreydcs.dp.service.annotation.handler.mongo;

import com.ospreydcs.dp.grpc.v1.annotation.*;
import com.ospreydcs.dp.service.annotation.handler.interfaces.AnnotationHandlerInterface;
import com.ospreydcs.dp.service.annotation.handler.model.HandlerExportDataRequest;
import com.ospreydcs.dp.service.annotation.handler.mongo.client.MongoAnnotationClientInterface;
import com.ospreydcs.dp.service.annotation.handler.mongo.client.MongoSyncAnnotationClient;
import com.ospreydcs.dp.service.annotation.handler.mongo.job.*;
import com.ospreydcs.dp.service.annotation.service.AnnotationServiceImpl;
import com.ospreydcs.dp.service.common.bson.annotation.AnnotationDocument;
import com.ospreydcs.dp.service.common.bson.dataset.DataSetDocument;
import com.ospreydcs.dp.service.common.exception.DpException;
import com.ospreydcs.dp.service.common.handler.QueueHandlerBase;
import com.ospreydcs.dp.service.common.model.ResultStatus;
import com.ospreydcs.dp.service.query.handler.mongo.client.MongoQueryClientInterface;
import com.ospreydcs.dp.service.query.handler.mongo.client.MongoSyncQueryClient;
import io.grpc.stub.StreamObserver;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.bson.types.ObjectId;

import java.util.*;

public class MongoAnnotationHandler extends QueueHandlerBase implements AnnotationHandlerInterface {

    // static variables
    private static final Logger logger = LogManager.getLogger();

    // configuration
    public static final String CFG_KEY_NUM_WORKERS = "AnnotationHandler.numWorkers";
    public static final int DEFAULT_NUM_WORKERS = 7;
    public static final String CFG_KEY_SAMPLE_STATUS_QUERY_DEFAULT_PAGE_SIZE =
            "AnnotationHandler.sampleStatusQueryDefaultPageSize";
    public static final int DEFAULT_SAMPLE_STATUS_QUERY_DEFAULT_PAGE_SIZE = 10_000;
    public static final String CFG_KEY_SAMPLE_STATUS_QUERY_MAX_PAGE_SIZE =
            "AnnotationHandler.sampleStatusQueryMaxPageSize";
    public static final int DEFAULT_SAMPLE_STATUS_QUERY_MAX_PAGE_SIZE = 100_000;
    public static final String CFG_KEY_SAMPLE_STATUS_SAVE_MAX_STATUSES =
            "AnnotationHandler.sampleStatusSaveMaxStatuses";
    public static final long DEFAULT_SAMPLE_STATUS_SAVE_MAX_STATUSES = 1_000_000L;

    /**
     * Resolves the effective page size for a sample status query: a requested limit of 0 selects
     * the configured default, and a larger request is silently clamped to the configured maximum
     * (consistent with Query API V2 page-size handling).
     */
    public static int sampleStatusQueryPageSize(int requestedLimit) {
        final int defaultPageSize = configMgr().getConfigInteger(
                CFG_KEY_SAMPLE_STATUS_QUERY_DEFAULT_PAGE_SIZE, DEFAULT_SAMPLE_STATUS_QUERY_DEFAULT_PAGE_SIZE);
        final int maxPageSize = configMgr().getConfigInteger(
                CFG_KEY_SAMPLE_STATUS_QUERY_MAX_PAGE_SIZE, DEFAULT_SAMPLE_STATUS_QUERY_MAX_PAGE_SIZE);
        final int limit = requestedLimit > 0 ? requestedLimit : defaultPageSize;
        return Math.min(limit, maxPageSize);
    }

    public static long sampleStatusSaveMaxStatuses() {
        return configMgr().getConfigLong(
                CFG_KEY_SAMPLE_STATUS_SAVE_MAX_STATUSES, DEFAULT_SAMPLE_STATUS_SAVE_MAX_STATUSES);
    }

    // instance variables
    private final MongoAnnotationClientInterface mongoAnnotationClient;
    private final MongoQueryClientInterface mongoQueryClient;

    public MongoAnnotationHandler(
            MongoAnnotationClientInterface mongoAnnotationClient,
            MongoQueryClientInterface mongoQueryClient
    ) {
        this.mongoAnnotationClient = mongoAnnotationClient;
        this.mongoQueryClient = mongoQueryClient;
    }

    public static MongoAnnotationHandler newMongoSyncAnnotationHandler() {
        return new MongoAnnotationHandler(
                new MongoSyncAnnotationClient(), new MongoSyncQueryClient());
    }

    protected int getNumWorkers_() {
        return configMgr().getConfigInteger(CFG_KEY_NUM_WORKERS, DEFAULT_NUM_WORKERS);
    }

    @Override
    protected boolean init_() {
        logger.trace("init_");
        if (!mongoAnnotationClient.init()) {
            logger.error("error in mongoAnnotationClient.init");
            return false;
        }
        if (!mongoQueryClient.init()) {
            logger.error("error in mongoQueryClient.init");
        }

        // Dataset export issues bucket time-range queries through executeDataBlockQuery, so this
        // process must establish the max bucket span invariant before relying on the query lower
        // bound (#197) just as the query service does. The flag it controls is process-wide.
        mongoQueryClient.verifyBucketSpans();

        return true;
    }

    @Override
    protected boolean fini_() {
        if (!mongoQueryClient.fini()) {
            logger.error("error in mongoQueryClient.fini");
        }
        if (!mongoAnnotationClient.fini()) {
            logger.error("error in mongoAnnotationClient.fini");
        }
        return true;
    }

    @Override
    public void handleSaveDataSet(
            SaveDataSetRequest request, 
            StreamObserver<SaveDataSetResponse> responseObserver
    ) {
        final SaveDataSetJob job = new SaveDataSetJob(
                request,
                responseObserver,
                mongoAnnotationClient,
                this);

        logger.debug("adding SaveDataSetJob id: {} to queue", responseObserver.hashCode());

        try {
            requestQueue.put(job);
        } catch (InterruptedException e) {
            logger.error("InterruptedException waiting for requestQueue.put");
            Thread.currentThread().interrupt();
        }
    }

    @Override
    public void handleQueryDataSets(
            QueryDataSetsRequest request,
            StreamObserver<QueryDataSetsResponse> responseObserver
    ) {
        final QueryDataSetsJob job =
                new QueryDataSetsJob(request, responseObserver, mongoAnnotationClient);

        logger.debug("adding queryDataSets job id: {} to queue", responseObserver.hashCode());

        try {
            requestQueue.put(job);
        } catch (InterruptedException e) {
            logger.error("InterruptedException waiting for requestQueue.put");
            Thread.currentThread().interrupt();
        }
    }

    @Override
    public void handleGetDataSet(
            GetDataSetRequest request,
            StreamObserver<GetDataSetResponse> responseObserver
    ) {
        final GetDataSetJob job = new GetDataSetJob(request, responseObserver, mongoAnnotationClient);

        logger.debug("adding GetDataSetJob id: {} to queue", responseObserver.hashCode());

        try {
            requestQueue.put(job);
        } catch (InterruptedException e) {
            logger.error("InterruptedException waiting for requestQueue.put");
            Thread.currentThread().interrupt();
        }
    }

    @Override
    public void handleDeleteDataSet(
            DeleteDataSetRequest request,
            StreamObserver<DeleteDataSetResponse> responseObserver
    ) {
        final DeleteDataSetJob job = new DeleteDataSetJob(request, responseObserver, mongoAnnotationClient);

        logger.debug("adding DeleteDataSetJob id: {} to queue", responseObserver.hashCode());

        try {
            requestQueue.put(job);
        } catch (InterruptedException e) {
            logger.error("InterruptedException waiting for requestQueue.put");
            Thread.currentThread().interrupt();
        }
    }

    public ResultStatus validateSaveDataSetRequest(SaveDataSetRequest request) {

        // create list of unique pv names in the request's DataBlocks using a set, convert set to list
        final Set<String> uniquePvNames = new TreeSet<>();
        final List<DataBlock> dataBlocks = request.getDataBlocksList();
        if (dataBlocks.isEmpty()) {
            return new ResultStatus(true, "SaveDataSetRequest must contain dataBlocks");
        }
        for (DataBlock dataBlock : dataBlocks) {
            List<String> blockPvNames = dataBlock.getPvNamesList();
            if (blockPvNames.isEmpty()) {
                return new ResultStatus(
                        true, "SaveDataSetRequest.DataBlock must contain pvNames");
            }
            uniquePvNames.addAll(blockPvNames);
        }

        // validate that each pv exists in the archive using a cheap existence check (distinct on
        // the pvName index) rather than the full stat aggregation, which sorts and groups over
        // every bucket for each PV and grows expensive as the archive grows.
        final Collection<String> existingPvNames = mongoQueryClient.executeQueryPvExistence(uniquePvNames);
        if (existingPvNames == null) {
            return new ResultStatus(true, "database error checking existence of PV names to validate request");
        }

        // remove each existing pv from the set, and make sure the set ends up empty
        uniquePvNames.removeAll(existingPvNames);

        // we should have removed all the pv names from the set of unique names, e.g., each one exists
        if (uniquePvNames.isEmpty()) {
            return new ResultStatus(false, "");
        } else {
            return new ResultStatus(true, "no PV metadata found for names: " + uniquePvNames.toString());
        }
    }

    @Override
    public void handleSaveAnnotation(
            SaveAnnotationRequest request,
            StreamObserver<SaveAnnotationResponse> responseObserver
    ) {
        final SaveAnnotationJob job = new SaveAnnotationJob(
                request,
                responseObserver,
                mongoAnnotationClient,
                this);

        logger.debug("adding SaveAnnotationJob id: {} to queue", responseObserver.hashCode());

        try {
            requestQueue.put(job);
        } catch (InterruptedException e) {
            logger.error("InterruptedException waiting for requestQueue.put");
            Thread.currentThread().interrupt();
        }
    }

    public ResultStatus validateSaveAnnotationRequest(SaveAnnotationRequest request) {

        // Use the throwing lookup variants, not find*: a failed query must not read as "your id
        // does not exist" — that inverts the caller's retry decision (#235 reject-vs-error
        // invariant). The job routes this ResultStatus to a rejection either way today, but the
        // message must at least say the lookup failed rather than assert the record is absent.

        // check that each id in dataSetIds exists in database
        for (String dataSetId : request.getDataSetIdsList()) {

            if (dataSetId.isBlank()) {
                final String errorMsg = "SaveAnnotationRequest.dataSetIds contains blank id string";
                return new ResultStatus(true, errorMsg);
            }

            // a malformed id is a client mistake, rejected with a precise message rather than
            // surfacing as a lookup failure (#248 plan D11)
            if (!ObjectId.isValid(dataSetId)) {
                final String errorMsg = "SaveAnnotationRequest.dataSetIds contains invalid id: " + dataSetId;
                return new ResultStatus(true, errorMsg);
            }

            // execute query to retrieve DataSetDocument with specified id
            final DataSetDocument dataSetDocument;
            try {
                dataSetDocument = mongoAnnotationClient.lookupDataSet(dataSetId);
            } catch (DpException ex) {
                return new ResultStatus(
                        true,
                        "error looking up DataSetDocument with id " + dataSetId + ": " + ex.getMessage());
            }
            if (dataSetDocument == null) {
                return new ResultStatus(
                        true,
                        "no DataSetDocument found with id: " + dataSetId);
            }
        }

        // check that each id in annotationIds exists in database
        for (String annotationId : request.getAnnotationIdsList()) {

            if (annotationId.isBlank()) {
                final String errorMsg = "SaveAnnotationRequest.annotationIds contains blank id string";
                return new ResultStatus(true, errorMsg);
            }

            // a malformed id is a client mistake, rejected with a precise message rather than
            // surfacing as a lookup failure (#248 plan D11)
            if (!ObjectId.isValid(annotationId)) {
                final String errorMsg = "SaveAnnotationRequest.annotationIds contains invalid id: " + annotationId;
                return new ResultStatus(true, errorMsg);
            }

            final AnnotationDocument annotationDocument;
            try {
                annotationDocument = mongoAnnotationClient.lookupAnnotation(annotationId);
            } catch (DpException ex) {
                return new ResultStatus(
                        true,
                        "error looking up AnnotationDocument with id " + annotationId + ": " + ex.getMessage());
            }
            if (annotationDocument == null) {
                return new ResultStatus(
                        true,
                        "no AnnotationDocument found with id: " + annotationId);

            }
        }


        return new ResultStatus(false, "");
    }

    @Override
    public void handleQueryAnnotations(
            QueryAnnotationsRequest request, StreamObserver<QueryAnnotationsResponse> responseObserver
    ) {
        final QueryAnnotationsJob job =
                new QueryAnnotationsJob(request, responseObserver, mongoAnnotationClient);

        logger.debug("adding queryAnnotations job id: {} to queue", responseObserver.hashCode());

        try {
            requestQueue.put(job);
        } catch (InterruptedException e) {
            logger.error("InterruptedException waiting for requestQueue.put");
            Thread.currentThread().interrupt();
        }
    }

    @Override
    public void handleGetAnnotation(
            GetAnnotationRequest request,
            StreamObserver<GetAnnotationResponse> responseObserver
    ) {
        final GetAnnotationJob job = new GetAnnotationJob(request, responseObserver, mongoAnnotationClient);

        logger.debug("adding GetAnnotationJob id: {} to queue", responseObserver.hashCode());

        try {
            requestQueue.put(job);
        } catch (InterruptedException e) {
            logger.error("InterruptedException waiting for requestQueue.put");
            Thread.currentThread().interrupt();
        }
    }

    @Override
    public void handleDeleteAnnotation(
            DeleteAnnotationRequest request,
            StreamObserver<DeleteAnnotationResponse> responseObserver
    ) {
        final DeleteAnnotationJob job = new DeleteAnnotationJob(request, responseObserver, mongoAnnotationClient);

        logger.debug("adding DeleteAnnotationJob id: {} to queue", responseObserver.hashCode());

        try {
            requestQueue.put(job);
        } catch (InterruptedException e) {
            logger.error("InterruptedException waiting for requestQueue.put");
            Thread.currentThread().interrupt();
        }
    }

    @Override
    public void handleGetCalculations(
            GetCalculationsRequest request,
            StreamObserver<GetCalculationsResponse> responseObserver
    ) {
        final GetCalculationsJob job = new GetCalculationsJob(request, responseObserver, mongoAnnotationClient);

        logger.debug("adding GetCalculationsJob id: {} to queue", responseObserver.hashCode());

        try {
            requestQueue.put(job);
        } catch (InterruptedException e) {
            logger.error("InterruptedException waiting for requestQueue.put");
            Thread.currentThread().interrupt();
        }
    }

    @Override
    public void handleExportData(HandlerExportDataRequest handlerRequest) {

        ExportDataJobBase job = null;
        switch (handlerRequest.exportDataRequest.getOutputFormat()) {
            case EXPORT_FORMAT_UNSPECIFIED -> {
                // this should be caught in validation, but just in case...
                final String errorMsg = "ExportDataRequest.outputFormat must be specified";
                AnnotationServiceImpl.sendExportDataResponseError(errorMsg, handlerRequest.responseObserver);
            }
            case EXPORT_FORMAT_HDF5 -> {
                job = new ExportDataJobHdf5(handlerRequest, mongoAnnotationClient, mongoQueryClient);
            }
            case EXPORT_FORMAT_CSV -> {
                job = new ExportDataJobCsv(handlerRequest, mongoAnnotationClient, mongoQueryClient);
            }
            case EXPORT_FORMAT_XLSX -> {
                job = new ExportDataJobExcel(handlerRequest, mongoAnnotationClient, mongoQueryClient);
            }
            case UNRECOGNIZED -> {
                // this should be caught in validation, but just in case...
                final String errorMsg = "ExportDataRequest.outputFormat unrecognized value";
                AnnotationServiceImpl.sendExportDataResponseError(errorMsg, handlerRequest.responseObserver);
            }
        }
        Objects.requireNonNull(job);

        logger.debug("adding ExportDataJobBase id: {} to queue", handlerRequest.responseObserver.hashCode());

        try {
            requestQueue.put(job);
        } catch (InterruptedException e) {
            logger.error("InterruptedException waiting for requestQueue.put");
            Thread.currentThread().interrupt();
        }
    }

    @Override
    public void handleSavePvMetadata(
            SavePvMetadataRequest request,
            StreamObserver<SavePvMetadataResponse> responseObserver
    ) {
        final SavePvMetadataJob job = new SavePvMetadataJob(request, responseObserver, mongoAnnotationClient);

        logger.debug("adding SavePvMetadataJob id: {} to queue", responseObserver.hashCode());

        try {
            requestQueue.put(job);
        } catch (InterruptedException e) {
            logger.error("InterruptedException waiting for requestQueue.put");
            Thread.currentThread().interrupt();
        }
    }

    @Override
    public void handleQueryPvMetadata(
            QueryPvMetadataRequest request,
            StreamObserver<QueryPvMetadataResponse> responseObserver
    ) {
        final QueryPvMetadataJob job = new QueryPvMetadataJob(request, responseObserver, mongoAnnotationClient);

        logger.debug("adding QueryPvMetadataJob id: {} to queue", responseObserver.hashCode());

        try {
            requestQueue.put(job);
        } catch (InterruptedException e) {
            logger.error("InterruptedException waiting for requestQueue.put");
            Thread.currentThread().interrupt();
        }
    }

    @Override
    public void handleGetPvMetadata(
            GetPvMetadataRequest request,
            StreamObserver<GetPvMetadataResponse> responseObserver
    ) {
        final GetPvMetadataJob job = new GetPvMetadataJob(request, responseObserver, mongoAnnotationClient);

        logger.debug("adding GetPvMetadataJob id: {} to queue", responseObserver.hashCode());

        try {
            requestQueue.put(job);
        } catch (InterruptedException e) {
            logger.error("InterruptedException waiting for requestQueue.put");
            Thread.currentThread().interrupt();
        }
    }

    @Override
    public void handleDeletePvMetadata(
            DeletePvMetadataRequest request,
            StreamObserver<DeletePvMetadataResponse> responseObserver
    ) {
        final DeletePvMetadataJob job = new DeletePvMetadataJob(request, responseObserver, mongoAnnotationClient);

        logger.debug("adding DeletePvMetadataJob id: {} to queue", responseObserver.hashCode());

        try {
            requestQueue.put(job);
        } catch (InterruptedException e) {
            logger.error("InterruptedException waiting for requestQueue.put");
            Thread.currentThread().interrupt();
        }
    }

    @Override
    public void handleSaveConfiguration(
            SaveConfigurationRequest request,
            StreamObserver<SaveConfigurationResponse> responseObserver
    ) {
        final SaveConfigurationJob job = new SaveConfigurationJob(request, responseObserver, mongoAnnotationClient);

        logger.debug("adding SaveConfigurationJob id: {} to queue", responseObserver.hashCode());

        try {
            requestQueue.put(job);
        } catch (InterruptedException e) {
            logger.error("InterruptedException waiting for requestQueue.put");
            Thread.currentThread().interrupt();
        }
    }

    @Override
    public void handleGetConfiguration(
            GetConfigurationRequest request,
            StreamObserver<GetConfigurationResponse> responseObserver
    ) {
        final GetConfigurationJob job = new GetConfigurationJob(request, responseObserver, mongoAnnotationClient);

        logger.debug("adding GetConfigurationJob id: {} to queue", responseObserver.hashCode());

        try {
            requestQueue.put(job);
        } catch (InterruptedException e) {
            logger.error("InterruptedException waiting for requestQueue.put");
            Thread.currentThread().interrupt();
        }
    }

    @Override
    public void handleQueryConfigurations(
            QueryConfigurationsRequest request,
            StreamObserver<QueryConfigurationsResponse> responseObserver
    ) {
        final QueryConfigurationsJob job = new QueryConfigurationsJob(request, responseObserver, mongoAnnotationClient);

        logger.debug("adding QueryConfigurationsJob id: {} to queue", responseObserver.hashCode());

        try {
            requestQueue.put(job);
        } catch (InterruptedException e) {
            logger.error("InterruptedException waiting for requestQueue.put");
            Thread.currentThread().interrupt();
        }
    }

    @Override
    public void handleDeleteConfiguration(
            DeleteConfigurationRequest request,
            StreamObserver<DeleteConfigurationResponse> responseObserver
    ) {
        final DeleteConfigurationJob job = new DeleteConfigurationJob(request, responseObserver, mongoAnnotationClient);

        logger.debug("adding DeleteConfigurationJob id: {} to queue", responseObserver.hashCode());

        try {
            requestQueue.put(job);
        } catch (InterruptedException e) {
            logger.error("InterruptedException waiting for requestQueue.put");
            Thread.currentThread().interrupt();
        }
    }

    @Override
    public void handleSaveConfigurationActivation(
            SaveConfigurationActivationRequest request,
            StreamObserver<SaveConfigurationActivationResponse> responseObserver
    ) {
        final SaveConfigurationActivationJob job =
                new SaveConfigurationActivationJob(request, responseObserver, mongoAnnotationClient);

        logger.debug("adding SaveConfigurationActivationJob id: {} to queue", responseObserver.hashCode());

        try {
            requestQueue.put(job);
        } catch (InterruptedException e) {
            logger.error("InterruptedException waiting for requestQueue.put");
            Thread.currentThread().interrupt();
        }
    }

    @Override
    public void handleGetConfigurationActivation(
            GetConfigurationActivationRequest request,
            StreamObserver<GetConfigurationActivationResponse> responseObserver
    ) {
        final GetConfigurationActivationJob job =
                new GetConfigurationActivationJob(request, responseObserver, mongoAnnotationClient);

        logger.debug("adding GetConfigurationActivationJob id: {} to queue", responseObserver.hashCode());

        try {
            requestQueue.put(job);
        } catch (InterruptedException e) {
            logger.error("InterruptedException waiting for requestQueue.put");
            Thread.currentThread().interrupt();
        }
    }

    @Override
    public void handleQueryConfigurationActivations(
            QueryConfigurationActivationsRequest request,
            StreamObserver<QueryConfigurationActivationsResponse> responseObserver
    ) {
        final QueryConfigurationActivationsJob job =
                new QueryConfigurationActivationsJob(request, responseObserver, mongoAnnotationClient);

        logger.debug("adding QueryConfigurationActivationsJob id: {} to queue", responseObserver.hashCode());

        try {
            requestQueue.put(job);
        } catch (InterruptedException e) {
            logger.error("InterruptedException waiting for requestQueue.put");
            Thread.currentThread().interrupt();
        }
    }

    @Override
    public void handleDeleteConfigurationActivation(
            DeleteConfigurationActivationRequest request,
            StreamObserver<DeleteConfigurationActivationResponse> responseObserver
    ) {
        final DeleteConfigurationActivationJob job =
                new DeleteConfigurationActivationJob(request, responseObserver, mongoAnnotationClient);

        logger.debug("adding DeleteConfigurationActivationJob id: {} to queue", responseObserver.hashCode());

        try {
            requestQueue.put(job);
        } catch (InterruptedException e) {
            logger.error("InterruptedException waiting for requestQueue.put");
            Thread.currentThread().interrupt();
        }
    }

    @Override
    public void handleGetActiveConfigurations(
            GetActiveConfigurationsRequest request,
            StreamObserver<GetActiveConfigurationsResponse> responseObserver
    ) {
        final GetActiveConfigurationsJob job =
                new GetActiveConfigurationsJob(request, responseObserver, mongoAnnotationClient);

        logger.debug("adding GetActiveConfigurationsJob id: {} to queue", responseObserver.hashCode());

        try {
            requestQueue.put(job);
        } catch (InterruptedException e) {
            logger.error("InterruptedException waiting for requestQueue.put");
            Thread.currentThread().interrupt();
        }
    }

    @Override
    public void handleSaveSampleStatuses(
            SaveSampleStatusesRequest request,
            StreamObserver<SaveSampleStatusesResponse> responseObserver
    ) {
        final SaveSampleStatusesJob job =
                new SaveSampleStatusesJob(request, responseObserver, mongoAnnotationClient);

        logger.debug("adding SaveSampleStatusesJob id: {} to queue", responseObserver.hashCode());

        try {
            requestQueue.put(job);
        } catch (InterruptedException e) {
            logger.error("InterruptedException waiting for requestQueue.put");
            Thread.currentThread().interrupt();
        }
    }

    @Override
    public void handleQuerySampleStatuses(
            QuerySampleStatusesRequest request,
            StreamObserver<QuerySampleStatusesResponse> responseObserver
    ) {
        final QuerySampleStatusesJob job =
                new QuerySampleStatusesJob(request, responseObserver, mongoAnnotationClient);

        logger.debug("adding QuerySampleStatusesJob id: {} to queue", responseObserver.hashCode());

        try {
            requestQueue.put(job);
        } catch (InterruptedException e) {
            logger.error("InterruptedException waiting for requestQueue.put");
            Thread.currentThread().interrupt();
        }
    }

    @Override
    public void handleQuerySampleStatusesStream(
            QuerySampleStatusesRequest request,
            StreamObserver<QuerySampleStatusesResponse> responseObserver
    ) {
        final QuerySampleStatusesStreamJob job =
                new QuerySampleStatusesStreamJob(request, responseObserver, mongoAnnotationClient);

        logger.debug("adding QuerySampleStatusesStreamJob id: {} to queue", responseObserver.hashCode());

        try {
            requestQueue.put(job);
        } catch (InterruptedException e) {
            logger.error("InterruptedException waiting for requestQueue.put");
            Thread.currentThread().interrupt();
        }
    }

    @Override
    public void handleDeleteSampleStatuses(
            DeleteSampleStatusesRequest request,
            StreamObserver<DeleteSampleStatusesResponse> responseObserver
    ) {
        final DeleteSampleStatusesJob job =
                new DeleteSampleStatusesJob(request, responseObserver, mongoAnnotationClient);

        logger.debug("adding DeleteSampleStatusesJob id: {} to queue", responseObserver.hashCode());

        try {
            requestQueue.put(job);
        } catch (InterruptedException e) {
            logger.error("InterruptedException waiting for requestQueue.put");
            Thread.currentThread().interrupt();
        }
    }

}
