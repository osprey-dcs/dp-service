package com.ospreydcs.dp.service.ingest.handler.mongo.client;

import com.mongodb.MongoException;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.UpdateOptions;
import com.mongodb.client.model.Updates;
import com.mongodb.client.result.InsertManyResult;
import com.mongodb.client.result.InsertOneResult;
import com.mongodb.client.result.UpdateResult;
import com.ospreydcs.dp.grpc.v1.common.Timestamp;
import com.ospreydcs.dp.grpc.v1.ingestion.IngestDataRequest;
import com.ospreydcs.dp.grpc.v1.ingestion.IngestionRequestStatus;
import com.ospreydcs.dp.grpc.v1.ingestion.QueryRequestStatusRequest;
import com.ospreydcs.dp.grpc.v1.ingestion.RegisterProviderRequest;
import com.ospreydcs.dp.service.common.bson.BsonConstants;
import com.ospreydcs.dp.service.common.bson.DataTimestampsDocument;
import com.ospreydcs.dp.service.common.bson.DpBsonDocumentBase;
import com.ospreydcs.dp.service.common.bson.ProviderDocument;
import com.ospreydcs.dp.service.common.bson.bucket.BucketDocument;
import com.ospreydcs.dp.service.common.bson.RequestStatusDocument;
import com.ospreydcs.dp.service.common.exception.DpException;
import com.ospreydcs.dp.service.common.protobuf.AttributesUtility;
import com.ospreydcs.dp.service.common.protobuf.TimestampUtility;
import com.ospreydcs.dp.service.common.mongo.MongoSyncClient;
import com.ospreydcs.dp.service.common.mongo.UpdateResultWrapper;
import com.ospreydcs.dp.service.ingest.handler.model.FindProviderResult;
import com.ospreydcs.dp.service.ingest.model.IngestionTaskResult;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.bson.types.ObjectId;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import static com.mongodb.client.model.Filters.and;
import static com.mongodb.client.model.Filters.eq;
import static com.mongodb.client.model.Indexes.ascending;

public class MongoSyncIngestionClient extends MongoSyncClient implements MongoIngestionClientInterface {

    private static final Logger logger = LogManager.getLogger();

    // per-PV bucket span statistics (#232), created in init() once the pvStats collection exists
    private PvStatsMaxSpanUpdater pvStatsMaxSpanUpdater = null;

    @Override
    public boolean init() {
        if (!super.init()) {
            return false;
        }
        // The updater works on raw Documents: it sets one field by name and never encodes a
        // PvStatsDocument, so it takes the collection re-typed rather than the POJO handle.
        pvStatsMaxSpanUpdater = new PvStatsMaxSpanUpdater(mongoCollectionPvStats.withDocumentClass(Document.class));
        return true;
    }

    @Override
    public UpdateResultWrapper upsertProvider(RegisterProviderRequest request) {

        Bson filter = Filters.eq(BsonConstants.BSON_KEY_PROVIDER_NAME, request.getProviderName());

        final Instant instantNow = Instant.now();
        Bson updates = Updates.combine(
                Updates.set(BsonConstants.BSON_KEY_PROVIDER_NAME, request.getProviderName()),
                Updates.set(BsonConstants.BSON_KEY_PROVIDER_DESCRIPTION, request.getDescription()),
                Updates.setOnInsert(BsonConstants.BSON_KEY_CREATED_AT, instantNow),
                Updates.set(BsonConstants.BSON_KEY_UPDATED_AT, instantNow)
        );
        if ( ! request.getTagsList().isEmpty()) {
            updates = Updates.combine(updates, Updates.set(BsonConstants.BSON_KEY_TAGS, request.getTagsList()));
        }
        if ( ! request.getAttributesList().isEmpty()) {
            updates = Updates.combine(
                    updates,
                    Updates.set(
                            BsonConstants.BSON_KEY_ATTRIBUTES,
                            AttributesUtility.attributeMapFromList(request.getAttributesList())));
        }

        UpdateOptions options = new UpdateOptions().upsert(true);

        try {
            UpdateResult updateResult = mongoCollectionProviders.updateOne(filter, updates, options);
            return new UpdateResultWrapper(updateResult);
        } catch (MongoException e) {
            logger.error(e);
            return new UpdateResultWrapper(e.getMessage());
        }
    }

    @Override
    public FindProviderResult findProvider(String providerName) {

        List<ProviderDocument> matchingDocuments = new ArrayList<>();

        // wrap this in a try/catch because otherwise we take out the thread if mongo throws an exception
        try {
            mongoCollectionProviders.find(
                    eq(BsonConstants.BSON_KEY_PROVIDER_NAME, providerName)).into(matchingDocuments);
        } catch (Exception ex) {
            final String errorMsg = "mongo exception in find(): " + ex.getMessage();
            logger.error(errorMsg);
            return FindProviderResult.findProviderError(errorMsg);
        }

        if (matchingDocuments.size() == 0) {
            return FindProviderResult.findProviderError("no providers found with specified name");
        } else if (matchingDocuments.size() == 1) {
            return FindProviderResult.findProviderSuccess(matchingDocuments.get(0));
        } else {
            return FindProviderResult.findProviderError("multiple providers found with specified name");
        }
    }

    @Override
    public String providerNameForId(String providerId) {

        List<ProviderDocument> matchingDocuments = new ArrayList<>();

        // wrap this in a try/catch because otherwise we take out the thread if mongo throws an exception
        try {
            mongoCollectionProviders.find(
                    eq(BsonConstants.BSON_KEY_PROVIDER_ID, new ObjectId(providerId))).into(matchingDocuments);
        } catch (Exception ex) {
            final String errorMsg = "mongo exception in find(): " + ex.getMessage();
            logger.error(errorMsg);
            return null;
        }

        if (matchingDocuments.isEmpty()) {
            return null;
        } else {
            return matchingDocuments.get(0).getName();
        }
    }

    @Override
    public IngestionTaskResult insertBatch(
            IngestDataRequest request, List<BucketDocument> dataDocumentBatch) {

        logger.debug(
                "inserting batch of bucket documents to mongo provider: {} request: {}",
                request.getProviderId(), request.getClientRequestId());

        // set createdAt time field for each document
        final Instant now = Instant.now();
        for (DpBsonDocumentBase document : dataDocumentBatch) {
            document.setCreatedAt(now);
        }

        // Record the batch's bucket span in pvStats BEFORE inserting the buckets (#232, D4). A
        // stats write that fails fails the request with no buckets inserted; the reverse order
        // could leave a bucket a query's firstTime lower bound never covers, a silent wrong answer
        // rather than an error.
        //
        // The span is the MAXIMUM over the batch, not the first document's. Today every bucket in a
        // batch embeds the same DataTimestamps -- BucketDocument.columnBucketDocument() builds one
        // DataTimestampsDocument from the request frame and sets it on every column's bucket -- so
        // the two are equal. Taking the max anyway costs one comparison in a loop already being run
        // for the PV names, and means a future change making per-column timestamps possible cannot
        // silently under-record the span for every column but the first. Under-recording is a
        // silent wrong answer on every later query, which is the failure class #232 exists to
        // remove; it must not depend on an invariant enforced in another file.
        if (dataDocumentBatch.isEmpty()) {
            // Unreachable through the service: IngestionValidationUtility rejects a frame with no
            // columns, and IngestDataJob returns on the rejected branch before reaching the client.
            // Guarded rather than assumed, since the span below reads from the batch.
            final String errorMsg = "insertBatch received empty bucket batch";
            logger.error(errorMsg);
            return new IngestionTaskResult(true, errorMsg, null);
        }
        final List<String> pvNames = new ArrayList<>(dataDocumentBatch.size());
        long spanSeconds = 0L;
        for (BucketDocument document : dataDocumentBatch) {
            pvNames.add(document.getPvName());
            final DataTimestampsDocument dataTimestamps = document.getDataTimestamps();
            if (dataTimestamps == null
                    || dataTimestamps.getFirstTime() == null
                    || dataTimestamps.getLastTime() == null) {
                final String errorMsg = "insertBatch bucket batch is missing dataTimestamps first/last time";
                logger.error(errorMsg);
                return new IngestionTaskResult(true, errorMsg, null);
            }
            final long documentSpanSeconds =
                    dataTimestamps.getLastTime().getSeconds() - dataTimestamps.getFirstTime().getSeconds();
            if (documentSpanSeconds < 0) {
                final String errorMsg =
                        "insertBatch bucket batch has negative time span: " + documentSpanSeconds;
                logger.error(errorMsg);
                return new IngestionTaskResult(true, errorMsg, null);
            }
            spanSeconds = Math.max(spanSeconds, documentSpanSeconds);
        }
        try {
            pvStatsMaxSpanUpdater.recordSpan(pvNames, spanSeconds);
        } catch (DpException ex) {
            final String errorMsg = "error recording bucket span statistics: " + ex.getMessage();
            logger.error(errorMsg, ex);
            return new IngestionTaskResult(true, errorMsg, null);
        }

        // insert batch of bson data documents to mongodb
        InsertManyResult result = null;
        try {
            result = mongoCollectionBuckets.insertMany(dataDocumentBatch); // SILENTLY FAILS IF TsDataBucket DOESN'T HAVE ACCESSOR METHODS FOR ALL INST VARS!
        } catch (MongoException ex) {
            // insertMany exception
            final String errorMsg = "MongoException in insertMany: " + ex.getMessage();
            logger.error(errorMsg, ex);
            return new IngestionTaskResult(true, errorMsg, null);
        }

        return new IngestionTaskResult(false, null, result);
    }

    @Override
    public InsertOneResult insertRequestStatus(RequestStatusDocument requestStatusDocument) {

        logger.debug(
                "inserting RequestStatus document to mongo provider: {} request: {}",
                requestStatusDocument.getProviderId(), requestStatusDocument.getRequestId());

        // set createdAt time field for document
        requestStatusDocument.addCreationTime();

        // insert RequestStatusDocument to mongodb
        InsertOneResult result = null;
        try {
            result = mongoCollectionRequestStatus.insertOne(requestStatusDocument); // SILENTLY FAILS IF TsDataBucket DOESN'T HAVE ACCESSOR METHODS FOR ALL INST VARS!
        } catch (MongoException ex) {
            // insertOne exception
            final String errorMsg = "insertRequestStatus MongoException: " + ex.getMessage();
            logger.error(errorMsg);
            return null;
        }
        return result;
    }

    @Override
    public MongoCursor<RequestStatusDocument> executeQueryRequestStatus(QueryRequestStatusRequest request) {

        final List<Bson> criteriaFilterList = new ArrayList<>();
        final List<QueryRequestStatusRequest.QueryRequestStatusCriterion> criterionList = request.getCriteriaList();
        for (QueryRequestStatusRequest.QueryRequestStatusCriterion criterion : criterionList) {

            switch (criterion.getCriterionCase()) {

                case PROVIDERIDCRITERION -> {
                    final String providerId = criterion.getProviderIdCriterion().getProviderId();
                    if (! providerId.isBlank()) {
                        Bson filter = Filters.eq(BsonConstants.BSON_KEY_REQ_STATUS_PROVIDER_ID, providerId);
                        criteriaFilterList.add(filter);
                    }
                }

                case PROVIDERNAMECRITERION -> {
                    final String providerName = criterion.getProviderNameCriterion().getProviderName();
                    if (!providerName.isBlank()) {
                        Bson filter = Filters.eq(BsonConstants.BSON_KEY_REQ_STATUS_PROVIDER_NAME, providerName);
                        criteriaFilterList.add(filter);
                    }
                }

                case REQUESTIDCRITERION -> {
                    final String requestId = criterion.getRequestIdCriterion().getRequestId();
                    if (!requestId.isBlank()) {
                        Bson filter = Filters.eq(BsonConstants.BSON_KEY_REQ_STATUS_REQUEST_ID, requestId);
                        criteriaFilterList.add(filter);
                    }
                }

                case STATUSCRITERION -> {
                    final List<IngestionRequestStatus> statusList = criterion.getStatusCriterion().getStatusList();
                    if (!statusList.isEmpty()) {
                        List<Integer> statusCaseList = new ArrayList<>();
                        for (IngestionRequestStatus status : statusList) {
                            statusCaseList.add(status.getNumber());
                        }
                        Bson filter = Filters.in(BsonConstants.BSON_KEY_REQ_STATUS_STATUS, statusCaseList);
                        criteriaFilterList.add(filter);
                    }
                }

                case TIMERANGECRITERION -> {
                    final Timestamp beginTimestamp = criterion.getTimeRangeCriterion().getBeginTime();
                    final Timestamp endTimestamp = criterion.getTimeRangeCriterion().getEndTime();
                    if (beginTimestamp.getEpochSeconds() > 0) {
                        Date beginDate = TimestampUtility.dateFromTimestamp(beginTimestamp);
                        Date endDate = null;
                        if (endTimestamp.getEpochSeconds() > 0) {
                            endDate = TimestampUtility.dateFromTimestamp(endTimestamp);
                        } else {
                            endDate = new Date();
                        }
                        Bson filter = Filters.and(
                                Filters.gte(BsonConstants.BSON_KEY_CREATED_AT, beginDate),
                                Filters.lte(BsonConstants.BSON_KEY_CREATED_AT, endDate));
                        criteriaFilterList.add(filter);
                    }
                }

                case CRITERION_NOT_SET -> {
                    // shouldn't happen since validation checks for this, but...
                    logger.error("executeQueryRequestStatus unexpected error criterion case not set");
                }

            }
        }

        if (criteriaFilterList.isEmpty()) {
            // shouldn't happen since validation checks for this, but...
            logger.error("executeQueryRequestStatus no search criteria specified");
            return null;
        }

        // create criteria filter to be combined with and operator
        Bson criteriaFilter = Filters.exists(BsonConstants.BSON_KEY_REQ_STATUS_ID);
        criteriaFilter = and(criteriaFilterList);

        logger.debug("executing queryRequestStatus filter: " + criteriaFilter.toString());

        final MongoCursor<RequestStatusDocument> resultCursor = mongoCollectionRequestStatus
                .find(criteriaFilter)
                .sort(ascending(BsonConstants.BSON_KEY_REQ_STATUS_ID))
                .cursor();

        if (resultCursor == null) {
            logger.error("executeQueryAnnotations received null cursor from mongodb.find");
        }

        return resultCursor;
    }

}
