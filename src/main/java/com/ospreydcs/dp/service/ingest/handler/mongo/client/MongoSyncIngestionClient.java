package com.ospreydcs.dp.service.ingest.handler.mongo.client;

import com.mongodb.MongoException;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.model.Filters;
import com.mongodb.client.result.InsertManyResult;
import com.mongodb.client.result.InsertOneResult;
import com.ospreydcs.dp.grpc.v1.ingestion.IngestDataRequest;
import com.ospreydcs.dp.grpc.v1.ingestion.QueryRequestStatusRequest;
import com.ospreydcs.dp.service.common.bson.BsonConstants;
import com.ospreydcs.dp.service.common.bson.bucket.BucketDocument;
import com.ospreydcs.dp.service.common.bson.RequestStatusDocument;
import com.ospreydcs.dp.service.common.mongo.MongoSyncClient;
import com.ospreydcs.dp.service.ingest.model.IngestionTaskResult;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.bson.conversions.Bson;

import java.util.ArrayList;
import java.util.List;

import static com.mongodb.client.model.Filters.and;
import static com.mongodb.client.model.Indexes.ascending;

public class MongoSyncIngestionClient extends MongoSyncClient implements MongoIngestionClientInterface {

    private static final Logger logger = LogManager.getLogger();

    @Override
    public IngestionTaskResult insertBatch(
            IngestDataRequest request, List<BucketDocument> dataDocumentBatch) {

        logger.debug(
                "inserting batch of bucket documents to mongo provider: {} request: {}",
                request.getProviderId(), request.getClientRequestId());

        // insert batch of bson data documents to mongodb
        InsertManyResult result = null;
        try {
            result = mongoCollectionBuckets.insertMany(dataDocumentBatch); // SILENTLY FAILS IF TsDataBucket DOESN'T HAVE ACCESSOR METHODS FOR ALL INST VARS!
        } catch (MongoException ex) {
            // insertMany exception
            final String errorMsg = "MongoException in insertMany: " + ex.getMessage();
            logger.error(errorMsg);
            return new IngestionTaskResult(true, errorMsg, null);
        }

        return new IngestionTaskResult(false, null, result);
    }

    @Override
    public InsertOneResult insertRequestStatus(RequestStatusDocument requestStatusDocument) {

        logger.debug(
                "inserting RequestStatus document to mongo provider: {} request: {}",
                requestStatusDocument.getProviderId(), requestStatusDocument.getRequestId());

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
                    final int providerId = criterion.getProviderIdCriterion().getProviderId();
                    if (providerId > 0) {
                        Bson filter = Filters.eq(BsonConstants.BSON_KEY_REQ_STATUS_PROVIDER_ID, providerId);
                        criteriaFilterList.add(filter);
                    }
                }

                case PROVIDERNAMECRITERION -> {
                }

                case REQUESTIDCRITERION -> {
                    final String requestId = criterion.getRequestIdCriterion().getRequestId();
                    if (!requestId.isBlank()) {
                        Bson filter = Filters.eq(BsonConstants.BSON_KEY_REQ_STATUS_REQUEST_ID, requestId);
                        criteriaFilterList.add(filter);
                    }
                }

                case STATUSCRITERION -> {
                }

                case TIMERANGECRITERION -> {
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
