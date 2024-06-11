package com.ospreydcs.dp.service.ingest.handler.mongo;

import com.mongodb.MongoException;
import com.mongodb.client.result.InsertManyResult;
import com.mongodb.client.result.InsertOneResult;
import com.ospreydcs.dp.grpc.v1.ingestion.IngestDataRequest;
import com.ospreydcs.dp.service.common.bson.bucket.BucketDocument;
import com.ospreydcs.dp.service.common.bson.RequestStatusDocument;
import com.ospreydcs.dp.service.common.mongo.MongoSyncClient;
import com.ospreydcs.dp.service.ingest.model.IngestionTaskResult;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;

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

}
