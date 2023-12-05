package com.ospreydcs.dp.service.ingest.handler.mongo;

import com.mongodb.MongoException;
import com.mongodb.client.result.InsertManyResult;
import com.mongodb.client.result.InsertOneResult;
import com.ospreydcs.dp.grpc.v1.ingestion.IngestionRequest;
import com.ospreydcs.dp.service.common.bson.BucketDocument;
import com.ospreydcs.dp.service.common.bson.RequestStatusDocument;
import com.ospreydcs.dp.service.common.mongo.MongoSyncClient;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;

public class MongoSyncIngestionClient extends MongoSyncClient implements MongoIngestionClientInterface {

    private static final Logger LOGGER = LogManager.getLogger();

    @Override
    public MongoIngestionHandler.IngestionTaskResult insertBatch(
            IngestionRequest request, List<BucketDocument> dataDocumentBatch) {

        LOGGER.debug("MongoSyncDbHandler.insertBatch");

        // insert batch of bson data documents to mongodb
        String msg = "";
        long recordsInsertedCount = 0;
        InsertManyResult result = null;
        try {
            result = mongoCollectionBuckets.insertMany(dataDocumentBatch); // SILENTLY FAILS IF TsDataBucket DOESN'T HAVE ACCESSOR METHODS FOR ALL INST VARS!
        } catch (MongoException ex) {
            // insertMany exception
            String errorMsg = "MongoException in insertMany: " + ex.getMessage();
            LOGGER.error(errorMsg);
            return new MongoIngestionHandler.IngestionTaskResult(true, errorMsg, null);
        }

        return new MongoIngestionHandler.IngestionTaskResult(false, null, result);
    }

    @Override
    public InsertOneResult insertRequestStatus(RequestStatusDocument requestStatusDocument) {
        // insert RequestStatusDocument to mongodb
        String msg = "";
        long recordsInsertedCount = 0;
        InsertOneResult result = null;
        try {
            result = mongoCollectionRequestStatus.insertOne(requestStatusDocument); // SILENTLY FAILS IF TsDataBucket DOESN'T HAVE ACCESSOR METHODS FOR ALL INST VARS!
        } catch (MongoException ex) {
            // insertOne exception
            String errorMsg = "insertRequestStatus MongoException: " + ex.getMessage();
            LOGGER.error(errorMsg);
            return null;
        }
        return result;
    }

}
