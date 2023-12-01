package com.ospreydcs.dp.service.ingest.handler.mongo;

import com.mongodb.MongoException;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.result.InsertManyResult;
import com.mongodb.client.result.InsertOneResult;
import com.ospreydcs.dp.grpc.v1.ingestion.IngestionRequest;
import com.ospreydcs.dp.service.common.bson.BucketDocument;
import com.ospreydcs.dp.service.common.bson.RequestStatusDocument;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.bson.codecs.configuration.CodecRegistry;
import org.bson.conversions.Bson;

import java.util.*;

public class MongoSyncHandler extends MongoHandlerBase {

    private static final Logger LOGGER = LogManager.getLogger();

    protected MongoClient mongoClient = null;
    protected MongoDatabase mongoDatabase = null;
    protected MongoCollection<BucketDocument> mongoCollectionBuckets = null;
    protected MongoCollection<RequestStatusDocument> mongoCollectionRequestStatus = null;

    @Override
    protected boolean initMongoClient(String connectString) {
        mongoClient = MongoClients.create(connectString);
        return true;
    }

    @Override
    protected boolean initMongoDatabase(String databaseName, CodecRegistry codecRegistry) {
        mongoDatabase = mongoClient.getDatabase(databaseName);
        mongoDatabase = mongoDatabase.withCodecRegistry(codecRegistry);
        return true;
    }

    @Override
    protected boolean initMongoCollectionBuckets(String collectionName) {
        mongoCollectionBuckets = mongoDatabase.getCollection(collectionName, BucketDocument.class);  // creates collection if it doesn't exist
        return true;
    }

    @Override
    protected boolean createMongoIndexBuckets(Bson fieldNamesBson) {
        mongoCollectionBuckets.createIndex(fieldNamesBson);
        return true;
    }

    @Override
    protected boolean initMongoCollectionRequestStatus(String collectionName) {
        mongoCollectionRequestStatus = mongoDatabase.getCollection(collectionName, RequestStatusDocument.class);  // creates collection if it doesn't exist
        return true;
    }

    @Override
    protected boolean createMongoIndexRequestStatus(Bson fieldNamesBson) {
        mongoCollectionRequestStatus.createIndex(fieldNamesBson);
        return true;
    }

    @Override
    protected IngestionTaskResult insertBatch(IngestionRequest request, List<BucketDocument> dataDocumentBatch) {

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
            return new IngestionTaskResult(true, errorMsg, null);
        }

        return new IngestionTaskResult(false, null, result);
    }

    @Override
    protected InsertOneResult insertRequestStatus(RequestStatusDocument requestStatusDocument) {
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
