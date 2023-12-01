package com.ospreydcs.dp.service.ingest.handler.mongo;

import com.mongodb.MongoException;
import com.mongodb.client.result.InsertManyResult;
import com.mongodb.client.result.InsertOneResult;
import com.mongodb.reactivestreams.client.MongoClient;
import com.mongodb.reactivestreams.client.MongoClients;
import com.mongodb.reactivestreams.client.MongoDatabase;
import com.mongodb.reactivestreams.client.MongoCollection;
import com.ospreydcs.dp.grpc.v1.ingestion.IngestionRequest;
import com.ospreydcs.dp.service.common.bson.BucketDocument;
import com.ospreydcs.dp.service.common.bson.RequestStatusDocument;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.bson.codecs.configuration.CodecRegistry;
import org.bson.conversions.Bson;
import org.reactivestreams.Publisher;

import java.util.List;

/**
 * This class implements the IDbHandler interface by overriding the abstract methods declared in MongoDbHandlerBase
 * using the async mongodb driver.  It follows the same pattern as the MongoSyncDbHandler.
 */
public class MongoAsyncHandler extends MongoHandlerBase {

    private static final Logger LOGGER = LogManager.getLogger();

    protected MongoClient mongoClient = null;
    protected MongoDatabase mongoDatabase = null;
    protected MongoCollection<BucketDocument> mongoCollectionBuckets = null;
    protected MongoCollection<RequestStatusDocument> mongoCollectionRequestStatus = null;

//    private class AsyncInsertResultTask implements Callable<Boolean> {
//
//        IngestionRequest request = null;
//        ObservableSubscriber<Object> subscriber = null;
//        StreamObserver<IngestionResponse> responseObserver = null;
//
//        public AsyncInsertResultTask(IngestionTaskParams params, ObservableSubscriber<Object> subscriber) {
//            this.request = params.getIngestionRequest();
//            this.subscriber = subscriber;
//        }
//
//        public Boolean call() {
//
//            LOGGER.debug("AsyncInsertResultTask.call");
//
//            // wait for insertMany operation to complete via subscriber
//            try {
//                subscriber.await();
//
//            } catch (MongoException ex) {
//                // send error response if MongoException encountered
//                String errorMsg = "MongoException encountered: " + ex.getMessage();
//                LOGGER.error(errorMsg);
//                // TODO: handle exception by marking failure for request in new mongodb request status collection
//                // previously sent DB error grpc response
//                return false;
//            }
//
//            var receivedList = subscriber.getReceived();
//            if (receivedList.size() == 0) {
//                // send error response if no result received via subscriber
//                String errorMsg = "no response received from insertMany() publisher";
//                LOGGER.error(errorMsg);
//                // TODO: handle exception by marking failure for request in new mongodb request status collection
//                // previously sent DB error grpc response
//                return false;
//            }
//
//            // handle InsertManyResult
//            InsertManyResult result = (InsertManyResult) receivedList.get(0);
//            if (!result.wasAcknowledged()) {
//                // send error response if result not acknowledged
//                String errorMsg = "mongodb error in insertMany, not acknowledged";
//                LOGGER.error(errorMsg);
//                // TODO: handle exception by marking failure for request in new mongodb request status collection
//                // previously sent DB error grpc response
//                return false;
//            }
//            long recordsInsertedCount = result.getInsertedIds().size();
//            long recordsExpected = request.getDataTable().getColumnsList().size();
//            if (recordsInsertedCount != recordsExpected) {
//                // send error response if records inserted doesn't match expected
//                String errorMsg = "error in insertMany, actual records inserted: "
//                        + recordsInsertedCount + " mismatch expected: " + recordsExpected;
//                LOGGER.error(errorMsg);
//                // TODO: handle exception by marking failure for request in new mongodb request status collection
//                // previously sent DB error grpc response
//                return false;
//            }
//
//            // generate a report response with ids inserted
//            // TODO: mark successful processing for request in new mongodb request status collection
////            List<String> idsInserted = new ArrayList<>();
////            for (var entry : result.getInsertedIds().entrySet()) {
////                idsInserted.add(entry.getValue().asString().getValue());
////            }
////            IngestionResponse reportResponse = ingestionResponseReport(request, idsInserted);
////            responseObserver.onNext(reportResponse);
//
//            return true;
//        }
//    }

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
        mongoCollectionRequestStatus =
                mongoDatabase.getCollection(collectionName, RequestStatusDocument.class);  // creates collection if it doesn't exist
        return true;
    }

    @Override
    protected boolean createMongoIndexRequestStatus(Bson fieldNamesBson) {
        mongoCollectionRequestStatus.createIndex(fieldNamesBson);
        return true;
    }

    @Override
    protected IngestionTaskResult insertBatch(IngestionRequest request, List<BucketDocument> dataDocumentBatch) {

        LOGGER.debug("MongoAsyncDbHandler.insertBatch");

        // invoke mongodb insertMany for batch, create subscriber to handle results
        Publisher<InsertManyResult> publisher = mongoCollectionBuckets.insertMany(dataDocumentBatch);  // SILENTLY FAILS IF TsDataBucket DOESN'T HAVE ACCESSOR METHODS FOR ALL INST VARS!
        var subscriber = new ObservableSubscriber<>();
        publisher.subscribe(subscriber);

        // wait for insert to complete and handle result
        try {
            subscriber.await();

        } catch (MongoException ex) {
            String errorMsg = "MongoException encountered: " + ex.getMessage();
            LOGGER.error(errorMsg);
            return new IngestionTaskResult(true, errorMsg, null);
        }

        var receivedList = subscriber.getReceived();
        if (receivedList.size() == 0) {
            String errorMsg = "no response received from insertMany() publisher";
            LOGGER.error(errorMsg);
            return new IngestionTaskResult(true, errorMsg, null);
        }

        InsertManyResult result = (InsertManyResult) receivedList.get(0);

        return new IngestionTaskResult(false, null, result);
    }

    @Override
    protected InsertOneResult insertRequestStatus(RequestStatusDocument requestStatusDocument) {

        // invoke mongodb insertOne, create subscriber to handle results
        Publisher<InsertOneResult> publisher =
                mongoCollectionRequestStatus.insertOne(requestStatusDocument);  // SILENTLY FAILS IF TsDataBucket DOESN'T HAVE ACCESSOR METHODS FOR ALL INST VARS!
        var subscriber = new ObservableSubscriber<>();
        publisher.subscribe(subscriber);

        // wait for insert to complete and handle result
        try {
            subscriber.await();

        } catch (MongoException ex) {
            String errorMsg = "insertRequestStatus MongoException: " + ex.getMessage();
            LOGGER.error(errorMsg);
            return null;
        }

        var receivedList = subscriber.getReceived();
        if (receivedList.size() == 0) {
            String errorMsg = "no response received from insertOne() publisher";
            LOGGER.error(errorMsg);
            return null;
        }

        return (InsertOneResult) receivedList.get(0);
    }
}
