package com.ospreydcs.dp.service.ingest.handler.mongo;

import com.mongodb.MongoException;
import com.mongodb.client.result.InsertManyResult;
import com.mongodb.client.result.InsertOneResult;
import com.ospreydcs.dp.grpc.v1.ingestion.IngestionRequest;
import com.ospreydcs.dp.service.common.bson.BucketDocument;
import com.ospreydcs.dp.service.common.bson.RequestStatusDocument;
import com.ospreydcs.dp.service.common.mongo.MongoAsyncClient;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.reactivestreams.Publisher;

import java.util.List;

public class MongoAsyncIngestionClient extends MongoAsyncClient implements MongoIngestionClientInterface {

    private static final Logger LOGGER = LogManager.getLogger();

    @Override
    public MongoIngestionHandler.IngestionTaskResult insertBatch(IngestionRequest request, List<BucketDocument> dataDocumentBatch) {

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
            return new MongoIngestionHandler.IngestionTaskResult(true, errorMsg, null);
        }

        var receivedList = subscriber.getReceived();
        if (receivedList.size() == 0) {
            String errorMsg = "no response received from insertMany() publisher";
            LOGGER.error(errorMsg);
            return new MongoIngestionHandler.IngestionTaskResult(true, errorMsg, null);
        }

        InsertManyResult result = (InsertManyResult) receivedList.get(0);

        return new MongoIngestionHandler.IngestionTaskResult(false, null, result);
    }

    @Override
    public InsertOneResult insertRequestStatus(RequestStatusDocument requestStatusDocument) {

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
