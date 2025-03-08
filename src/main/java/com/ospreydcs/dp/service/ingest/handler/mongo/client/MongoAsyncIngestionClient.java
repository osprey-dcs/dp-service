package com.ospreydcs.dp.service.ingest.handler.mongo.client;

import com.mongodb.MongoException;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.result.InsertManyResult;
import com.mongodb.client.result.InsertOneResult;
import com.ospreydcs.dp.grpc.v1.ingestion.IngestDataRequest;
import com.ospreydcs.dp.grpc.v1.ingestion.QueryRequestStatusRequest;
import com.ospreydcs.dp.grpc.v1.ingestion.RegisterProviderRequest;
import com.ospreydcs.dp.service.common.bson.DpBsonDocumentBase;
import com.ospreydcs.dp.service.common.bson.bucket.BucketDocument;
import com.ospreydcs.dp.service.common.bson.RequestStatusDocument;
import com.ospreydcs.dp.service.common.mongo.MongoAsyncClient;
import com.ospreydcs.dp.service.common.mongo.UpdateResultWrapper;
import com.ospreydcs.dp.service.ingest.handler.model.FindProviderResult;
import com.ospreydcs.dp.service.ingest.handler.mongo.ObservableSubscriber;
import com.ospreydcs.dp.service.ingest.model.IngestionTaskResult;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.reactivestreams.Publisher;

import java.time.Instant;
import java.util.List;

public class MongoAsyncIngestionClient extends MongoAsyncClient implements MongoIngestionClientInterface {

    private static final Logger logger = LogManager.getLogger();

    @Override
    public UpdateResultWrapper upsertProvider(RegisterProviderRequest request) {
        throw new UnsupportedOperationException("upsertProvider method not implemented");
    }

    @Override
    public FindProviderResult findProvider(String providerName) {
        throw new UnsupportedOperationException("findProvider method not implemented");
    }

    @Override
    public String providerNameForId(String providerId) {
        throw new UnsupportedOperationException("validateProviderId method not implemented");
    }

    @Override
    public IngestionTaskResult insertBatch(IngestDataRequest request, List<BucketDocument> dataDocumentBatch) {

        logger.debug("inserting batch of bucket documents to mongo");

        // set createdAt field for each document in batch
        final Instant now = Instant.now();
        for (DpBsonDocumentBase document : dataDocumentBatch) {
            document.setCreatedAt(now);
        }

        // invoke mongodb insertMany for batch, create subscriber to handle results
        Publisher<InsertManyResult> publisher = mongoCollectionBuckets.insertMany(dataDocumentBatch);  // SILENTLY FAILS IF TsDataBucket DOESN'T HAVE ACCESSOR METHODS FOR ALL INST VARS!
        var subscriber = new ObservableSubscriber<>();
        publisher.subscribe(subscriber);

        // wait for insert to complete and handle result
        try {
            subscriber.await();

        } catch (MongoException ex) {
            String errorMsg = "MongoException encountered: " + ex.getMessage();
            logger.error(errorMsg);
            return new IngestionTaskResult(true, errorMsg, null);
        }

        var receivedList = subscriber.getReceived();
        if (receivedList.size() == 0) {
            String errorMsg = "no response received from insertMany() publisher";
            logger.error(errorMsg);
            return new IngestionTaskResult(true, errorMsg, null);
        }

        InsertManyResult result = (InsertManyResult) receivedList.get(0);

        return new IngestionTaskResult(false, null, result);
    }

    @Override
    public InsertOneResult insertRequestStatus(RequestStatusDocument requestStatusDocument) {

        logger.debug(
                "inserting RequestStatus document to mongo provider: {} request: {}",
                requestStatusDocument.getProviderId(), requestStatusDocument.getRequestId());

        // set createdAt field for document
        requestStatusDocument.setCreatedAt(Instant.now());

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
            logger.error(errorMsg);
            return null;
        }

        var receivedList = subscriber.getReceived();
        if (receivedList.size() == 0) {
            String errorMsg = "no response received from insertOne() publisher";
            logger.error(errorMsg);
            return null;
        }

        return (InsertOneResult) receivedList.get(0);
    }

    @Override
    public MongoCursor<RequestStatusDocument> executeQueryRequestStatus(QueryRequestStatusRequest request) {
        throw new UnsupportedOperationException("executeQueryRequestStatus method not implemented");
    }
}
