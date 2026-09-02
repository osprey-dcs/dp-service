package com.ospreydcs.dp.service.common.mongo;

import com.mongodb.reactivestreams.client.MongoClient;
import com.mongodb.reactivestreams.client.MongoClients;
import com.mongodb.reactivestreams.client.MongoCollection;
import com.mongodb.reactivestreams.client.MongoDatabase;
import com.ospreydcs.dp.service.common.bson.ProviderDocument;
import com.ospreydcs.dp.service.common.bson.annotation.AnnotationDocument;
import com.ospreydcs.dp.service.common.bson.bucket.BucketDocument;
import com.ospreydcs.dp.service.common.bson.RequestStatusDocument;
import com.ospreydcs.dp.service.common.bson.calculations.CalculationsDocument;
import com.ospreydcs.dp.service.common.bson.dataset.DataSetDocument;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.bson.codecs.configuration.CodecRegistry;
import org.bson.conversions.Bson;

import javax.xml.crypto.Data;

public class MongoAsyncClient extends MongoClientBase {

    // static variables
    private static final Logger LOGGER = LogManager.getLogger();

    // instance variables
    protected MongoClient mongoClient = null;
    protected MongoDatabase mongoDatabase = null;
    protected MongoCollection<ProviderDocument> mongoCollectionProviders = null;
    protected MongoCollection<BucketDocument> mongoCollectionBuckets = null;
    protected MongoCollection<RequestStatusDocument> mongoCollectionRequestStatus = null;
    protected MongoCollection<DataSetDocument> mongoCollectionDataSets = null;
    protected MongoCollection<AnnotationDocument> mongoCollectionAnnotations = null;
    protected MongoCollection<CalculationsDocument> mongoCollectionCalculations = null;

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
    protected boolean initMongoCollectionProviders(String collectionName) {
        mongoCollectionProviders = mongoDatabase.getCollection(collectionName, ProviderDocument.class);  // creates collection if it doesn't exist
        return true;
    }

    @Override
    protected boolean createMongoIndexProviders(Bson fieldNamesBson) {
        mongoCollectionProviders.createIndex(fieldNamesBson);
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
    protected boolean initMongoCollectionDataSets(String collectionName) {
        mongoCollectionDataSets =
                mongoDatabase.getCollection(collectionName, DataSetDocument.class);  // creates collection if it doesn't exist
        return true;
    }

    @Override
    protected boolean createMongoIndexDataSets(Bson fieldNamesBson) {
        mongoCollectionDataSets.createIndex(fieldNamesBson);
        return true;
    }

    @Override
    protected boolean initMongoCollectionAnnotations(String collectionName) {
        mongoCollectionAnnotations =
                mongoDatabase.getCollection(collectionName, AnnotationDocument.class);  // creates collection if it doesn't exist
        return true;
    }

    @Override
    protected boolean createMongoIndexAnnotations(Bson fieldNamesBson) {
        mongoCollectionAnnotations.createIndex(fieldNamesBson);
        return true;
    }

    @Override
    protected boolean initMongoCollectionCalculations(String collectionName) {
        mongoCollectionCalculations =
                mongoDatabase.getCollection(collectionName, CalculationsDocument.class);  // creates collection if it doesn't exist
        return true;
    }

    @Override
    protected boolean createMongoIndexCalculations(Bson fieldNamesBson) {
        mongoCollectionCalculations.createIndex(fieldNamesBson);
        return true;
    }

    @Override
    protected boolean initMongoCollectionPvMetadata(String collectionName) {
        // pvMetadata collection not used by async client
        return true;
    }

    @Override
    protected boolean createMongoIndexPvMetadata(Bson fieldNamesBson) {
        // pvMetadata indexes not used by async client
        return true;
    }

    @Override
    protected boolean createMongoIndexPvMetadataWithOptions(Bson fieldNamesBson, com.mongodb.client.model.IndexOptions indexOptions) {
        // pvMetadata indexes not used by async client
        return true;
    }

    @Override
    protected boolean initMongoCollectionConfigurations(String collectionName) {
        // configurations collection not used by async client
        return true;
    }

    @Override
    protected boolean createMongoIndexConfigurations(Bson fieldNamesBson) {
        // configurations indexes not used by async client
        return true;
    }

    @Override
    protected boolean createMongoIndexConfigurationsWithOptions(Bson fieldNamesBson, com.mongodb.client.model.IndexOptions indexOptions) {
        // configurations indexes not used by async client
        return true;
    }

    @Override
    protected boolean initMongoCollectionConfigurationActivations(String collectionName) {
        // configurationActivations collection not used by async client
        return true;
    }

    @Override
    protected boolean createMongoIndexConfigurationActivations(Bson fieldNamesBson) {
        // configurationActivations indexes not used by async client
        return true;
    }

    @Override
    protected boolean createMongoIndexConfigurationActivationsWithOptions(Bson fieldNamesBson, com.mongodb.client.model.IndexOptions indexOptions) {
        // configurationActivations indexes not used by async client
        return true;
    }

    @Override
    protected boolean initMongoCollectionSampleStatusBuckets(String collectionName) {
        // sampleStatusBuckets collection not used by async client
        return true;
    }

    /**
     * Not supported on this client: the reactive driver exposes no synchronous
     * {@code MongoDatabase}, and the runner needs one because a migration must complete before
     * startup continues rather than proceeding asynchronously alongside it.
     *
     * <p>This returns true — it does <b>not</b> fail startup — and the distinction is worth being
     * precise about, because failing closed is the whole point of the mechanism elsewhere. The async
     * client is not a second database; it connects to the same one the sync clients do, and every
     * deployed service runs a sync client that migrates it. So the schema this client sees is
     * established by those processes, and refusing to start here would fail a process that has no
     * migration to perform and no way to perform one.
     *
     * <p>What is genuinely missing is the <i>version check</i>: this client cannot confirm the
     * database matches the schema this binary expects. That gap is acceptable only while the async
     * client stays off every production path — {@code MongoIngestionHandler}'s async factory is
     * commented out, and the sole constructor call is in {@code MongoAsyncIngestionHandlerTest}.
     * <b>Putting this client into service requires implementing the version check first</b>, by
     * reading the marker through a short-lived sync client, or the process would serve requests
     * against a schema it never verified.
     */
    @Override
    protected boolean runSchemaMigrations() {
        LOGGER.warn(
                "schema migrations and the schema version check are not supported on the async "
                        + "mongo client; relying on a sync-client process to have migrated this "
                        + "database. Do not put the async client on a production path without "
                        + "implementing the version check.");
        return true;
    }

    @Override
    protected boolean createMongoIndexSampleStatusBuckets(Bson fieldNamesBson) {
        // sampleStatusBuckets indexes not used by async client
        return true;
    }
}
