package com.ospreydcs.dp.service.common.mongo;

import com.mongodb.ReadPreference;
import com.mongodb.WriteConcern;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.ospreydcs.dp.service.common.bson.ProviderDocument;
import com.ospreydcs.dp.service.common.bson.annotation.AnnotationDocument;
import com.ospreydcs.dp.service.common.bson.bucket.BucketDocument;
import com.ospreydcs.dp.service.common.bson.bucket.BucketSpanLimits;
import com.ospreydcs.dp.service.common.bson.bucket.BucketSpanVerifier;
import com.ospreydcs.dp.service.common.bson.RequestStatusDocument;
import com.ospreydcs.dp.service.common.bson.calculations.CalculationsDocument;
import com.ospreydcs.dp.service.common.bson.dataset.DataSetDocument;
import com.ospreydcs.dp.service.common.bson.configuration.ConfigurationActivationDocument;
import com.ospreydcs.dp.service.common.bson.configuration.ConfigurationDocument;
import com.ospreydcs.dp.service.common.bson.pvmetadata.PvMetadataDocument;
import com.ospreydcs.dp.service.common.bson.samplestatus.SampleStatusBucketDocument;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.bson.Document;
import org.bson.codecs.configuration.CodecRegistry;
import org.bson.conversions.Bson;

public class MongoSyncClient extends MongoClientBase {

    // static variables
    private static final Logger logger = LogManager.getLogger();

    private static final String CFG_KEY_VERIFY_BUCKET_SPANS_ON_STARTUP =
            "Buckets.verifyBucketSpansOnStartup";
    private static final boolean DEFAULT_VERIFY_BUCKET_SPANS_ON_STARTUP = true;

    // instance variables
    protected MongoClient mongoClient = null;
    protected MongoDatabase mongoDatabase = null;
    protected MongoCollection<ProviderDocument> mongoCollectionProviders = null;
    protected MongoCollection<BucketDocument> mongoCollectionBuckets = null;
    protected MongoCollection<RequestStatusDocument> mongoCollectionRequestStatus = null;
    protected MongoCollection<DataSetDocument> mongoCollectionDataSets = null;
    protected MongoCollection<AnnotationDocument> mongoCollectionAnnotations = null;
    protected MongoCollection<CalculationsDocument> mongoCollectionCalculations = null;
    protected MongoCollection<PvMetadataDocument> mongoCollectionPvMetadata = null;
    protected MongoCollection<ConfigurationDocument> mongoCollectionConfigurations = null;
    protected MongoCollection<ConfigurationActivationDocument> mongoCollectionConfigurationActivations = null;
    protected MongoCollection<SampleStatusBucketDocument> mongoCollectionSampleStatusBuckets = null;

    @Override
    protected boolean initMongoClient(String connectString) {
        mongoClient = MongoClients.create(connectString);
        return true;
    }

    @Override
    protected boolean initMongoDatabase(String databaseName, CodecRegistry codecRegistry) {

        // run 'hello' to detect mongo topology
        final MongoDatabase adminDatabase = mongoClient.getDatabase(ADMIN_DATABASE_NAME);
        final Document hello = adminDatabase.runCommand(new Document("hello", 1));
        logger.debug("mongo topology detection response: " + hello.toJson());

        if (hello.containsKey("setName")) {
            // Replica set detected
            logger.info("mongo replica set topology detected: " + hello.getString("setName"));

            // Use primary preferred read and majority write for safety
            mongoDatabase = mongoClient.getDatabase(databaseName)
                    .withReadPreference(ReadPreference.primaryPreferred())
                    .withWriteConcern(WriteConcern.MAJORITY);
        } else {
            // Standalone detected
            System.out.println("mongo standalone topology detected");

            // Standalone: normal read/write
            mongoDatabase = mongoClient.getDatabase(databaseName)
                    .withReadPreference(ReadPreference.primary())
                    .withWriteConcern(WriteConcern.ACKNOWLEDGED);
        }

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
        mongoCollectionRequestStatus = mongoDatabase.getCollection(collectionName, RequestStatusDocument.class);  // creates collection if it doesn't exist
        return true;
    }

    @Override
    protected boolean createMongoIndexRequestStatus(Bson fieldNamesBson) {
        mongoCollectionRequestStatus.createIndex(fieldNamesBson);
        return true;
    }

    @Override
    protected boolean initMongoCollectionDataSets(String collectionName) {
        mongoCollectionDataSets = mongoDatabase.getCollection(collectionName, DataSetDocument.class);  // creates collection if it doesn't exist
        return true;
    }

    @Override
    protected boolean createMongoIndexDataSets(Bson fieldNamesBson) {
        mongoCollectionDataSets.createIndex(fieldNamesBson);
        return true;
    }

    @Override
    protected boolean initMongoCollectionAnnotations(String collectionName) {
        mongoCollectionAnnotations = mongoDatabase.getCollection(collectionName, AnnotationDocument.class);  // creates collection if it doesn't exist
        return true;
    }

    @Override
    protected boolean createMongoIndexAnnotations(Bson fieldNamesBson) {
        mongoCollectionAnnotations.createIndex(fieldNamesBson);
        return true;
    }

    @Override
    protected boolean initMongoCollectionCalculations(String collectionName) {
        mongoCollectionCalculations = mongoDatabase.getCollection(collectionName, CalculationsDocument.class);  // creates collection if it doesn't exist
        return true;
    }

    @Override
    protected boolean createMongoIndexCalculations(Bson fieldNamesBson) {
        mongoCollectionCalculations.createIndex(fieldNamesBson);
        return true;
    }

    @Override
    protected boolean initMongoCollectionPvMetadata(String collectionName) {
        mongoCollectionPvMetadata = mongoDatabase.getCollection(collectionName, PvMetadataDocument.class);
        return true;
    }

    @Override
    protected boolean createMongoIndexPvMetadata(Bson fieldNamesBson) {
        mongoCollectionPvMetadata.createIndex(fieldNamesBson);
        return true;
    }

    @Override
    protected boolean createMongoIndexPvMetadataWithOptions(Bson fieldNamesBson, com.mongodb.client.model.IndexOptions indexOptions) {
        mongoCollectionPvMetadata.createIndex(fieldNamesBson, indexOptions);
        return true;
    }

    @Override
    protected boolean initMongoCollectionConfigurations(String collectionName) {
        mongoCollectionConfigurations = mongoDatabase.getCollection(collectionName, ConfigurationDocument.class);
        return true;
    }

    @Override
    protected boolean createMongoIndexConfigurations(Bson fieldNamesBson) {
        mongoCollectionConfigurations.createIndex(fieldNamesBson);
        return true;
    }

    @Override
    protected boolean createMongoIndexConfigurationsWithOptions(Bson fieldNamesBson, com.mongodb.client.model.IndexOptions indexOptions) {
        mongoCollectionConfigurations.createIndex(fieldNamesBson, indexOptions);
        return true;
    }

    @Override
    protected boolean initMongoCollectionConfigurationActivations(String collectionName) {
        mongoCollectionConfigurationActivations = mongoDatabase.getCollection(collectionName, ConfigurationActivationDocument.class);
        return true;
    }

    @Override
    protected boolean createMongoIndexConfigurationActivations(Bson fieldNamesBson) {
        mongoCollectionConfigurationActivations.createIndex(fieldNamesBson);
        return true;
    }

    @Override
    protected boolean createMongoIndexConfigurationActivationsWithOptions(Bson fieldNamesBson, com.mongodb.client.model.IndexOptions indexOptions) {
        mongoCollectionConfigurationActivations.createIndex(fieldNamesBson, indexOptions);
        return true;
    }

    @Override
    protected boolean initMongoCollectionSampleStatusBuckets(String collectionName) {
        mongoCollectionSampleStatusBuckets = mongoDatabase.getCollection(collectionName, SampleStatusBucketDocument.class);
        return true;
    }

    @Override
    protected boolean createMongoIndexSampleStatusBuckets(Bson fieldNamesBson) {
        mongoCollectionSampleStatusBuckets.createIndex(fieldNamesBson);
        return true;
    }

    /**
     * Confirms that the stored archive satisfies the configured maximum bucket span before the
     * query-side time-range lower bound (#197) is applied to it. Ingestion enforces the limit for
     * new data only, so data ingested before the limit existed could otherwise be silently excluded
     * from query results.
     *
     * <p>Lives on the shared sync client because every service that issues bucket time-range
     * queries must establish this before relying on the bound, not just the query service: the
     * annotation service reaches the same filter through dataset export. The flag it controls is
     * process-wide, so each service process must verify independently.
     *
     * <p>Runs at startup and blocks. The result is recorded so the scan happens once per limit
     * value rather than on every restart; see {@link BucketSpanVerifier}. On violation or error the
     * bound is disabled for the process and queries fall back to the slower unbounded scan, so the
     * service still returns correct results.
     *
     * @return true — verification never blocks startup; a failure disables the optimization instead
     */
    public boolean verifyBucketSpans() {

        if (!configMgr().getConfigBoolean(
                CFG_KEY_VERIFY_BUCKET_SPANS_ON_STARTUP, DEFAULT_VERIFY_BUCKET_SPANS_ON_STARTUP)) {
            logger.info(
                    "bucket span verification disabled by configuration ({}); the query time-range "
                            + "lower bound is applied without verifying the stored archive",
                    CFG_KEY_VERIFY_BUCKET_SPANS_ON_STARTUP);
            return true;
        }

        final long limitSeconds = BucketSpanLimits.getMaxBucketSpanSeconds();

        final BucketSpanVerifier.VerificationResult result = BucketSpanVerifier.verify(
                mongoDatabase.getCollection(getCollectionNameBuckets()),
                mongoDatabase.getCollection(BucketSpanVerifier.COLLECTION_NAME_BUCKET_SPAN_VERIFICATION),
                limitSeconds);

        if (!result.boundIsSafe()) {
            BucketSpanLimits.disableQueryLowerBound();
        }

        return true;
    }

}
