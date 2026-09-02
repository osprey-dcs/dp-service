package com.ospreydcs.dp.service.common.mongo;

import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import com.ospreydcs.dp.service.common.bson.ProviderDocument;
import com.ospreydcs.dp.service.common.bson.RequestStatusDocument;
import com.ospreydcs.dp.service.common.bson.annotation.AnnotationDocument;
import com.ospreydcs.dp.service.common.bson.bucket.BucketDocument;
import com.ospreydcs.dp.service.common.bson.calculations.CalculationsDocument;
import com.ospreydcs.dp.service.common.bson.dataset.DataBlockDocument;
import com.ospreydcs.dp.service.common.bson.dataset.DataSetDocument;
import com.ospreydcs.dp.service.common.bson.configuration.ConfigurationActivationDocument;
import com.ospreydcs.dp.service.common.bson.configuration.ConfigurationDocument;
import com.ospreydcs.dp.service.common.bson.pvmetadata.PvMetadataDocument;
import com.ospreydcs.dp.service.common.bson.samplestatus.SampleStatusBucketDocument;
import com.ospreydcs.dp.service.query.handler.mongo.client.MongoSyncQueryClient;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.bson.conversions.Bson;
import org.bson.types.ObjectId;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

import static com.mongodb.client.model.Filters.and;
import static com.mongodb.client.model.Filters.eq;

public class MongoTestClient extends MongoSyncClient {

    // static variables
    private static final Logger logger = LogManager.getLogger();

    // constants
    public static final String MONGO_TEST_DATABASE_NAME = "dp-test";
    public static final String CFG_KEY_TEST_DATABASE_NAME = "MongoClient.testDatabaseName";
    public static final int MONGO_FIND_RETRY_COUNT = 300;
    public static final int MONGO_FIND_RETRY_INTERVAL_MILLIS = 100;

    /**
     * Suppresses the migration runner for the throwaway init that precedes dropping the test
     * database. Instance state rather than a config key so it cannot leak into the second init,
     * which must exercise the real path.
     */
    private boolean skipSchemaMigrations = false;

    @Override
    public boolean init() {

        // resolve test database name from config (supports DP_MONGO_TEST_DB_NAME env override), falling back to constant
        String testDatabaseName = configMgr().getConfigString(CFG_KEY_TEST_DATABASE_NAME, MONGO_TEST_DATABASE_NAME);

        // override the default database name globally
        logger.warn("overriding db name globally to: {} — THIS DATABASE WILL BE DROPPED", testDatabaseName);
        MongoClientBase.setMongoDatabaseName(testDatabaseName);

        // Init so we have a database client for dropping the existing db. Migrations are suppressed
        // for this pass: it runs against whatever the previous test run left behind, and that
        // database is about to be dropped, so migrating it is at best wasted work. It is also
        // actively harmful — a test that deliberately leaves a stuck `migrating: true` claim (as the
        // migration runner's own failure-path tests do) would make this init block for the full
        // claim-wait timeout and then fail, in whatever unrelated test happened to run next.
        skipSchemaMigrations = true;
        try {
            super.init();
            dropTestDatabase();
            super.fini();
        } finally {
            skipSchemaMigrations = false;
        }

        // re-initialize to recreate db and collections as needed; this pass migrates normally, which
        // for a freshly dropped database means taking the fresh-install path
        return super.init();
    }

    @Override
    protected boolean runSchemaMigrations() {
        if (skipSchemaMigrations) {
            logger.debug("skipping schema migrations for the pre-drop test client init");
            return true;
        }
        return super.runSchemaMigrations();
    }

    public void dropTestDatabase() {
        String dbName = getMongoDatabaseName();
        if (dbName.equals(MongoClientBase.MONGO_DATABASE_NAME)) {
            throw new IllegalStateException(
                    "dropTestDatabase() refused to drop production database: " + dbName);
        }
        logger.warn("dropping database: {}", dbName);
        MongoDatabase database = this.mongoClient.getDatabase(dbName);
        database.drop();
    }

    public static void prepareTestDatabase() {
        MongoTestClient testClient = new MongoTestClient();
        testClient.init();
    }

    public static String getConfiguredTestDatabaseName() {
        return getMongoDatabaseName();
    }

    /**
     * Writes a bucket document straight to the collection, bypassing the ingestion service and its
     * validation. Lets a test create a bucket that ingestion would reject — notably one exceeding
     * the max bucket span limit, standing in for data ingested before that limit existed (#197).
     */
    public void insertBucketDocument(BucketDocument bucketDocument) {
        mongoCollectionBuckets.insertOne(bucketDocument);
    }

    public ProviderDocument findProvider(String providerId) {
        for (int retryCount = 0 ; retryCount < MONGO_FIND_RETRY_COUNT ; ++retryCount){
            List<ProviderDocument> matchingDocuments = new ArrayList<>();
            mongoCollectionProviders.find(eq("_id", new ObjectId(providerId))).into(matchingDocuments);
            if (matchingDocuments.size() > 0) {
                return matchingDocuments.get(0);
            } else {
                try {
                    logger.info("findProvider id: " + providerId + " retrying");
                    Thread.sleep(MONGO_FIND_RETRY_INTERVAL_MILLIS);
                } catch (InterruptedException ex) {
                    // ignore and just retry
                }
            }
        }
        return null;
    }

    public BucketDocument findBucket(String id) {
        for (int retryCount = 0 ; retryCount < MONGO_FIND_RETRY_COUNT ; ++retryCount){
            List<BucketDocument> matchingBuckets = new ArrayList<>();
            mongoCollectionBuckets.find(eq("_id", id)).into(matchingBuckets);
            if (matchingBuckets.size() > 0) {
                return matchingBuckets.get(0);
            } else {
                try {
                    logger.info("findBucket id: " + id + " retrying");
                    Thread.sleep(MONGO_FIND_RETRY_INTERVAL_MILLIS);
                } catch (InterruptedException ex) {
                    // ignore and just retry
                }
            }
        }
        return null;
    }

    public RequestStatusDocument findRequestStatus(String providerId, String requestId) {
        for (int retryCount = 0 ; retryCount < MONGO_FIND_RETRY_COUNT ; ++retryCount) {
            List<RequestStatusDocument> matchingDocuments = new ArrayList<>();
            Bson filter = and(eq("providerId", providerId), eq("requestId", requestId));
            mongoCollectionRequestStatus.find(filter).into(matchingDocuments);
            if (matchingDocuments.size() > 0) {
                return matchingDocuments.get(0);
            } else {
                try {
                    logger.info("findRequestStatus providerId: " + providerId
                            + " requestId: " + requestId
                            + " retrying");
                    Thread.sleep(MONGO_FIND_RETRY_INTERVAL_MILLIS);
                } catch (InterruptedException ex) {
                    // ignore and just retry
                }
            }
        }
        return null;
    }

    public DataSetDocument findDataSet(String dataSetId) {
        for (int retryCount = 0 ; retryCount < MONGO_FIND_RETRY_COUNT ; ++retryCount){
            List<DataSetDocument> matchingDocuments = new ArrayList<>();
            mongoCollectionDataSets.find(eq("_id", new ObjectId(dataSetId))).into(matchingDocuments);
            if (matchingDocuments.size() > 0) {
                return matchingDocuments.get(0);
            } else {
                try {
                    logger.info("findDataSet id: " + dataSetId + " retrying");
                    Thread.sleep(MONGO_FIND_RETRY_INTERVAL_MILLIS);
                } catch (InterruptedException ex) {
                    // ignore and just retry
                }
            }
        }
        return null;
    }

    public AnnotationDocument findAnnotation(String annotationId) {
        for (int retryCount = 0 ; retryCount < MONGO_FIND_RETRY_COUNT ; ++retryCount){
            final List<AnnotationDocument> matchingAnnotations = new ArrayList<>();
            mongoCollectionAnnotations.find(eq("_id", new ObjectId(annotationId))).into(matchingAnnotations);
            if (matchingAnnotations.size() > 0) {
                return matchingAnnotations.get(0);
            } else {
                try {
                    logger.info("findAnnotation id: " + annotationId + " retrying");
                    Thread.sleep(MONGO_FIND_RETRY_INTERVAL_MILLIS);
                } catch (InterruptedException ex) {
                    // ignore and just retry
                }
            }
        }
        return null;
    }

    public PvMetadataDocument findPvMetadata(String pvName) {
        for (int retryCount = 0 ; retryCount < MONGO_FIND_RETRY_COUNT ; ++retryCount){
            final List<PvMetadataDocument> matchingDocuments = new ArrayList<>();
            mongoCollectionPvMetadata.find(eq("pvName", pvName)).into(matchingDocuments);
            if (matchingDocuments.size() > 0) {
                return matchingDocuments.get(0);
            } else {
                try {
                    logger.info("findPvMetadata pvName: " + pvName + " retrying");
                    Thread.sleep(MONGO_FIND_RETRY_INTERVAL_MILLIS);
                } catch (InterruptedException ex) {
                    // ignore and just retry
                }
            }
        }
        return null;
    }

    public List<BucketDocument> findDataSetBuckets(DataSetDocument dataset) {
        final MongoSyncQueryClient mongoSyncQueryClient = new MongoSyncQueryClient();
        mongoSyncQueryClient.init();
        final List<BucketDocument> datasetBuckets = new ArrayList<>();
        for (DataBlockDocument dataBlock : dataset.getDataBlocks()) {
            final MongoCursor<BucketDocument> documentCursor = mongoSyncQueryClient.executeDataBlockQuery(dataBlock);
            while (documentCursor.hasNext()) {
                datasetBuckets.add(documentCursor.next());
            }
        }
        return datasetBuckets;
    }

    public MongoCursor<BucketDocument> findDataBlockBuckets(DataBlockDocument dataBlock) {
        final MongoSyncQueryClient mongoSyncQueryClient = new MongoSyncQueryClient();
        mongoSyncQueryClient.init();
        return mongoSyncQueryClient.executeDataBlockQuery(dataBlock);
    }

    public ConfigurationDocument findConfiguration(String configurationName) {
        for (int retryCount = 0 ; retryCount < MONGO_FIND_RETRY_COUNT ; ++retryCount){
            final List<ConfigurationDocument> matchingDocuments = new ArrayList<>();
            mongoCollectionConfigurations.find(eq("configurationName", configurationName)).into(matchingDocuments);
            if (matchingDocuments.size() > 0) {
                return matchingDocuments.get(0);
            } else {
                try {
                    logger.info("findConfiguration configurationName: " + configurationName + " retrying");
                    Thread.sleep(MONGO_FIND_RETRY_INTERVAL_MILLIS);
                } catch (InterruptedException ex) {
                    // ignore and just retry
                }
            }
        }
        return null;
    }

    public ConfigurationActivationDocument findConfigurationActivationById(String clientActivationId) {
        for (int retryCount = 0 ; retryCount < MONGO_FIND_RETRY_COUNT ; ++retryCount){
            final List<ConfigurationActivationDocument> matchingDocuments = new ArrayList<>();
            mongoCollectionConfigurationActivations.find(eq("clientActivationId", clientActivationId))
                    .into(matchingDocuments);
            if (matchingDocuments.size() > 0) {
                return matchingDocuments.get(0);
            } else {
                try {
                    logger.info("findConfigurationActivationById id: " + clientActivationId + " retrying");
                    Thread.sleep(MONGO_FIND_RETRY_INTERVAL_MILLIS);
                } catch (InterruptedException ex) {
                    // ignore and just retry
                }
            }
        }
        return null;
    }

    public ConfigurationActivationDocument findConfigurationActivationByCompositeKey(
            String configurationName, Instant startTime) {
        for (int retryCount = 0 ; retryCount < MONGO_FIND_RETRY_COUNT ; ++retryCount){
            final List<ConfigurationActivationDocument> matchingDocuments = new ArrayList<>();
            final Bson filter = and(
                    eq("configurationName", configurationName),
                    eq("startTime", startTime));
            mongoCollectionConfigurationActivations.find(filter).into(matchingDocuments);
            if (matchingDocuments.size() > 0) {
                return matchingDocuments.get(0);
            } else {
                try {
                    logger.info("findConfigurationActivationByCompositeKey configurationName: "
                            + configurationName + " startTime: " + startTime + " retrying");
                    Thread.sleep(MONGO_FIND_RETRY_INTERVAL_MILLIS);
                } catch (InterruptedException ex) {
                    // ignore and just retry
                }
            }
        }
        return null;
    }

    public CalculationsDocument findCalculations(String calculationsId) {
        for (int retryCount = 0 ; retryCount < MONGO_FIND_RETRY_COUNT ; ++retryCount){
            final List<CalculationsDocument> matchingDocuments = new ArrayList<>();
            mongoCollectionCalculations.find(eq("_id", new ObjectId(calculationsId))).into(matchingDocuments);
            if (matchingDocuments.size() > 0) {
                return matchingDocuments.get(0);
            } else {
                try {
                    logger.info("findCalculations id: " + calculationsId + " retrying");
                    Thread.sleep(MONGO_FIND_RETRY_INTERVAL_MILLIS);
                } catch (InterruptedException ex) {
                    // ignore and just retry
                }
            }
        }
        return null;
    }

    /**
     * Returns all sampleStatusBuckets documents for the given identity prefix, ordered by
     * firstTimeNanos. Follows the retry-loop pattern (worker-thread insertion): retries until at
     * least one matching document appears, returning an empty list if none ever does — callers
     * expecting zero documents should use findSampleStatusBucketsNoRetry().
     */
    public List<SampleStatusBucketDocument> findSampleStatusBuckets(String pvName, String domain, String layer) {
        for (int retryCount = 0 ; retryCount < MONGO_FIND_RETRY_COUNT ; ++retryCount){
            final List<SampleStatusBucketDocument> matchingDocuments =
                    findSampleStatusBucketsNoRetry(pvName, domain, layer);
            if (matchingDocuments.size() > 0) {
                return matchingDocuments;
            } else {
                try {
                    logger.info("findSampleStatusBuckets pvName: " + pvName + " retrying");
                    Thread.sleep(MONGO_FIND_RETRY_INTERVAL_MILLIS);
                } catch (InterruptedException ex) {
                    // ignore and just retry
                }
            }
        }
        return new ArrayList<>();
    }

    public List<SampleStatusBucketDocument> findSampleStatusBucketsNoRetry(
            String pvName, String domain, String layer) {
        final List<SampleStatusBucketDocument> matchingDocuments = new ArrayList<>();
        mongoCollectionSampleStatusBuckets.find(and(
                        eq("pvName", pvName), eq("domain", domain), eq("layer", layer)))
                .sort(com.mongodb.client.model.Sorts.ascending("firstTimeNanos"))
                .into(matchingDocuments);
        return matchingDocuments;
    }

}
