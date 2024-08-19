package com.ospreydcs.dp.service.common.mongo;

import com.mongodb.client.MongoDatabase;
import com.ospreydcs.dp.service.common.bson.RequestStatusDocument;
import com.ospreydcs.dp.service.common.bson.annotation.AnnotationDocument;
import com.ospreydcs.dp.service.common.bson.bucket.BucketDocument;
import com.ospreydcs.dp.service.common.bson.dataset.DataSetDocument;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.bson.conversions.Bson;
import org.bson.types.ObjectId;

import java.util.ArrayList;
import java.util.List;

import static com.mongodb.client.model.Filters.and;
import static com.mongodb.client.model.Filters.eq;

public class MongoTestClient extends MongoSyncClient {

    // static variables
    private static final Logger logger = LogManager.getLogger();

    // constants
    public static final String MONGO_TEST_DATABASE_NAME = "dp-test";
    public static final int MONGO_FIND_RETRY_COUNT = 300;
    public static final int MONGO_FIND_RETRY_INTERVAL_MILLIS = 100;

    @Override
    public boolean init() {

        // override the default database name globally
        logger.info("overriding db name globally to: {}", MONGO_TEST_DATABASE_NAME);
        MongoClientBase.setMongoDatabaseName(MONGO_TEST_DATABASE_NAME);

        // init so we have database client for dropping existing db
        super.init();
        dropTestDatabase();
        super.fini();

        // re-initialize to recreate db and collections as needed
        return super.init();
    }

    public void dropTestDatabase() {
        logger.info("dropping database: {}", MONGO_TEST_DATABASE_NAME);
        MongoDatabase database = this.mongoClient.getDatabase(MONGO_TEST_DATABASE_NAME);
        database.drop();
    }

    public static void prepareTestDatabase() {
        MongoTestClient testClient = new MongoTestClient();
        testClient.init();
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
            List<AnnotationDocument> matchingAnnotations = new ArrayList<>();
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


}
