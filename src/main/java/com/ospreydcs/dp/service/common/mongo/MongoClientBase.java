package com.ospreydcs.dp.service.common.mongo;

import com.mongodb.client.model.Indexes;
import com.ospreydcs.dp.common.config.ConfigurationManager;
import com.ospreydcs.dp.service.common.bson.BucketDocument;
import com.ospreydcs.dp.service.common.bson.BsonConstants;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.bson.codecs.configuration.CodecProvider;
import org.bson.codecs.configuration.CodecRegistry;
import org.bson.codecs.pojo.PojoCodecProvider;
import org.bson.conversions.Bson;

import static com.mongodb.MongoClientSettings.getDefaultCodecRegistry;
import static org.bson.codecs.configuration.CodecRegistries.fromProviders;
import static org.bson.codecs.configuration.CodecRegistries.fromRegistries;

public abstract class MongoClientBase {

    // abstract methods
    protected abstract boolean initMongoClient(String connectString);
    protected abstract boolean initMongoDatabase(String databaseName, CodecRegistry codecRegistry);
    protected abstract boolean initMongoCollectionBuckets(String collectionName);
    protected abstract boolean createMongoIndexBuckets(Bson fieldNamesBson);
    protected abstract boolean initMongoCollectionRequestStatus(String collectionName);
    protected abstract boolean createMongoIndexRequestStatus(Bson fieldNamesBson);

    private static final Logger LOGGER = LogManager.getLogger();

    // constants
    public static final String MONGO_DATABASE_NAME = "dp";
    public static final String COLLECTION_NAME_BUCKETS = "buckets";
    public static final String COLLECTION_NAME_REQUEST_STATUS = "requestStatus";

    // configuration
    public static final int DEFAULT_NUM_WORKERS = 7;
    public static final String CFG_KEY_DB_HOST = "MongoClient.dbHost";
    public static final String DEFAULT_DB_HOST = "localhost";
    public static final String CFG_KEY_DB_PORT = "MongoClient.dbPort";
    public static final int DEFAULT_DB_PORT = 27017;
    public static final String CFG_KEY_DB_USER = "MongoClient.dbUser";
    public static final String DEFAULT_DB_USER = "admin";
    public static final String CFG_KEY_DB_PASSWORD = "MongoClient.dbPassword";
    public static final String DEFAULT_DB_PASSWORD = "admin";

    protected static ConfigurationManager configMgr() {
        return ConfigurationManager.getInstance();
    }

    protected CodecRegistry getPojoCodecRegistry() {
        // set up mongo codec registry for handling pojos automatically
        // create mongo codecs for model classes
//        CodecProvider pojoCodecProvider = PojoCodecProvider.builder().register(TsDataBucket.class, DatumModel.class).build();
        String packageName = BucketDocument.class.getPackageName();
        CodecProvider pojoCodecProvider = PojoCodecProvider.builder().register(packageName).build();
        //        CodecProvider pojoCodecProvider = PojoCodecProvider.builder().automatic(true).build();
        CodecRegistry pojoCodecRegistry =
                fromRegistries(getDefaultCodecRegistry(), fromProviders(pojoCodecProvider));
        return pojoCodecRegistry;
    }

    private boolean createMongoIndexesBuckets() {
        createMongoIndexBuckets(Indexes.ascending(
                BsonConstants.BSON_KEY_BUCKET_NAME));
        createMongoIndexBuckets(Indexes.ascending(
                BsonConstants.BSON_KEY_BUCKET_NAME, BsonConstants.BSON_KEY_BUCKET_FIRST_TIME));
        createMongoIndexBuckets(Indexes.ascending(
                BsonConstants.BSON_KEY_BUCKET_NAME, BsonConstants.BSON_KEY_BUCKET_FIRST_TIME_SECS, BsonConstants.BSON_KEY_BUCKET_FIRST_TIME_NANOS));
        createMongoIndexBuckets(Indexes.ascending(
                BsonConstants.BSON_KEY_BUCKET_NAME, BsonConstants.BSON_KEY_BUCKET_LAST_TIME));
        createMongoIndexBuckets(Indexes.ascending(
                BsonConstants.BSON_KEY_BUCKET_NAME, BsonConstants.BSON_KEY_BUCKET_LAST_TIME_SECS, BsonConstants.BSON_KEY_BUCKET_LAST_TIME_NANOS));
        return true;
    }

    private boolean createMongoIndexesRequestStatus() {
        createMongoIndexRequestStatus(Indexes.ascending(
                BsonConstants.BSON_KEY_REQ_STATUS_PROVIDER_ID, BsonConstants.BSON_KEY_REQ_STATUS_REQUEST_ID));
        createMongoIndexRequestStatus(Indexes.ascending(
                BsonConstants.BSON_KEY_REQ_STATUS_PROVIDER_ID, BsonConstants.BSON_KEY_REQ_STATUS_TIME));
        return true;
    }

    public static String getMongoConnectString() {

        // mongodb://admin:admin@localhost:27017/

        String dbHost = configMgr().getConfigString(CFG_KEY_DB_HOST, DEFAULT_DB_HOST);
        Integer dbPort = configMgr().getConfigInteger(CFG_KEY_DB_PORT, DEFAULT_DB_PORT);
        String dbUser = configMgr().getConfigString(CFG_KEY_DB_USER, DEFAULT_DB_USER);
        String dbPassword = configMgr().getConfigString(CFG_KEY_DB_PASSWORD, DEFAULT_DB_PASSWORD);

        String connectString = "mongodb://" + dbUser + ":" + dbPassword + "@" + dbHost + ":" + dbPort + "/";

        return connectString;
    }

    protected String getMongoDatabaseName() {
        return MONGO_DATABASE_NAME;
    }

    protected String getCollectionNameBuckets() {
        return COLLECTION_NAME_BUCKETS;
    }

    protected String getCollectionNameRequestStatus() {
        return COLLECTION_NAME_REQUEST_STATUS;
    }

    public boolean init() {

        LOGGER.debug("init");

        String connectString = getMongoConnectString();
        String databaseName = getMongoDatabaseName();
        String collectionNameBuckets = getCollectionNameBuckets();
        String collectionNameRequestStatus = getCollectionNameRequestStatus();
        LOGGER.info("init connectString: {} databaseName: {}", connectString, databaseName);
        LOGGER.info("init collection names buckets: {} requestStatus: {}", collectionNameBuckets, collectionNameRequestStatus);

        // connect mongo client
        initMongoClient(connectString);

        // connect to database
        initMongoDatabase(databaseName, getPojoCodecRegistry());

        // initialize buckets collection
        initMongoCollectionBuckets(collectionNameBuckets);
        createMongoIndexesBuckets();

        // initialize request status collection
        initMongoCollectionRequestStatus(collectionNameRequestStatus);
        createMongoIndexesRequestStatus();

        return true;
    }

    public boolean fini() {
        LOGGER.info("fini");
        return true;
    }
}
