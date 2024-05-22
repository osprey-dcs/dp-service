package com.ospreydcs.dp.service.common.mongo;

import com.mongodb.client.model.Indexes;
import com.ospreydcs.dp.grpc.v1.common.DataValue;
import com.ospreydcs.dp.service.common.bson.annotation.AnnotationDocument;
import com.ospreydcs.dp.service.common.bson.annotation.CommentAnnotationDocument;
import com.ospreydcs.dp.service.common.bson.dataset.DocumentDataBlock;
import com.ospreydcs.dp.service.common.bson.dataset.DataSetDocument;
import com.ospreydcs.dp.service.common.bson.bucket.*;
import com.ospreydcs.dp.service.common.config.ConfigurationManager;
import com.ospreydcs.dp.service.common.bson.*;
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

    // static variables
    private static final Logger logger = LogManager.getLogger();
    private static String mongoDatabaseName = null;

    // constants
    public static final String MONGO_DATABASE_NAME = "dp";
    public static final String COLLECTION_NAME_BUCKETS = "buckets";
    public static final String COLLECTION_NAME_REQUEST_STATUS = "requestStatus";
    public static final String COLLECTION_NAME_DATA_SETS = "dataSets";
    public static final String COLLECTION_NAME_ANNOTATIONS = "annotations";

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

    // abstract methods
    protected abstract boolean initMongoClient(String connectString);
    protected abstract boolean initMongoDatabase(String databaseName, CodecRegistry codecRegistry);
    protected abstract boolean initMongoCollectionBuckets(String collectionName);
    protected abstract boolean createMongoIndexBuckets(Bson fieldNamesBson);
    protected abstract boolean initMongoCollectionRequestStatus(String collectionName);
    protected abstract boolean createMongoIndexRequestStatus(Bson fieldNamesBson);
    protected abstract boolean initMongoCollectionDataSets(String collectionName);
    protected abstract boolean createMongoIndexDataSets(Bson fieldNamesBson);
    protected abstract boolean initMongoCollectionAnnotations(String collectionName);
    protected abstract boolean createMongoIndexAnnotations(Bson fieldNamesBson);

    protected static ConfigurationManager configMgr() {
        return ConfigurationManager.getInstance();
    }

    protected CodecRegistry getPojoCodecRegistry() {

        // set up mongo codec registry for handling pojos automatically
        // create mongo codecs for model classes

//        CodecProvider pojoCodecProvider = PojoCodecProvider.builder().register(TsDataBucket.class, DatumModel.class).build();

        // Registration by packageName led to an exception in the query service when iterating result cursor,
        // see details below about registrering classes explicitly.
//        String packageName = BucketDocument.class.getPackageName();
//        LOGGER.trace("CodecProvider registering packageName: " + packageName);
//        CodecProvider pojoCodecProvider = PojoCodecProvider.builder().register(packageName).build();

        // Was registering POJO classes with CodecProvider by packageName as shown above, but this doesn't work
        // when using find with a cursor.  I got an exception "Decoding errored with: A class could not be found for the discriminator: DOUBLE"
        // "A custom Codec or PojoCodec may need to be explicitly configured and registered to handle this type."
        // Indeed, registering the classes explicitly solved that problem but sort of a bummer because any new ones must
        // be explicitly registered here.
        CodecProvider pojoCodecProvider = PojoCodecProvider.builder().register(
                BucketDocument.class,
                DataValue.class,
                RequestStatusDocument.class,
                AnnotationDocument.class,
                CommentAnnotationDocument.class,
                DataSetDocument.class,
                DocumentDataBlock.class
        ).build();

        //        CodecProvider pojoCodecProvider = PojoCodecProvider.builder().automatic(true).build();

        CodecRegistry pojoCodecRegistry =
                fromRegistries(getDefaultCodecRegistry(), fromProviders(pojoCodecProvider));
        return pojoCodecRegistry;
    }

    private boolean createMongoIndexesBuckets() {
        createMongoIndexBuckets(Indexes.ascending(
                BsonConstants.BSON_KEY_BUCKET_NAME));
        createMongoIndexBuckets(Indexes.ascending(
                BsonConstants.BSON_KEY_BUCKET_NAME,
                BsonConstants.BSON_KEY_BUCKET_FIRST_TIME));
        createMongoIndexBuckets(Indexes.ascending(
                BsonConstants.BSON_KEY_BUCKET_NAME,
                BsonConstants.BSON_KEY_BUCKET_FIRST_TIME_SECS,
                BsonConstants.BSON_KEY_BUCKET_FIRST_TIME_NANOS));
        createMongoIndexBuckets(Indexes.ascending(
                BsonConstants.BSON_KEY_BUCKET_NAME,
                BsonConstants.BSON_KEY_BUCKET_LAST_TIME));
        createMongoIndexBuckets(Indexes.ascending(
                BsonConstants.BSON_KEY_BUCKET_NAME,
                BsonConstants.BSON_KEY_BUCKET_LAST_TIME_SECS,
                BsonConstants.BSON_KEY_BUCKET_LAST_TIME_NANOS));
        return true;
    }

    private boolean createMongoIndexesRequestStatus() {
        createMongoIndexRequestStatus(Indexes.ascending(
                BsonConstants.BSON_KEY_REQ_STATUS_PROVIDER_ID,
                BsonConstants.BSON_KEY_REQ_STATUS_REQUEST_ID));
        createMongoIndexRequestStatus(Indexes.ascending(
                BsonConstants.BSON_KEY_REQ_STATUS_PROVIDER_ID,
                BsonConstants.BSON_KEY_REQ_STATUS_TIME));
        return true;
    }

    private boolean createMongoIndexesDataSets() {
        createMongoIndexDataSets(Indexes.text(BsonConstants.BSON_KEY_DATA_SET_DESCRIPTION)); // text index on comment field
        return true;
    }

    private boolean createMongoIndexesAnnotations() {
        createMongoIndexAnnotations(Indexes.ascending(BsonConstants.BSON_KEY_ANNOTATION_TYPE));
        createMongoIndexAnnotations(Indexes.text(BsonConstants.BSON_KEY_ANNOTATION_COMMENT)); // text index on comment field
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

    protected static String getMongoDatabaseName() {
        if (mongoDatabaseName == null) {
            return MONGO_DATABASE_NAME;
        } else {
            return mongoDatabaseName;
        }
    }

    protected static void setMongoDatabaseName(String databaseName) {
        if (databaseName.isBlank()) {
            logger.error("setDatabaseName specified database name is empty");
        } else {
            mongoDatabaseName = databaseName;
        }
    }

    protected String getCollectionNameBuckets() {
        return COLLECTION_NAME_BUCKETS;
    }

    protected String getCollectionNameRequestStatus() {
        return COLLECTION_NAME_REQUEST_STATUS;
    }

    protected String getCollectionNameDataSets() {
        return COLLECTION_NAME_DATA_SETS;
    }

    protected String getCollectionNameAnnotations() {
        return COLLECTION_NAME_ANNOTATIONS;
    }

    public boolean init() {

        logger.trace("init");

        String connectString = getMongoConnectString();
        String databaseName = getMongoDatabaseName();
        String collectionNameBuckets = getCollectionNameBuckets();
        String collectionNameRequestStatus = getCollectionNameRequestStatus();
        String collectionNameDataSets = getCollectionNameDataSets();
        String collectionNameAnnotations = getCollectionNameAnnotations();
        logger.info("mongo client init connectString: {} databaseName: {}", connectString, databaseName);
        logger.info("mongo client init collection names buckets: {} requestStatus: {} annotations: {}",
                collectionNameBuckets, collectionNameRequestStatus, collectionNameAnnotations);

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

        // initialize datasets collection
        initMongoCollectionDataSets(collectionNameDataSets);
        createMongoIndexesDataSets();

        // initialize annotations collection
        initMongoCollectionAnnotations(collectionNameAnnotations);
        createMongoIndexesAnnotations();

        return true;
    }

    public boolean fini() {
        logger.trace("fini");
        return true;
    }
}
