package com.ospreydcs.dp.service.ingest.handler.mongo;

import com.mongodb.client.model.Indexes;
import com.mongodb.client.result.InsertManyResult;
import com.mongodb.client.result.InsertOneResult;
import com.ospreydcs.dp.common.config.ConfigurationManager;
import com.ospreydcs.dp.grpc.v1.common.*;
import com.ospreydcs.dp.grpc.v1.ingestion.IngestionRequest;
import com.ospreydcs.dp.service.common.bson.*;
import com.ospreydcs.dp.service.common.grpc.GrpcUtility;
import com.ospreydcs.dp.service.ingest.handler.model.HandlerIngestionRequest;
import com.ospreydcs.dp.service.ingest.handler.IngestionHandlerBase;
import com.ospreydcs.dp.service.ingest.handler.IngestionHandlerInterface;
import com.ospreydcs.dp.service.ingest.handler.model.HandlerIngestionResult;
import com.ospreydcs.dp.service.ingest.model.DataTimeSpecModel;
import com.ospreydcs.dp.service.ingest.model.DpIngestionException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.bson.codecs.configuration.CodecProvider;
import org.bson.codecs.configuration.CodecRegistry;
import org.bson.codecs.pojo.PojoCodecProvider;
import org.bson.conversions.Bson;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.mongodb.MongoClientSettings.getDefaultCodecRegistry;
import static org.bson.codecs.configuration.CodecRegistries.fromProviders;
import static org.bson.codecs.configuration.CodecRegistries.fromRegistries;

public abstract class MongoHandlerBase extends IngestionHandlerBase implements IngestionHandlerInterface {

    // abstract methods
    protected abstract boolean initMongoClient(String connectString);
    protected abstract boolean initMongoDatabase(String databaseName, CodecRegistry codecRegistry);
    protected abstract boolean initMongoCollectionBuckets(String collectionName);
    protected abstract boolean createMongoIndexBuckets(Bson fieldNamesBson);
    protected abstract boolean initMongoCollectionRequestStatus(String collectionName);
    protected abstract boolean createMongoIndexRequestStatus(Bson fieldNamesBson);
    protected abstract IngestionTaskResult insertBatch(
            IngestionRequest request, List<BucketDocument> dataDocumentBatch);
    protected abstract InsertOneResult insertRequestStatus(RequestStatusDocument requestStatusDocument);

    private static final Logger LOGGER = LogManager.getLogger();

    // constants
    private static final int TIMEOUT_SECONDS = 60;
    protected static final int MAX_INGESTION_QUEUE_SIZE = 1;
    protected static final int POLL_TIMEOUT_SECONDS = 1;
    public static final String MONGO_DATABASE_NAME = "dp";
    protected static final String COLLECTION_NAME_BUCKETS = "buckets";
    protected static final String COLLECTION_NAME_REQUEST_STATUS = "requestStatus";

    // configuration
    public static final String CFG_KEY_NUM_WORKERS = "MongoHandler.numWorkers";
    public static final int DEFAULT_NUM_WORKERS = 7;
    public static final String CFG_KEY_DB_HOST = "MongoHandler.dbHost";
    public static final String DEFAULT_DB_HOST = "localhost";
    public static final String CFG_KEY_DB_PORT = "MongoHandler.dbPort";
    public static final int DEFAULT_DB_PORT = 27017;
    public static final String CFG_KEY_DB_USER = "MongoHandler.dbUser";
    public static final String DEFAULT_DB_USER = "admin";
    public static final String CFG_KEY_DB_PASSWORD = "MongoHandler.dbPassword";
    public static final String DEFAULT_DB_PASSWORD = "admin";

    // BSON field name constants
    protected static final String BSON_KEY_BUCKET_NAME = "columnName";
    protected static final String BSON_KEY_BUCKET_FIRST_TIME = "firstDate";
    protected static final String BSON_KEY_BUCKET_FIRST_TIME_SECS = "firstSeconds";
    protected static final String BSON_KEY_BUCKET_FIRST_TIME_NANOS = "firstNanos";
    protected static final String BSON_KEY_BUCKET_LAST_TIME = "lastDate";
    protected static final String BSON_KEY_BUCKET_LAST_TIME_SECS = "lastSeconds";
    protected static final String BSON_KEY_BUCKET_LAST_TIME_NANOS = "lastNanos";
    protected static final String BSON_KEY_REQ_STATUS_PROVIDER_ID = "providerId";
    protected static final String BSON_KEY_REQ_STATUS_REQUEST_ID = "requestId";
    protected static final String BSON_KEY_REQ_STATUS_TIME = "updateTime";
    public static final String BSON_VALUE_STATUS_SUCCESS = "success";
    public static final String BSON_VALUE_STATUS_REJECTED = "rejected";
    public static final String BSON_VALUE_STATUS_ERROR = "error";

    protected ExecutorService executorService = null;
    protected BlockingQueue<HandlerIngestionRequest> ingestionQueue =
            new LinkedBlockingQueue<>(MAX_INGESTION_QUEUE_SIZE);
    private final AtomicBoolean shutdownRequested = new AtomicBoolean(false);

    protected class IngestionTaskResult {

        private boolean isError = false;
        private String msg = null;
        private InsertManyResult insertManyResult = null;

        public IngestionTaskResult(boolean isError, String msg, InsertManyResult insertManyResult) {
            this.isError = isError;
            this.msg = msg;
            this.insertManyResult = insertManyResult;
        }

     }

    private class IngestionWorker implements Runnable {

        private final BlockingQueue queue;

        public IngestionWorker(BlockingQueue q) {
            this.queue = q;
        }

        public void run() {

            try {
                while (!Thread.currentThread().isInterrupted() && !shutdownRequested.get()) {

//                    // block while waiting for a queue element
//                    HandlerIngestionRequest handlerIngestionRequest = (HandlerIngestionRequest) queue.take();

                    // pool for next queue item with a timeout
                    HandlerIngestionRequest handlerIngestionRequest =
                            (HandlerIngestionRequest) queue.poll(POLL_TIMEOUT_SECONDS, TimeUnit.SECONDS);

                    if (handlerIngestionRequest != null) {
                        try {
                            handleIngestionRequest(handlerIngestionRequest);
                        } catch (Exception ex) {
                            LOGGER.error("IngestionWorker.run encountered exception: {}", ex.getMessage());
                        }
                    }
                }

                LOGGER.debug("IngestionWorker shutting down");
                System.err.println("IngestionWorker shutting down");

            } catch (InterruptedException ex) {
                LOGGER.error("InterruptedException in IngestionWorker.run");
                Thread.currentThread().interrupt();
            }
        }

    }

    private static ConfigurationManager configMgr() {
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

    /**
     * Generates a list of POJO objects, which are written as a batch to mongodb by customizing the codec registry.
     *
     * NOTE: DATABASE CODE LIKE insertMany SILENTLY FAILS IF AN INSTANCE VARIABLE IS ADDED TO TsDataBucket
     * WITHOUT ACCESSOR METHODS!!!  Very hard to troubleshoot.
     *
     * @param request
     * @return
     */
    protected static List<BucketDocument> generateBucketsFromRequest(IngestionRequest request)
            throws DpIngestionException {

        final List<BucketDocument> bucketList = new ArrayList<>();

        // get timestamp details
        DataTimeSpecModel timeSpecModel = new DataTimeSpecModel(request.getDataTable().getDataTimeSpec());
        final Timestamp firstTimestamp = timeSpecModel.getFirstTimestamp();
        final long firstTimestampSeconds = firstTimestamp.getEpochSeconds();
        final long firstTimestampNanos = firstTimestamp.getNanoseconds();
        final Date firstTimestampDate = GrpcUtility.dateFromTimestamp(firstTimestamp);
        final Timestamp lastTimestamp = timeSpecModel.getLastTimestamp();
        final long lastTimestampSeconds = lastTimestamp.getEpochSeconds();
        final long lastTimestampNanos = lastTimestamp.getNanoseconds();
        final Date lastTimestampDate = GrpcUtility.dateFromTimestamp(lastTimestamp);

        // create BSON document for each column
        final List<DataColumn> columns = request.getDataTable().getDataColumnsList();
        for (DataColumn column : columns) {
//            final BucketDocument bucket = new BucketDocument(); // ALL NEW FIELDS MUST HAVE ACCESSOR METHODS OR CODEC SILENTLY FAILS!
            final String columnName = column.getName();
            final String documentId = columnName + "-" + firstTimestampSeconds + "-" + firstTimestampNanos;

            // determine data type for column, create appropriate bucket subtype and add column data to it
            // PS this code is ugly, but we need the switch statement to create the correct subtype and
            // to know what method to call on datum to get the value.  The code below could be moved to other
            // places (like utility method on bucket subtype) but in the end it would need to do the same thing.
            BucketDocument bucket = null;
            boolean first = true;
            DataValue.ValueOneofCase columnDataType = null;
            for (DataValue datum : column.getDataValuesList()) {

                // use data type of first element to set expected data type for column
                if (first) {
                    columnDataType = datum.getValueOneofCase();
                } else {
                    if (datum.getValueOneofCase() != columnDataType) {
                        String errorMsg = "provider: " + request.getProviderId()
                                + " request: " + request.getClientRequestId()
                                + " column: " + columnName
                                + " data type mismatch: " + datum.getValueOneofCase().name()
                                + " expected: " + columnDataType.name();
                        LOGGER.debug(errorMsg);
                        throw new DpIngestionException(errorMsg);
                    }
                }

                boolean unhandledDataType = false;
                switch (datum.getValueOneofCase()) {

                    case STRINGVALUE -> {
                        if (first) {
                            bucket = new StringBucketDocument();
                            bucket.initColumnDataList();
                        }
//                        ((StringBucketDocument) bucket).addColumnData(datum.getStringValue());
                        bucket.addColumnData(datum.getStringValue());
                    }
                    case FLOATVALUE -> {
                        if (first) {
                            bucket = new DoubleBucketDocument();
                            bucket.initColumnDataList();
                        }
                        bucket.addColumnData(datum.getFloatValue());
                    }
                    case INTVALUE -> {
                        if (first) {
                            bucket = new LongBucketDocument();
                            bucket.initColumnDataList();
                        }
                        bucket.addColumnData(datum.getIntValue());
                    }
                    case BOOLEANVALUE -> {
                        if (first) {
                            bucket = new BooleanBucketDocument();
                            bucket.initColumnDataList();
                        }
                        bucket.addColumnData(datum.getBooleanValue());
                    }
                    case ARRAYVALUE -> {
                        unhandledDataType = true;
                    }
                    case BYTEARRAYVALUE -> {
                        unhandledDataType = true;
                    }
                    case IMAGE -> {
                        unhandledDataType = true;
                    }
                    case STRUCTUREVALUE -> {
                        unhandledDataType = true;
                    }
                    case VALUEONEOF_NOT_SET -> {
                        String errorMsg = "provider: " + request.getProviderId()
                                + " request: " + request.getClientRequestId()
                                + " column: " + columnName
                                + " data type not specified (DataValue.valueOneof)";
                        LOGGER.debug(errorMsg);
                        throw new DpIngestionException(errorMsg);
                    }
                }
                if (unhandledDataType) {
                    String errorMsg = "provider: " + request.getProviderId()
                            + " request: " + request.getClientRequestId()
                            + " column: " + columnName
                            + " unhandled data type: " + columnDataType.name();
                    LOGGER.debug(errorMsg);
                    throw new DpIngestionException(errorMsg);
                }

                first = false;
            }

            bucket.setId(documentId);
            bucket.setColumnName(columnName);
            bucket.setFirstTime(firstTimestampDate);
            bucket.setFirstSeconds(firstTimestampSeconds);
            bucket.setFirstNanos(firstTimestampNanos);
            bucket.setLastTime(lastTimestampDate);
            bucket.setLastSeconds(lastTimestampSeconds);
            bucket.setLastNanos(lastTimestampNanos);
            bucket.setSampleFrequency(timeSpecModel.getSampleFrequency());
            bucket.setNumSamples(timeSpecModel.getNumSamples());

            // add metadata
            Map<String, String> attributeMap = new TreeMap<>();
            String eventDescription = "";
            long eventSeconds = 0;
            long eventNanos = 0;
            for (Attribute attribute : request.getAttributesList()) {
                attributeMap.put(attribute.getName(), attribute.getValue());
            }
            if (request.hasEventMetadata()) {
                if (request.getEventMetadata().getEventDescription() != null) {
                    eventDescription = request.getEventMetadata().getEventDescription();
                }
                if (request.getEventMetadata().hasEventTimestamp()) {
                    eventSeconds = request.getEventMetadata().getEventTimestamp().getEpochSeconds();
                    eventNanos = request.getEventMetadata().getEventTimestamp().getNanoseconds();
                }
            }
            bucket.setAttributeMap(attributeMap);
            bucket.setEventDescription(eventDescription);
            bucket.setEventSeconds(eventSeconds);
            bucket.setEventNanos(eventNanos);

            bucketList.add(bucket);
        }

        return bucketList;
    }

    private boolean createMongoIndexesBuckets() {
        createMongoIndexBuckets(Indexes.ascending(
                BSON_KEY_BUCKET_NAME));
        createMongoIndexBuckets(Indexes.ascending(
                BSON_KEY_BUCKET_NAME, BSON_KEY_BUCKET_FIRST_TIME));
        createMongoIndexBuckets(Indexes.ascending(
                BSON_KEY_BUCKET_NAME, BSON_KEY_BUCKET_FIRST_TIME_SECS, BSON_KEY_BUCKET_FIRST_TIME_NANOS));
        createMongoIndexBuckets(Indexes.ascending(
                BSON_KEY_BUCKET_NAME, BSON_KEY_BUCKET_LAST_TIME));
        createMongoIndexBuckets(Indexes.ascending(
                BSON_KEY_BUCKET_NAME, BSON_KEY_BUCKET_LAST_TIME_SECS, BSON_KEY_BUCKET_LAST_TIME_NANOS));
        return true;
    }

    private boolean createMongoIndexesRequestStatus() {
        createMongoIndexRequestStatus(Indexes.ascending(
                BSON_KEY_REQ_STATUS_PROVIDER_ID, BSON_KEY_REQ_STATUS_REQUEST_ID));
        createMongoIndexRequestStatus(Indexes.ascending(
                BSON_KEY_REQ_STATUS_PROVIDER_ID, BSON_KEY_REQ_STATUS_TIME));
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

    /**
     * Initializes handler. Creates ExecutorService with fixed thread pool.
     *
     * @return
     */
    public boolean init() {

        LOGGER.debug("init");

        int numWorkers = configMgr().getConfigInteger(CFG_KEY_NUM_WORKERS, DEFAULT_NUM_WORKERS);
        LOGGER.info("init numWorkers: {}", numWorkers);

        // init ExecutorService
        executorService = Executors.newFixedThreadPool(numWorkers);

        for (int i = 1 ; i <= numWorkers ; i++) {
            IngestionWorker worker = new IngestionWorker(ingestionQueue);
            executorService.execute(worker);
        }

        // add a JVM shutdown hook just in case
        final Thread shutdownHook =
                new Thread(() -> this.fini());
        Runtime.getRuntime().addShutdownHook(shutdownHook);

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

    /**
     * Cleans up handler.  Shuts down ExecutorService.
     *
     * @return
     */
    public boolean fini() {

        if (shutdownRequested.get()) {
            return true;
        }

        shutdownRequested.set(true);

        LOGGER.info("MongoHandlerBase.fini");
        System.err.println("MongoHandlerBase.fini");

        // shut down executor service
        try {
            LOGGER.debug("shutting down executorService");
            System.err.println("shutting down executorService");
            executorService.shutdown();
            executorService.awaitTermination(TIMEOUT_SECONDS, TimeUnit.SECONDS);
            LOGGER.debug("executorService shutdown completed");
            System.err.println("executorService shutdown completed");
        } catch (InterruptedException ex) {
            executorService.shutdownNow();
            LOGGER.error("InterruptedException in executorService.shutdown: " + ex.getMessage());
            System.err.println("InterruptedException in executorService.shutdown: " + ex.getMessage());
            Thread.currentThread().interrupt();
        }

        LOGGER.info("MongoHandlerBase.fini shutdown completed");
        System.err.println("MongoHandlerBase.fini shutdown completed");

        return true;
    }

    @Override
    public boolean start() {
        return true;
    }

    @Override
    public boolean stop() {
        return true;
    }

    protected HandlerIngestionResult handleIngestionRequest(HandlerIngestionRequest handlerIngestionRequest) {

        final IngestionRequest request = handlerIngestionRequest.request;
        LOGGER.debug("IngestionWorker.run handling request providerId: {} requestId: {}",
                request.getProviderId(), request.getClientRequestId());

        String status = BSON_VALUE_STATUS_SUCCESS;
        boolean isError = false;
        String errorMsg = "";
        List<String> idsCreated = new ArrayList<>();

        if (handlerIngestionRequest.rejected) {
            // request already rejected, but we want to add details in request status
            isError = true;
            errorMsg = handlerIngestionRequest.rejectMsg;
            status = BSON_VALUE_STATUS_REJECTED;

        } else {

            // generate batch of bucket documents for request
            List<BucketDocument> dataDocumentBatch = null;
            try {
                dataDocumentBatch = generateBucketsFromRequest(request);
            } catch (DpIngestionException e) {
                isError = true;
                errorMsg = e.getMessage();
                status = BSON_VALUE_STATUS_ERROR;
            }

            if (dataDocumentBatch != null) {
                // add the batch to mongo and handle result
                IngestionTaskResult ingestionTaskResult = insertBatch(request, dataDocumentBatch);

                if (ingestionTaskResult.isError) {
                    isError = true;
                    errorMsg = ingestionTaskResult.msg;
                    LOGGER.error(errorMsg);

                } else {

                    InsertManyResult insertManyResult = ingestionTaskResult.insertManyResult;

                    if (!insertManyResult.wasAcknowledged()) {
                        // check mongo insertMany result was acknowledged
                        isError = true;
                        errorMsg = "insertMany result not acknowledged";
                        LOGGER.error(errorMsg);

                    } else {

                        long recordsInsertedCount = insertManyResult.getInsertedIds().size();
                        long recordsExpected = request.getDataTable().getDataColumnsList().size();
                        if (recordsInsertedCount != recordsExpected) {
                            // check records inserted matches expected
                            isError = true;
                            errorMsg = "insertMany actual records inserted: "
                                    + recordsInsertedCount + " mismatch expected: " + recordsExpected;
                            LOGGER.error(errorMsg);

                        } else {
                            // get list of ids created
                            for (var entry : insertManyResult.getInsertedIds().entrySet()) {
                                idsCreated.add(entry.getValue().asString().getValue());
                            }
                        }
                    }
                }

                if (isError) {
                    status = BSON_VALUE_STATUS_ERROR;
                }
            }
        }

        // save request status and check result of insert operation
        RequestStatusDocument statusDocument = new RequestStatusDocument(
                request.getProviderId(),
                request.getClientRequestId(),
                status,
                errorMsg,
                idsCreated);
        InsertOneResult insertRequestStatusResult = insertRequestStatus(statusDocument);
        if (insertRequestStatusResult == null) {
            LOGGER.error("error inserting request status");
        } else {
            if (!insertRequestStatusResult.wasAcknowledged()) {
                LOGGER.error("insertOne not acknowledged inserting request status");
            } else {
                LOGGER.debug("inserted request status id:" + insertRequestStatusResult.getInsertedId());
            }
        }

        return new HandlerIngestionResult(isError, errorMsg);
    }

    public void onNext(HandlerIngestionRequest handlerIngestionRequest) {

        LOGGER.debug("MongoDbHandlerBase.handleIngestionRequest");

        try {
            ingestionQueue.put(handlerIngestionRequest);
        } catch (InterruptedException e) {
            LOGGER.error("InterruptedException waiting for ingestionQueue.put");
            Thread.currentThread().interrupt();
        }
    }

}
