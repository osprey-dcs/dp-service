package com.ospreydcs.dp.service.ingest.handler.mongo;

import com.mongodb.client.result.InsertManyResult;
import com.mongodb.client.result.InsertOneResult;
import com.ospreydcs.dp.common.config.ConfigurationManager;
import com.ospreydcs.dp.grpc.v1.common.Attribute;
import com.ospreydcs.dp.grpc.v1.common.DataColumn;
import com.ospreydcs.dp.grpc.v1.common.DataValue;
import com.ospreydcs.dp.grpc.v1.common.Timestamp;
import com.ospreydcs.dp.grpc.v1.ingestion.IngestionRequest;
import com.ospreydcs.dp.service.common.bson.*;
import com.ospreydcs.dp.service.common.grpc.GrpcUtility;
import com.ospreydcs.dp.service.common.mongo.MongoClientBase;
import com.ospreydcs.dp.service.ingest.handler.IngestionHandlerBase;
import com.ospreydcs.dp.service.ingest.handler.IngestionHandlerInterface;
import com.ospreydcs.dp.service.ingest.handler.model.HandlerIngestionRequest;
import com.ospreydcs.dp.service.ingest.handler.model.HandlerIngestionResult;
import com.ospreydcs.dp.service.ingest.model.DataTimeSpecModel;
import com.ospreydcs.dp.service.ingest.model.DpIngestionException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;

public class MongoIngestionHandler extends IngestionHandlerBase implements IngestionHandlerInterface {

    private static final Logger LOGGER = LogManager.getLogger();

    // constants
    private static final int TIMEOUT_SECONDS = 60;
    protected static final int MAX_QUEUE_SIZE = 1;
    protected static final int POLL_TIMEOUT_SECONDS = 1;

    // configuration
    public static final String CFG_KEY_NUM_WORKERS = "IngestionHandler.numWorkers";
    public static final int DEFAULT_NUM_WORKERS = 7;

    final private MongoIngestionClientInterface mongoIngestionClientInterface;

    protected ExecutorService executorService = null;
    protected BlockingQueue<HandlerIngestionRequest> requestQueue =
            new LinkedBlockingQueue<>(MAX_QUEUE_SIZE);
    private final AtomicBoolean shutdownRequested = new AtomicBoolean(false);

    public MongoIngestionHandler(MongoIngestionClientInterface clientInterface) {
        this.mongoIngestionClientInterface = clientInterface;
    }

    public static MongoIngestionHandler newMongoSyncIngestionHandler() {
        return new MongoIngestionHandler(new MongoSyncIngestionClient());
    }

    public static MongoIngestionHandler newMongoAsyncIngestionHandler() {
        return new MongoIngestionHandler(new MongoAsyncIngestionClient());
    }

    private static ConfigurationManager configMgr() {
        return ConfigurationManager.getInstance();
    }

    public static class IngestionTaskResult {

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

                    // poll for next queue item with a timeout
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

            } catch (InterruptedException ex) {
                LOGGER.error("InterruptedException in IngestionWorker.run");
                Thread.currentThread().interrupt();
            }
        }

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

    /**
     * Initializes handler. Creates ExecutorService with fixed thread pool.
     *
     * @return
     */
    @Override
    public boolean init() {

        LOGGER.debug("init");

        if (!mongoIngestionClientInterface.init()) {
            LOGGER.error("error in mongoIngestionClientInterface.init()");
            return false;
        }

        int numWorkers = configMgr().getConfigInteger(CFG_KEY_NUM_WORKERS, DEFAULT_NUM_WORKERS);
        LOGGER.info("init numWorkers: {}", numWorkers);

        // init ExecutorService
        executorService = Executors.newFixedThreadPool(numWorkers);

        for (int i = 1 ; i <= numWorkers ; i++) {
            IngestionWorker worker = new IngestionWorker(requestQueue);
            executorService.execute(worker);
        }

        // add a JVM shutdown hook just in case
        final Thread shutdownHook = new Thread(() -> this.fini());
        Runtime.getRuntime().addShutdownHook(shutdownHook);

        return true;
    }

    /**
     * Cleans up handler.  Shuts down ExecutorService.
     *
     * @return
     */
    @Override
    public boolean fini() {

        if (shutdownRequested.get()) {
            return true;
        }

        shutdownRequested.set(true);

        LOGGER.debug("fini");

        // shut down executor service
        try {
            LOGGER.debug("shutting down executorService");
            executorService.shutdown();
            executorService.awaitTermination(TIMEOUT_SECONDS, TimeUnit.SECONDS);
            LOGGER.debug("executorService shutdown completed");
        } catch (InterruptedException ex) {
            executorService.shutdownNow();
            LOGGER.error("InterruptedException in executorService.shutdown: " + ex.getMessage());
            Thread.currentThread().interrupt();
        }

        MongoClientBase mongoClient = (MongoClientBase) mongoIngestionClientInterface;
        if (!mongoClient.fini()) {
            LOGGER.error("error in mongoIngestionClientInterface.fini()");
        }

        LOGGER.debug("fini shutdown completed");

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

        String status = BsonConstants.BSON_VALUE_STATUS_SUCCESS;
        boolean isError = false;
        String errorMsg = "";
        List<String> idsCreated = new ArrayList<>();

        if (handlerIngestionRequest.rejected) {
            // request already rejected, but we want to add details in request status
            isError = true;
            errorMsg = handlerIngestionRequest.rejectMsg;
            status = BsonConstants.BSON_VALUE_STATUS_REJECTED;

        } else {

            // generate batch of bucket documents for request
            List<BucketDocument> dataDocumentBatch = null;
            try {
                dataDocumentBatch = generateBucketsFromRequest(request);
            } catch (DpIngestionException e) {
                isError = true;
                errorMsg = e.getMessage();
                status = BsonConstants.BSON_VALUE_STATUS_ERROR;
            }

            if (dataDocumentBatch != null) {
                // add the batch to mongo and handle result
                IngestionTaskResult ingestionTaskResult =
                        mongoIngestionClientInterface.insertBatch(request, dataDocumentBatch);

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
                    status = BsonConstants.BSON_VALUE_STATUS_ERROR;
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
        InsertOneResult insertRequestStatusResult = mongoIngestionClientInterface.insertRequestStatus(statusDocument);
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

        LOGGER.debug("handleIngestionRequest");

        try {
            requestQueue.put(handlerIngestionRequest);
        } catch (InterruptedException e) {
            LOGGER.error("InterruptedException waiting for requestQueue.put");
            Thread.currentThread().interrupt();
        }
    }

}
