package com.ospreydcs.dp.service.query.handler.mongo;

import com.ospreydcs.dp.grpc.v1.query.*;
import com.ospreydcs.dp.service.common.config.ConfigurationManager;
import com.ospreydcs.dp.grpc.v1.common.*;
import com.ospreydcs.dp.service.common.bson.BucketDocument;
import com.ospreydcs.dp.service.common.grpc.GrpcUtility;
import com.ospreydcs.dp.service.query.handler.QueryHandlerBase;
import com.ospreydcs.dp.service.query.handler.interfaces.QueryHandlerInterface;
import com.ospreydcs.dp.service.query.handler.mongo.client.MongoQueryClientInterface;
import com.ospreydcs.dp.service.query.handler.mongo.client.MongoSyncQueryClient;
import com.ospreydcs.dp.service.query.handler.mongo.dispatch.*;
import com.ospreydcs.dp.service.query.handler.mongo.job.QueryMetadataJob;
import com.ospreydcs.dp.service.query.handler.mongo.job.HandlerJob;
import com.ospreydcs.dp.service.query.handler.mongo.job.QueryDataJob;
import io.grpc.stub.StreamObserver;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;

public class MongoQueryHandler extends QueryHandlerBase implements QueryHandlerInterface {

    private static final Logger logger = LogManager.getLogger();

    // constants
    private static final int TIMEOUT_SECONDS = 60;
    protected static final int MAX_QUEUE_SIZE = 1;
    protected static final int POLL_TIMEOUT_SECONDS = 1;
    public static final int MAX_GRPC_MESSAGE_SIZE = 4_000_000;

    // configuration
    public static final String CFG_KEY_NUM_WORKERS = "QueryHandler.numWorkers";
    public static final int DEFAULT_NUM_WORKERS = 7;

    private MongoQueryClientInterface mongoQueryClient = null;
    protected ExecutorService executorService = null;
    protected BlockingQueue<HandlerJob> requestQueue = new LinkedBlockingQueue<>(MAX_QUEUE_SIZE);
    private final AtomicBoolean shutdownRequested = new AtomicBoolean(false);

    public MongoQueryHandler(MongoQueryClientInterface clientInterface) {
        this.mongoQueryClient = clientInterface;
    }

    public static MongoQueryHandler newMongoSyncQueryHandler() {
        return new MongoQueryHandler(new MongoSyncQueryClient());
    }

    private static ConfigurationManager configMgr() {
        return ConfigurationManager.getInstance();
    }

    private class QueueWorker implements Runnable {

        private final BlockingQueue queue;

        public QueueWorker(BlockingQueue q) {
            this.queue = q;
        }
        public void run() {
            try {
                while (!Thread.currentThread().isInterrupted() && !shutdownRequested.get()) {

//                    // block while waiting for a queue element
//                    HandlerQueryRequest request = (HandlerQueryRequest) queue.take();

                    // poll for next queue item with a timeout
                    HandlerJob job =
                            (HandlerJob) queue.poll(POLL_TIMEOUT_SECONDS, TimeUnit.SECONDS);

                    if (job != null) {
                        try {
                            job.execute();
                        } catch (Exception ex) {
                            logger.error("QueryWorker.run encountered exception: {}", ex.getMessage());
                            ex.printStackTrace(System.err);
                        }
                    }
                }

                logger.trace("QueryWorker shutting down");

            } catch (InterruptedException ex) {
                logger.error("InterruptedException in QueryWorker.run");
                Thread.currentThread().interrupt();
            }
        }
    }

    public static DataTimestamps dataTimestampsForBucket(BucketDocument document) {

        final DataTimestamps.Builder dataTimestampsBuilder = DataTimestamps.newBuilder();

        // TODO: determine whether to use explicit timestamp list if BucketDocument contains non-empty list,
        // otherwise use SamplingClock as currently implemented...

        final Timestamp startTime = GrpcUtility.timestampFromSeconds(document.getFirstSeconds(), document.getFirstNanos());
        final SamplingClock.Builder samplingClockBuilder = SamplingClock.newBuilder();
        samplingClockBuilder.setStartTime(startTime);
        samplingClockBuilder.setPeriodNanos(document.getSampleFrequency());
        samplingClockBuilder.setCount(document.getNumSamples());
        samplingClockBuilder.build();

        dataTimestampsBuilder.setSamplingClock(samplingClockBuilder);
        return dataTimestampsBuilder.build();
    }

    public static <T> QueryDataResponse.QueryResult.QueryData.DataBucket dataBucketFromDocument(
            BucketDocument<T> document
    ) {
        final QueryDataResponse.QueryResult.QueryData.DataBucket.Builder bucketBuilder =
                QueryDataResponse.QueryResult.QueryData.DataBucket.newBuilder();

        bucketBuilder.setDataTimestamps(dataTimestampsForBucket(document));

        final DataColumn.Builder columnBuilder = DataColumn.newBuilder();
        columnBuilder.setName(document.getColumnName());
//        addBucketDataToColumn(document, columnBuilder);
        for (T dataValue: document.getColumnDataList()) {
            final DataValue.Builder valueBuilder = DataValue.newBuilder();
            document.addColumnDataValue(dataValue, valueBuilder);
            valueBuilder.build();
            columnBuilder.addDataValues(valueBuilder);
        }
        columnBuilder.build();
        bucketBuilder.setDataColumn(columnBuilder);

        return bucketBuilder.build();
    }

    /**
     * Initializes handler. Creates ExecutorService with fixed thread pool.
     *
     * @return
     */
    @Override
    public boolean init() {

        logger.trace("init");

        if (!mongoQueryClient.init()) {
            logger.error("error in mongoQueryClient.init()");
            return false;
        }

        int numWorkers = configMgr().getConfigInteger(CFG_KEY_NUM_WORKERS, DEFAULT_NUM_WORKERS);
        logger.info("init numWorkers: {}", numWorkers);

        // init ExecutorService
        executorService = Executors.newFixedThreadPool(numWorkers);

        for (int i = 1 ; i <= numWorkers ; i++) {
            QueueWorker worker = new QueueWorker(requestQueue);
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

        logger.trace("fini");

        // shut down executor service
        try {
            logger.trace("shutting down executorService");
            executorService.shutdown();
            executorService.awaitTermination(TIMEOUT_SECONDS, TimeUnit.SECONDS);
            logger.trace("executorService shutdown completed");
        } catch (InterruptedException ex) {
            executorService.shutdownNow();
            logger.error("InterruptedException in executorService.shutdown: " + ex.getMessage());
            Thread.currentThread().interrupt();
        }

        if (!mongoQueryClient.fini()) {
            logger.error("error in mongoQueryClient.fini()");
        }

        logger.info("fini shutdown completed");

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

    @Override
    public void handleQueryDataStream(
            QueryDataRequest.QuerySpec querySpec, StreamObserver<QueryDataResponse> responseObserver) {

        final DataResponseStreamDispatcher dispatcher = new DataResponseStreamDispatcher(responseObserver);
        final QueryDataJob job = new QueryDataJob(querySpec, dispatcher, responseObserver, mongoQueryClient);

        logger.debug("adding queryResponseStream job id: {} to queue", responseObserver.hashCode());

        try {
            requestQueue.put(job);
        } catch (InterruptedException e) {
            logger.error("InterruptedException waiting for requestQueue.put");
            Thread.currentThread().interrupt();
        }
    }

    @Override
    public QueryResultCursor handleQueryDataBidiStream(
            QueryDataRequest.QuerySpec querySpec, StreamObserver<QueryDataResponse> responseObserver) {


        final DataResponseBidiStreamDispatcher dispatcher = new DataResponseBidiStreamDispatcher(responseObserver);
        final QueryDataJob job = new QueryDataJob(querySpec, dispatcher, responseObserver, mongoQueryClient);
        final QueryResultCursor resultCursor = new QueryResultCursor(this, dispatcher);

        logger.debug("adding queryResponseCursor job id: {} to queue", responseObserver.hashCode());

        try {
            requestQueue.put(job);
        } catch (InterruptedException e) {
            logger.error("InterruptedException waiting for requestQueue.put");
            Thread.currentThread().interrupt();
        }

        return resultCursor;
    }

    @Override
    public void handleQueryData(
            QueryDataRequest.QuerySpec querySpec, StreamObserver<QueryDataResponse> responseObserver) {

        final DataResponseUnaryDispatcher dispatcher = new DataResponseUnaryDispatcher(responseObserver);
        final QueryDataJob job = new QueryDataJob(querySpec, dispatcher, responseObserver, mongoQueryClient);

        logger.debug("adding queryResponseSingle job id: {} to queue", responseObserver.hashCode());

        try {
            requestQueue.put(job);
        } catch (InterruptedException e) {
            logger.error("InterruptedException waiting for requestQueue.put");
            Thread.currentThread().interrupt();
        }
    }

    @Override
    public void handleQueryDataTable(
            QueryDataRequest.QuerySpec querySpec, StreamObserver<QueryTableResponse> responseObserver) {

        final TableResponseDispatcher dispatcher = new TableResponseDispatcher(responseObserver, querySpec);
        final QueryDataJob job = new QueryDataJob(querySpec, dispatcher, responseObserver, mongoQueryClient);

        logger.debug("adding queryResponseTable job id: {} to queue", responseObserver.hashCode());

        try {
            requestQueue.put(job);
        } catch (InterruptedException e) {
            logger.error("InterruptedException waiting for requestQueue.put");
            Thread.currentThread().interrupt();
        }
    }

    @Override
    public void handleQueryMetadata(
            QueryMetadataRequest.QuerySpec querySpec, StreamObserver<QueryMetadataResponse> responseObserver
    ) {
        final QueryMetadataJob job =
                new QueryMetadataJob(querySpec, responseObserver, mongoQueryClient);

        logger.debug("adding getColumnInfo job id: {} to queue", responseObserver.hashCode());

        try {
            requestQueue.put(job);
        } catch (InterruptedException e) {
            logger.error("InterruptedException waiting for requestQueue.put");
            Thread.currentThread().interrupt();
        }

    }

}
