package com.ospreydcs.dp.service.query.handler.mongo;

import com.ospreydcs.dp.common.config.ConfigurationManager;
import com.ospreydcs.dp.grpc.v1.common.DataColumn;
import com.ospreydcs.dp.grpc.v1.common.DataValue;
import com.ospreydcs.dp.grpc.v1.common.FixedIntervalTimestampSpec;
import com.ospreydcs.dp.grpc.v1.common.Timestamp;
import com.ospreydcs.dp.grpc.v1.query.QueryRequest;
import com.ospreydcs.dp.grpc.v1.query.QueryResponse;
import com.ospreydcs.dp.service.common.bson.BucketDocument;
import com.ospreydcs.dp.service.common.grpc.GrpcUtility;
import com.ospreydcs.dp.service.query.handler.QueryHandlerBase;
import com.ospreydcs.dp.service.query.handler.interfaces.QueryHandlerInterface;
import com.ospreydcs.dp.service.query.handler.mongo.client.MongoQueryClientInterface;
import com.ospreydcs.dp.service.query.handler.mongo.client.MongoSyncQueryClient;
import com.ospreydcs.dp.service.query.handler.mongo.dispatch.ResponseCursorDispatcher;
import com.ospreydcs.dp.service.query.handler.mongo.dispatch.ResponseSingleDispatcher;
import com.ospreydcs.dp.service.query.handler.mongo.dispatch.ResponseStreamDispatcher;
import com.ospreydcs.dp.service.query.handler.mongo.model.QueryJob;
import io.grpc.stub.StreamObserver;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;

public class MongoQueryHandler extends QueryHandlerBase implements QueryHandlerInterface {

    private static final Logger LOGGER = LogManager.getLogger();

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
    protected BlockingQueue<QueryJob> requestQueue = new LinkedBlockingQueue<>(MAX_QUEUE_SIZE);
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

    private class QueryWorker implements Runnable {

        private final BlockingQueue queue;

        public QueryWorker(BlockingQueue q) {
            this.queue = q;
        }
        public void run() {
            try {
                while (!Thread.currentThread().isInterrupted() && !shutdownRequested.get()) {

//                    // block while waiting for a queue element
//                    HandlerQueryRequest request = (HandlerQueryRequest) queue.take();

                    // poll for next queue item with a timeout
                    QueryJob job =
                            (QueryJob) queue.poll(POLL_TIMEOUT_SECONDS, TimeUnit.SECONDS);

                    if (job != null) {
                        try {
                            executeQueryAndDispatchResults(job);
                        } catch (Exception ex) {
                            LOGGER.error("QueryWorker.run encountered exception: {}", ex.getMessage());
                        }
                    }
                }

                LOGGER.debug("QueryWorker shutting down");
                System.err.println("QueryWorker shutting down");

            } catch (InterruptedException ex) {
                LOGGER.error("InterruptedException in QueryWorker.run");
                Thread.currentThread().interrupt();
            }
        }
    }

    public static FixedIntervalTimestampSpec bucketSamplingInterval(BucketDocument document) {
        Timestamp startTime = GrpcUtility.timestampFromSeconds(document.getFirstSeconds(), document.getFirstNanos());
        FixedIntervalTimestampSpec.Builder samplingIntervalBuilder = FixedIntervalTimestampSpec.newBuilder();
        samplingIntervalBuilder.setStartTime(startTime);
        samplingIntervalBuilder.setSampleIntervalNanos(document.getSampleFrequency());
        samplingIntervalBuilder.setNumSamples(document.getNumSamples());
        return samplingIntervalBuilder.build();
    }

    public static <T> QueryResponse.QueryReport.QueryData.DataBucket dataBucketFromDocument(BucketDocument<T> document) {

        QueryResponse.QueryReport.QueryData.DataBucket.Builder bucketBuilder =
                QueryResponse.QueryReport.QueryData.DataBucket.newBuilder();

        bucketBuilder.setSamplingInterval(bucketSamplingInterval(document));

        DataColumn.Builder columnBuilder = DataColumn.newBuilder();
        columnBuilder.setName(document.getColumnName());
//        addBucketDataToColumn(document, columnBuilder);
        for (T dataValue: document.getColumnDataList()) {
            DataValue.Builder valueBuilder = DataValue.newBuilder();
            document.addColumnDataValue(dataValue, valueBuilder);
            valueBuilder.build();
            columnBuilder.addDataValues(valueBuilder);
        }
        columnBuilder.build();
        bucketBuilder.setDataColumn(columnBuilder);

        return bucketBuilder.build();
    }

    protected void executeQueryAndDispatchResults(QueryJob job) {
        LOGGER.debug("executeQueryAndDispatchResults");
        final var cursor = mongoQueryClient.executeQuery(job.getQuerySpec());
        job.getDispatcher().handleResult(cursor);
    }

    /**
     * Initializes handler. Creates ExecutorService with fixed thread pool.
     *
     * @return
     */
    @Override
    public boolean init() {

        LOGGER.debug("init");

        if (!mongoQueryClient.init()) {
            LOGGER.error("error in mongoQueryClient.init()");
            return false;
        }

        int numWorkers = configMgr().getConfigInteger(CFG_KEY_NUM_WORKERS, DEFAULT_NUM_WORKERS);
        LOGGER.info("init numWorkers: {}", numWorkers);

        // init ExecutorService
        executorService = Executors.newFixedThreadPool(numWorkers);

        for (int i = 1 ; i <= numWorkers ; i++) {
            QueryWorker worker = new QueryWorker(requestQueue);
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

        LOGGER.info("fini");
        System.err.println("fini");

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

        if (!mongoQueryClient.fini()) {
            LOGGER.error("error in mongoQueryClient.fini()");
        }

        LOGGER.info("fini shutdown completed");
        System.err.println("fini shutdown completed");

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
    public void handleQueryResponseStream(
            QueryRequest.QuerySpec querySpec, StreamObserver<QueryResponse> responseObserver) {

        LOGGER.debug("handleQueryResponseStream");

        final ResponseStreamDispatcher dispatcher = new ResponseStreamDispatcher(responseObserver);
        final QueryJob job = new QueryJob(querySpec, dispatcher);

        try {
            requestQueue.put(job);
        } catch (InterruptedException e) {
            LOGGER.error("InterruptedException waiting for requestQueue.put");
            Thread.currentThread().interrupt();
        }
    }

    @Override
    public QueryResultCursor handleQueryResponseCursor(
            QueryRequest.QuerySpec querySpec, StreamObserver<QueryResponse> responseObserver) {

        LOGGER.debug("handleQueryResponseCursor");

        final ResponseCursorDispatcher dispatcher = new ResponseCursorDispatcher(responseObserver);
        final QueryJob job = new QueryJob(querySpec, dispatcher);
        final QueryResultCursor resultCursor = new QueryResultCursor(this, dispatcher);

        try {
            requestQueue.put(job);
        } catch (InterruptedException e) {
            LOGGER.error("InterruptedException waiting for requestQueue.put");
            Thread.currentThread().interrupt();
        }

        return resultCursor;
    }

    @Override
    public void handleQueryResponseSingle(
            QueryRequest.QuerySpec querySpec, StreamObserver<QueryResponse> responseObserver) {

        LOGGER.debug("handleQueryResponseSingle");

        final ResponseSingleDispatcher dispatcher = new ResponseSingleDispatcher(responseObserver);
        final QueryJob job = new QueryJob(querySpec, dispatcher);

        try {
            requestQueue.put(job);
        } catch (InterruptedException e) {
            LOGGER.error("InterruptedException waiting for requestQueue.put");
            Thread.currentThread().interrupt();
        }
    }

}
