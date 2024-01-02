package com.ospreydcs.dp.service.query.benchmark;

import com.mongodb.client.result.InsertManyResult;
import com.ospreydcs.dp.common.config.ConfigurationManager;
import com.ospreydcs.dp.grpc.v1.common.Timestamp;
import com.ospreydcs.dp.grpc.v1.query.DpQueryServiceGrpc;
import com.ospreydcs.dp.grpc.v1.query.QueryRequest;
import com.ospreydcs.dp.grpc.v1.query.QueryResponse;
import com.ospreydcs.dp.service.common.bson.BucketDocument;
import com.ospreydcs.dp.service.common.bson.BucketUtility;
import com.ospreydcs.dp.service.common.grpc.GrpcUtility;
import com.ospreydcs.dp.service.query.handler.mongo.client.MongoSyncQueryClient;
import io.grpc.Grpc;
import io.grpc.InsecureChannelCredentials;
import io.grpc.ManagedChannel;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.text.DecimalFormat;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.*;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.*;

public class QueryPerformanceBenchmark {

    private static final Logger LOGGER = LogManager.getLogger();

    // static variables
    private static BenchmarkDbClient DB_CLIENT = new BenchmarkDbClient();
    private static long START_SECONDS = 0L;

    // constants
    protected static final String COLUMN_NAME_BASE = "queryBenchmark_";
    private static final Integer AWAIT_TIMEOUT_MINUTES = 1;
    private static final Integer TERMINATION_TIMEOUT_MINUTES = 5;

    // configuration
    public static final String CFG_KEY_GRPC_CONNECT_STRING = "QueryBenchmark.grpcConnectString";
    public static final String DEFAULT_GRPC_CONNECT_STRING = "localhost:50052";

    protected static class BenchmarkDbClient extends MongoSyncQueryClient {

//        private static String collectionNamePrefix = null;
//
//        private static String getTestCollectionNamePrefix() {
//            if (collectionNamePrefix == null) {
//                collectionNamePrefix = "test-" + System.currentTimeMillis() + "-";
//            }
//            return collectionNamePrefix;
//        }
//
//        protected static String getTestCollectionNameBuckets() {
//            return getTestCollectionNamePrefix() + MongoClientBase.COLLECTION_NAME_BUCKETS;
//        }
//
//        protected static String getTestCollectionNameRequestStatus() {
//            return getTestCollectionNamePrefix() + MongoClientBase.COLLECTION_NAME_REQUEST_STATUS;
//        }
//
//        @Override
//        protected String getCollectionNameBuckets() {
//            return getTestCollectionNameBuckets();
//        }
//
//        @Override
//        protected String getCollectionNameRequestStatus() {
//            return getTestCollectionNameRequestStatus();
//        }

        public int insertBucketDocuments(List<BucketDocument> documentList) {
            InsertManyResult result = mongoCollectionBuckets.insertMany(documentList);
            return result.getInsertedIds().size();
        }
    }

    static class InsertTaskParams {

        final public long bucketStartSeconds;
        final public int numSamplesPerSecond;
        final public int numSecondsPerBucket;
        final public int numColumns;

        public InsertTaskParams(
                long bucketStartSeconds,
                int numSamplesPerSecond,
                int numSecondsPerBucket,
                int numColumns
        ) {
            this.bucketStartSeconds = bucketStartSeconds;
            this.numSamplesPerSecond = numSamplesPerSecond;
            this.numSecondsPerBucket = numSecondsPerBucket;
            this.numColumns = numColumns;
        }
    }

    static class InsertTaskResult {
        public final int bucketsInserted;
        public InsertTaskResult(int bucketsInserted) {
            this.bucketsInserted = bucketsInserted;
        }
    }

    static class InsertTask implements Callable<InsertTaskResult> {

        final public InsertTaskParams params;

        public InsertTask (InsertTaskParams params) {
            this.params = params;
        }

        public InsertTaskResult call() {
            return createAndInsertBucket(params);
        }

    }

    private static InsertTaskResult createAndInsertBucket(InsertTaskParams params) {
        List<BucketDocument> bucketList = BucketUtility.createBucketDocuments(
                params.bucketStartSeconds,
                params.numSamplesPerSecond,
                params.numSecondsPerBucket,
                COLUMN_NAME_BASE,
                params.numColumns,
                1);
        int bucketsInserted = DB_CLIENT.insertBucketDocuments(bucketList);
        return new InsertTaskResult(bucketsInserted);
    }

    static class QueryTaskParams {
        private int streamNumber;
        private List<String> columnNames;

        public QueryTaskParams(
                int streamNumber,
                List<String> columnNames) {

            this.streamNumber = streamNumber;
            this.columnNames = columnNames;
        }
    }

    static class QueryTaskResult {
        final public boolean status;
        final public long dataValuesReceived;
        final public long dataBytesReceived;
        final public long grpcBytesReceived;
        public QueryTaskResult(
                boolean status,
                long dataValuesReceived,
                long dataBytesReceived,
                long grpcBytesReceived
        ) {
            this.status = status;
            this.dataValuesReceived = dataValuesReceived;
            this.dataBytesReceived = dataBytesReceived;
            this.grpcBytesReceived = grpcBytesReceived;
        }
    }

    static class QueryTask implements Callable<QueryTaskResult> {

        private ManagedChannel channel = null;
        private QueryTaskParams params = null;

        public QueryTask(
                ManagedChannel channel,
                QueryTaskParams params) {

            this.channel = channel;
            this.params = params;
        }

        public QueryTaskResult call() {
            QueryTaskResult result = sendQueryDataByTimeRequestBlocking(this.channel, this.params);
            return result;
        }
    }

    private static ConfigurationManager configMgr() {
        return ConfigurationManager.getInstance();
    }

    private static void loadBucketData() {
        // load database with data for query
        Instant t0 = Instant.now();
        LOGGER.info("loading database");
        DB_CLIENT.init();
        final int numSamplesPerSecond = 1000;
        final int numSecondsPerBucket = 1;
        final int numColumns = 4000;
        final int numBucketsPerColumn = 60;
        final long startSeconds = Instant.now().getEpochSecond();

        // set up executorService with tasks to create and insert a batch of bucket documents
        // with a task for each second's data
        var executorService = Executors.newFixedThreadPool(7);
        List<InsertTask> insertTaskList = new ArrayList<>();
        for (int bucketIndex = 0 ; bucketIndex < numBucketsPerColumn ; ++bucketIndex) {
            InsertTaskParams taskParams = new InsertTaskParams(
                    startSeconds+bucketIndex,
                    numSamplesPerSecond,
                    numSecondsPerBucket,
                    numColumns);
            InsertTask task = new InsertTask(taskParams);
            insertTaskList.add(task);
        }

        // invoke tasks to create and insert bucket documents via executorService
        List<Future<InsertTaskResult>> insertTaskResultFutureList = null;
        try {
            insertTaskResultFutureList = executorService.invokeAll(insertTaskList);
            executorService.shutdown();
            if (executorService.awaitTermination(TERMINATION_TIMEOUT_MINUTES, TimeUnit.MINUTES)) {
                for (int i = 0 ; i < insertTaskResultFutureList.size() ; i++) {
                    Future<InsertTaskResult> future = insertTaskResultFutureList.get(i);
                    InsertTaskResult result = future.get();
                    if (result.bucketsInserted != numColumns) {
                        LOGGER.error("loading error, unexpected numBucketsInserted: {}", result.bucketsInserted);
                        DB_CLIENT.fini();
                        System.exit(1);
                    }
                }
            } else {
                LOGGER.error("loading error, executorService.awaitTermination reached timeout");
                executorService.shutdownNow();
                DB_CLIENT.fini();
                System.exit(1);
            }
        } catch (InterruptedException | ExecutionException ex) {
            LOGGER.error("loading error, executorService interrupted by exception: {}", ex.getMessage());
            executorService.shutdownNow();
            DB_CLIENT.fini();
            Thread.currentThread().interrupt();
            System.exit(1);
        }

        // clean up after loading and calculate stats
        DB_CLIENT.fini();
        LOGGER.info("finished loading database");
        Instant t1 = Instant.now();
        long dtMillis = t0.until(t1, ChronoUnit.MILLIS);
        double secondsElapsed = dtMillis / 1_000.0;
        final DecimalFormat formatter = new DecimalFormat("#,###.00");
        String dtSecondsString = formatter.format(secondsElapsed);
        LOGGER.info("loading time: {} seconds", dtSecondsString);

        // save start time for use in queries
        START_SECONDS = startSeconds;
    }

    private static QueryRequest buildQueryRequest(QueryTaskParams params) {

        final long startSeconds = START_SECONDS;
        Timestamp.Builder startTimeBuilder = Timestamp.newBuilder();
        startTimeBuilder.setEpochSeconds(startSeconds);
        startTimeBuilder.setNanoseconds(0);
        startTimeBuilder.build();
        
        final long endSeconds = startSeconds + 60;
        Timestamp.Builder endTimeBuilder = Timestamp.newBuilder();
        endTimeBuilder.setEpochSeconds(endSeconds);
        endTimeBuilder.setNanoseconds(0);
        endTimeBuilder.build();

        QueryRequest.Builder requestBuilder = QueryRequest.newBuilder();

        QueryRequest.QuerySpec.Builder querySpecBuilder = QueryRequest.QuerySpec.newBuilder();
        querySpecBuilder.setStartTime(startTimeBuilder);
        querySpecBuilder.setEndTime(endTimeBuilder);
        querySpecBuilder.addAllColumnNames(params.columnNames);
        querySpecBuilder.build();
        requestBuilder.setQuerySpec(querySpecBuilder);

        return requestBuilder.build();
    }

    private static QueryTaskResult sendQueryDataByTimeRequestBlocking(
            ManagedChannel channel,
            QueryTaskParams params) {

        final int streamNumber = params.streamNumber;

        boolean success = true;
        String msg = "";
        long dataValuesReceived = 0;
        long dataBytesReceived = 0;
        long grpcBytesReceived = 0;

//        final CountDownLatch finishLatch = new CountDownLatch(1);
//        final boolean[] runtimeError = {false}; // must be final for access by inner class, but we need to modify the value, so final array

//        StreamObserver<QueryDataResponse> responseObserver = new StreamObserver<QueryDataResponse>() {
//
//            @Override
//            public void onNext(QueryDataResponse queryDataResponse) {
//            }
//
//            @Override
//            public void onError(Throwable t) {
//                Status status = Status.fromThrowable(t);
//                LOGGER.error("stream: {} queryDataByTime() Failed status: {} message: {}",
//                        status, t.getMessage());
//                runtimeError[0] = true;
//                finishLatch.countDown();
//            }
//
//            @Override
//            public void onCompleted() {
//                LOGGER.debug("stream: {} Finished queryDataByTime()", streamNumber);
//                finishLatch.countDown();
//            }
//        }

        int numBucketsReceived = 0;

        QueryRequest request = buildQueryRequest(params);
        DpQueryServiceGrpc.DpQueryServiceBlockingStub blockingStub = DpQueryServiceGrpc.newBlockingStub(channel);
        Iterator<QueryResponse> responseStream = blockingStub.queryResponseStream(request);
        while (responseStream.hasNext()) {
            QueryResponse response = responseStream.next();
            final String responseType = response.getResponseType().name();
//            long firstSeconds = response.getFirstTime().getEpochSeconds();
//            long lastSeconds = response.getLastTime().getEpochSeconds();
            LOGGER.debug("stream: {} received response type: {}", streamNumber, responseType);
            grpcBytesReceived = grpcBytesReceived + response.getSerializedSize();

            if (response.hasQueryReject()) {
                success = false;
                msg = "stream: " + streamNumber
                        + " received reject with message: " + response.getQueryReject().getMessage();
                LOGGER.error(msg);

            } else if (response.hasQueryReport()) {

                QueryResponse.QueryReport report = response.getQueryReport();

                if (report.hasQueryData()) {
                    QueryResponse.QueryReport.QueryData queryData = report.getQueryData();
                    int numResultBuckets = queryData.getDataBucketsCount();
                    LOGGER.debug("stream: {} received data result numBuckets: {}", numResultBuckets);
                    for (QueryResponse.QueryReport.QueryData.DataBucket bucket : queryData.getDataBucketsList()) {
                        int dataValuesCount = bucket.getDataColumn().getDataValuesCount();
                        LOGGER.debug(
                                "stream: {} bucket column: {} startTime: {} numValues: {}",
                                streamNumber,
                                bucket.getDataColumn().getName(),
                                GrpcUtility.dateFromTimestamp(bucket.getSamplingInterval().getStartTime()),
                                dataValuesCount);
                        dataValuesReceived = dataValuesReceived + dataValuesCount;
                        dataBytesReceived = dataBytesReceived + (dataValuesCount * Double.BYTES);
                        numBucketsReceived = numBucketsReceived + 1;
                    }

                } else if (report.hasQueryStatus()) {
                    final QueryResponse.QueryReport.QueryStatus status = report.getQueryStatus();

                    if (status.getQueryStatusType()
                            == QueryResponse.QueryReport.QueryStatus.QueryStatusType.QUERY_STATUS_ERROR) {
                        success = false;
                        final String errorMsg = status.getStatusMessage();
                        msg = "stream: " + streamNumber + " received error response: " + errorMsg;
                        LOGGER.error(msg);
                        break;

                    } else if (status.getQueryStatusType()
                            == QueryResponse.QueryReport.QueryStatus.QueryStatusType.QUERY_STATUS_EMPTY) {
                        success = false;
                        msg = "stream: " + streamNumber + " query returned no data";
                        LOGGER.error(msg);
                        break;
                    }

                } else {
                    success = false;
                    msg = "stream: " + streamNumber + " received QueryReport with unexpected content";
                    LOGGER.error(msg);
                    break;
                }

            } else {
                success = false;
                msg = "stream: " + streamNumber + " received unexpected response";
                LOGGER.error(msg);
                break;
            }
        }

        if (success) {

            // expected number of buckets for query is the number of columns * 60 (which is hardwired in the query)
            int numBucketsExpected = params.columnNames.size() * 60;

            if (numBucketsReceived != numBucketsExpected) {
                // validate number of buckets received matches summary
                success = false;
                LOGGER.error(
                        "stream: {} numBucketsRecieved: {} mismatch numBucketsExpected: {}",
                        streamNumber, numBucketsReceived, numBucketsExpected);
            }

        }

        return new QueryTaskResult(success, dataValuesReceived, dataBytesReceived, grpcBytesReceived);
    }


    public static double queryScenario(
            ManagedChannel channel,
            int numPvs,
            int pvsPerRequest,
            int numThreads) {

        boolean success = true;
        long dataValuesReceived = 0;
        long dataBytesReceived = 0;
        long grpcBytesReceived = 0;

        // create thread pool of specified size
        LOGGER.info("creating thread pool of size: {}", numThreads);
        final var executorService = Executors.newFixedThreadPool(numThreads);

        // create list of thread pool tasks, each to submit a stream of IngestionRequests
        final long startSeconds = Instant.now().getEpochSecond();
        final List<QueryTask> taskList = new ArrayList<>();
        List<String> currentBatchColumns = new ArrayList<>();
        int currentBatchIndex = 1;
        for (int i = 1 ; i <= numPvs ; i++) {
            final String columnName = COLUMN_NAME_BASE + i;
            currentBatchColumns.add(columnName);
            if (currentBatchColumns.size() == pvsPerRequest) {
                // add task for existing batch of columns
                final QueryTaskParams params = new QueryTaskParams(currentBatchIndex, currentBatchColumns);
                final QueryTask task = new QueryTask(channel, params);
                taskList.add(task);
                // start a new batch of columns
                currentBatchColumns = new ArrayList<>();
                currentBatchIndex = currentBatchIndex + 1;
            }
        }
        // add task for final batch of columns, if not empty
        if (currentBatchColumns.size() > 0) {
            final QueryTaskParams params = new QueryTaskParams(currentBatchIndex, currentBatchColumns);
            final QueryTask task = new QueryTask(channel, params);
            taskList.add(task);
        }

        // start performance measurment timer
        final Instant t0 = Instant.now();

        // submit tasks to executor service
        List<Future<QueryTaskResult>> resultList = null;
        try {
            resultList = executorService.invokeAll(taskList);
            executorService.shutdown();
            if (executorService.awaitTermination(TERMINATION_TIMEOUT_MINUTES, TimeUnit.MINUTES)) {
                for (int i = 0 ; i < resultList.size() ; i++) {
                    Future<QueryTaskResult> future = resultList.get(i);
                    QueryTaskResult result = future.get();
                    if (!result.status) {
                        success = false;
                    }
                    dataValuesReceived = dataValuesReceived + result.dataValuesReceived;
                    dataBytesReceived = dataBytesReceived + result.dataBytesReceived;
                    grpcBytesReceived = grpcBytesReceived + result.grpcBytesReceived;
                }
                if (!success) {
                    LOGGER.error("thread pool future returned false");
                }
            } else {
                LOGGER.error("timeout reached in executorService.awaitTermination");
                executorService.shutdownNow();
            }
        } catch (InterruptedException | ExecutionException ex) {
            executorService.shutdownNow();
            LOGGER.warn("Data transmission interrupted by exception: {}", ex.getMessage());
            Thread.currentThread().interrupt();
        }

        if (success) {

            // stop performance measurement timer, measure elapsed time and subtract time spent building requests
            final Instant t1 = Instant.now();
            final long dtMillis = t0.until(t1, ChronoUnit.MILLIS);
            final double secondsElapsed = dtMillis / 1_000.0;

            final String dataValuesReceivedString = String.format("%,8d", dataValuesReceived);
            final String dataBytesReceivedString = String.format("%,8d", dataBytesReceived);
            final String grpcBytesReceivedString = String.format("%,8d", grpcBytesReceived);
            final String grpcOverheadBytesString = String.format("%,8d", grpcBytesReceived - dataBytesReceived);
            LOGGER.info("data values received: {}", dataValuesReceivedString);
            LOGGER.info("data bytes received: {}", dataBytesReceivedString);
            LOGGER.info("grpc bytes received: {}", grpcBytesReceivedString);
            LOGGER.info("grpc overhead bytes: {}", grpcOverheadBytesString);

            final double dataValueRate = dataValuesReceived / secondsElapsed;
            final double dataMByteRate = (dataBytesReceived / 1_000_000.0) / secondsElapsed;
            final double grpcMByteRate = (grpcBytesReceived / 1_000_000.0) / secondsElapsed;
            final DecimalFormat formatter = new DecimalFormat("#,###.00");
            final String dtSecondsString = formatter.format(secondsElapsed);
            final String dataValueRateString = formatter.format(dataValueRate);
            final String dataMbyteRateString = formatter.format(dataMByteRate);
            final String grpcMbyteRateString = formatter.format(grpcMByteRate);
            LOGGER.info("execution time: {} seconds", dtSecondsString);
            LOGGER.info("data value rate: {} values/sec", dataValueRateString);
            LOGGER.info("data byte rate: {} MB/sec", dataMbyteRateString);
            LOGGER.info("grpc byte rate: {} MB/sec", grpcMbyteRateString);

            return dataValueRate;

        } else {
            LOGGER.error("streaming ingestion scenario failed, performance data invalid");
        }

        return 0.0;
    }

    public static void queryExperiment(ManagedChannel channel) {

//        final DpQueryServiceGrpc.DpQueryServiceStub asyncStub = DpQueryServiceGrpc.newStub(channel);

        final int[] totalNumPvsArray = {/*100, 500,*/ 1000};
        final int[] numPvsPerRequestArray = {/*1, 10,*/ 25/*, 50*/};
        final int[] numThreadsArray = {/*1, 3,*/ 5/*, 7*/};

        Map<String, Double> rateMap = new TreeMap<>();
        for (int numPvs : totalNumPvsArray) {
            for (int pvsPerRequest : numPvsPerRequestArray) {
                for (int numThreads : numThreadsArray) {
                    String mapKey =
                            "numPvs: " + numPvs + " pvsPerRequest: " + pvsPerRequest + " numThreads: " + numThreads;
                    LOGGER.info(
                            "running queryScenario, numPvs: {} pvsPerRequest: {} threads: {}",
                            numPvs,pvsPerRequest, numThreads);
                    double writeRate = queryScenario(channel, numPvs, pvsPerRequest, numThreads);
                    rateMap.put(mapKey, writeRate);
                }
            }
        }

        // print results summary
        double maxRate = 0.0;
        double minRate = 100_000_000;
        System.out.println("======================================");
        System.out.println("queryExperiment results");
        System.out.println("======================================");
        final DecimalFormat formatter = new DecimalFormat("#,###.00");
        for (var mapEntry : rateMap.entrySet()) {
            final String mapKey = mapEntry.getKey();
            final double rate = mapEntry.getValue();
            final String dataValueRateString = formatter.format(rate);
            System.out.println(mapKey + " rate: " + dataValueRateString + " values/sec");
            if (rate > maxRate) {
                maxRate = rate;
            }
            if (rate < minRate) {
                minRate = rate;
            }
        }
        System.out.println("max rate: " + maxRate);
        System.out.println("min rate: " + minRate);
    }

    public static void main(final String[] args) {

        // load data for use by the query benchmark
        loadBucketData();

        // Create a communication channel to the server, known as a Channel. Channels are thread-safe
        // and reusable. It is common to create channels at the beginning of your application and reuse
        // them until the application shuts down.
        //
        // For the example we use plaintext insecure credentials to avoid needing TLS certificates. To
        // use TLS, use TlsChannelCredentials instead.
        String connectString = configMgr().getConfigString(CFG_KEY_GRPC_CONNECT_STRING, DEFAULT_GRPC_CONNECT_STRING);
        LOGGER.info("Creating gRPC channel using connect string: {}", connectString);
        final ManagedChannel channel =
                Grpc.newChannelBuilder(connectString, InsecureChannelCredentials.create()).build();

        queryExperiment(channel);

        // ManagedChannels use resources like threads and TCP connections. To prevent leaking these
        // resources the channel should be shut down when it will no longer be used. If it may be used
        // again leave it running.
        try {
            boolean awaitSuccess = channel.shutdownNow().awaitTermination(TERMINATION_TIMEOUT_MINUTES, TimeUnit.SECONDS);
            if (!awaitSuccess) {
                LOGGER.error("timeout in channel.shutdownNow.awaitTermination");
            }
        } catch (InterruptedException e) {
            LOGGER.error("InterruptedException in channel.shutdownNow.awaitTermination: " + e.getMessage());
        }
    }

}
