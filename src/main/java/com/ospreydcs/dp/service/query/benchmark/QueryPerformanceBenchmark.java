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
import com.ospreydcs.dp.service.common.mongo.MongoClientBase;
import com.ospreydcs.dp.service.query.handler.mongo.MongoSyncQueryClient;
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

    // constants
    protected static final String COLUMN_NAME_BASE = "queryBenchmark_";
    private static final Integer AWAIT_TIMEOUT_MINUTES = 1;
    private static final Integer TERMINATION_TIMEOUT_MINUTES = 5;

    // configuration
    public static final String CFG_KEY_GRPC_CONNECT_STRING = "QueryBenchmark.grpcConnectString";
    public static final String DEFAULT_GRPC_CONNECT_STRING = "localhost:50052";
    public static final String CFG_KEY_START_SECONDS = "QueryBenchmark.startSeconds";
    public static final Integer DEFAULT_START_SECONDS = 1698767462;

    protected static class BenchmarkDbClient extends MongoSyncQueryClient {

        private static String collectionNamePrefix = null;

        private static String getTestCollectionNamePrefix() {
            if (collectionNamePrefix == null) {
                collectionNamePrefix = "test-" + System.currentTimeMillis() + "-";
            }
            return collectionNamePrefix;
        }

        protected static String getTestCollectionNameBuckets() {
            return getTestCollectionNamePrefix() + MongoClientBase.COLLECTION_NAME_BUCKETS;
        }

        protected static String getTestCollectionNameRequestStatus() {
            return getTestCollectionNamePrefix() + MongoClientBase.COLLECTION_NAME_REQUEST_STATUS;
        }

        @Override
        protected String getCollectionNameBuckets() {
            return getTestCollectionNameBuckets();
        }

        @Override
        protected String getCollectionNameRequestStatus() {
            return getTestCollectionNameRequestStatus();
        }

        public int insertBucketDocuments(List<BucketDocument> documentList) {
            InsertManyResult result = mongoCollectionBuckets.insertMany(documentList);
            return result.getInsertedIds().size();
        }
    }

    static class QueryParams {
        private int streamNumber;
        private List<String> columnNames;

        public QueryParams(
                int streamNumber,
                List<String> columnNames) {

            this.streamNumber = streamNumber;
            this.columnNames = columnNames;
        }
    }

    static class QueryDataTaskResult {
        private boolean status;
        public QueryDataTaskResult(boolean status) {
            this.status = status;
        }
        public boolean getStatus() {
            return status;
        }
    }

    static class QueryDataByTimeTask implements Callable<QueryDataTaskResult> {

        private ManagedChannel channel = null;
        private QueryParams params = null;

        public QueryDataByTimeTask (
                ManagedChannel channel,
                QueryParams params) {

            this.channel = channel;
            this.params = params;
        }

        public QueryDataTaskResult call() {
            QueryDataTaskResult result = sendQueryDataByTimeRequestBlocking(this.channel, this.params);
            return result;
        }

    }

    private static ConfigurationManager configMgr() {
        return ConfigurationManager.getInstance();
    }

    private static QueryRequest buildQueryDataByTimeRequest(QueryParams params) {

        Integer startSeconds = configMgr().getConfigInteger(CFG_KEY_START_SECONDS, DEFAULT_START_SECONDS);
        Timestamp.Builder startTimeBuilder = Timestamp.newBuilder();
        startTimeBuilder.setEpochSeconds(startSeconds);
        startTimeBuilder.setNanoseconds(0);
        startTimeBuilder.build();
        
        Integer endSeconds = startSeconds + 60;
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

    private static QueryDataTaskResult sendQueryDataByTimeRequestBlocking(
            ManagedChannel channel,
            QueryParams params) {

        final int streamNumber = params.streamNumber;

        boolean success = true;
        final CountDownLatch finishLatch = new CountDownLatch(1);
        final boolean[] runtimeError = {false}; // must be final for access by inner class, but we need to modify the value, so final array

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

        QueryRequest request = buildQueryDataByTimeRequest(params);
        DpQueryServiceGrpc.DpQueryServiceBlockingStub blockingStub = DpQueryServiceGrpc.newBlockingStub(channel);
        Iterator<QueryResponse> responseStream = blockingStub.queryResponseStream(request);
        while (responseStream.hasNext()) {
            QueryResponse response = responseStream.next();
            final String responseType = response.getResponseType().name();
//            long firstSeconds = response.getFirstTime().getEpochSeconds();
//            long lastSeconds = response.getLastTime().getEpochSeconds();
            LOGGER.debug("stream: {} received response type: {}", streamNumber, responseType);
            if (response.hasQueryReject()) {
                LOGGER.debug("stream: {} received reject with message: {}",
                        streamNumber, response.getQueryReject().getMessage());

            } else if (response.hasQueryReport()) {

                QueryResponse.QueryReport report = response.getQueryReport();

                if (report.hasQuerySummary()) {
                    QueryResponse.QueryReport.QuerySummary summary = report.getQuerySummary();
                    LOGGER.debug(
                            "stream: {} received result summary numResults: {}",
                            streamNumber, summary.getNumBuckets());

                } else if (report.hasQueryData()) {
                    QueryResponse.QueryReport.QueryData queryData = report.getQueryData();
                    int numResultBuckets = queryData.getDataBucketsCount();
                    LOGGER.debug("stream: {} received data result numBuckets: {}", numResultBuckets);
                    for (QueryResponse.QueryReport.QueryData.DataBucket bucket : queryData.getDataBucketsList()) {
                        LOGGER.debug(
                                "stream: {} bucket column: {} startTime: {} numValues: {}",
                                streamNumber,
                                bucket.getDataColumn().getName(),
                                GrpcUtility.dateFromTimestamp(bucket.getSamplingInterval().getStartTime()),
                                bucket.getDataColumn().getDataValuesCount());
                    }

                } else if (report.hasQueryError()) {
                    QueryResponse.QueryReport.QueryError queryError = report.getQueryError();
                    LOGGER.error(
                            "stream: {} received error response: {}",
                            streamNumber, queryError.getMessage());

                } else {
                    LOGGER.error("stream: {} received QueryReport with unexpected content", streamNumber);
                }

            } else {
                LOGGER.error("stream: {} received unexpected response", streamNumber);
            }
        }

        return new QueryDataTaskResult(success);
    }


    public static double queryScenario(
            ManagedChannel channel,
            int numPvs,
            int numThreads) {

        boolean success = true;

        // create thread pool of specified size
        LOGGER.debug("creating thread pool of size: {}", numThreads);
        var executorService = Executors.newFixedThreadPool(numThreads);

        // create list of thread pool tasks, each to submit a stream of IngestionRequests
        final long startSeconds = Instant.now().getEpochSecond();
        List<QueryDataByTimeTask> taskList = new ArrayList<>();
        int lastColumnIndex = 0;
        for (int i = 1 ; i <= numPvs ; i++) {
            String columnName = "pv_" + i;
            QueryParams params = new QueryParams(i, List.of(columnName));
            QueryDataByTimeTask task = new QueryDataByTimeTask(channel, params);
            taskList.add(task);
        }

        // start performance measurment timer
        Instant t0 = Instant.now();

        // submit tasks to executor service
        List<Future<QueryDataTaskResult>> resultList = null;
        try {
            resultList = executorService.invokeAll(taskList);
            executorService.shutdown();
            if (executorService.awaitTermination(TERMINATION_TIMEOUT_MINUTES, TimeUnit.MINUTES)) {
                for (int i = 0 ; i < resultList.size() ; i++) {
                    Future<QueryDataTaskResult> future = resultList.get(i);
                    QueryDataTaskResult result = future.get();
                    if (!result.getStatus()) {
                        success = false;
                    }
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
            Instant t1 = Instant.now();
            long dtMillis = t0.until(t1, ChronoUnit.MILLIS);
            double secondsElapsed = dtMillis / 1_000.0;

            DecimalFormat formatter = new DecimalFormat("#,###.00");
            String dtSecondsString = formatter.format(secondsElapsed);
            LOGGER.debug("execution time: {} seconds", dtSecondsString);

            return 0.0; // TODO: return real data value received rate

        } else {
            LOGGER.error("streaming ingestion scenario failed, performance data invalid");
        }

        return 0.0;
    }

    public static void queryExperiment(ManagedChannel channel) {

//        final DpQueryServiceGrpc.DpQueryServiceStub asyncStub = DpQueryServiceGrpc.newStub(channel);

        final DecimalFormat formatter = new DecimalFormat("#,###.00");

        // load database with data for query
        Instant t0 = Instant.now();
        LOGGER.info("loading database");
        DB_CLIENT.init();
        final int numSamplesPerSecond = 1000;
        final int numSecondsPerBucket = 1;
        final int numColumns = 4000;
        final int numBucketsPerColumn = 60;
        final long startSeconds = Instant.now().getEpochSecond();
        for (int bucketIndex = 0 ; bucketIndex < numBucketsPerColumn ; ++bucketIndex) {
            List<BucketDocument> bucketList = BucketUtility.createBucketDocuments(
                    startSeconds+bucketIndex,
                    numSamplesPerSecond,
                    numSecondsPerBucket,
                    COLUMN_NAME_BASE,
                    numColumns,
                    1);
            DB_CLIENT.insertBucketDocuments(bucketList);
        }
        DB_CLIENT.fini();
        LOGGER.info("finished loading database");
        Instant t1 = Instant.now();
        long dtMillis = t0.until(t1, ChronoUnit.MILLIS);
        double secondsElapsed = dtMillis / 1_000.0;
        String dtSecondsString = formatter.format(secondsElapsed);
        LOGGER.debug("loading time: {} seconds", dtSecondsString);

        // TODO: don't exit!
        System.exit(0);

        final int[] numPvsArray = {/*1,*/ 10/*, 25, 50, 100, 250*/};
        final int[] numThreadsArray = {1/*, 3, 5, 7*/};

        Map<String, Double> writeRateMap = new TreeMap<>();
        for (int numPvs : numPvsArray) {
            for (int numThreads : numThreadsArray) {
                String mapKey = "numPvs: " + numPvs + " numThreads: " + numThreads;
                LOGGER.info("running queryDataByTimeScenario, numPvs: {}", numPvs);
                double writeRate = queryScenario(channel, numPvs, numThreads);
                writeRateMap.put(mapKey, writeRate);
            }
        }

        // print results summary
        double maxRate = 0.0;
        double minRate = 100_000_000;
        System.out.println("======================================");
        System.out.println("queryDataByTimeExperiment results");
        System.out.println("======================================");
        for (var mapEntry : writeRateMap.entrySet()) {
            final String mapKey = mapEntry.getKey();
            final double writeRate = mapEntry.getValue();
            final String dataValueRateString = formatter.format(writeRate);
            System.out.println(mapKey + " writeRate: " + dataValueRateString + " values/sec");
            if (writeRate > maxRate) {
                maxRate = writeRate;
            }
            if (writeRate < minRate) {
                minRate = writeRate;
            }
        }
        System.out.println("max write rate: " + maxRate);
        System.out.println("min write rate: " + minRate);
    }

    public static void main(final String[] args) {

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
