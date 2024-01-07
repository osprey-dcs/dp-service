package com.ospreydcs.dp.service.query.benchmark;

import com.ospreydcs.dp.grpc.v1.query.DpQueryServiceGrpc;
import com.ospreydcs.dp.grpc.v1.query.QueryRequest;
import com.ospreydcs.dp.grpc.v1.query.QueryResponse;
import com.ospreydcs.dp.service.common.grpc.GrpcUtility;
import io.grpc.Grpc;
import io.grpc.InsecureChannelCredentials;
import io.grpc.ManagedChannel;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.time.Instant;
import java.util.*;
import java.util.concurrent.TimeUnit;

public class BenchmarkApiResponseStream extends BenchmarkApiBase {

    // static variables
    private static final Logger LOGGER = LogManager.getLogger();
    private static long START_SECONDS = 0L;

    private static class QueryResponseStreamTask extends QueryTask {
        public QueryResponseStreamTask(ManagedChannel channel, QueryTaskParams params) {
            super(channel, params);
        }
        public QueryTaskResult call() {
            QueryTaskResult result = sendQueryResponseStream(this.channel, this.params);
            return result;
        }
    }

    private static QueryTaskResult sendQueryResponseStream(
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
        int numResponsesReceived = 0;

        QueryRequest request = buildQueryRequest(params, START_SECONDS);
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
                    numResponsesReceived = numResponsesReceived + 1;
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
            final int numBucketsExpected = params.columnNames.size() * 60;

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

    protected QueryResponseStreamTask newQueryTask(ManagedChannel channel, QueryTaskParams params) {
        return new QueryResponseStreamTask(channel, params);
    }

    public static void main(final String[] args) {

        START_SECONDS = Instant.now().getEpochSecond();

        // load data for use by the query benchmark
        loadBucketData(START_SECONDS);

        // Create a communication channel to the server, known as a Channel. Channels are thread-safe
        // and reusable. It is common to create channels at the beginning of your application and reuse
        // them until the application shuts down.
        //
        // For the example we use plaintext insecure credentials to avoid needing TLS certificates. To
        // use TLS, use TlsChannelCredentials instead.
        String connectString = getConnectString();
        LOGGER.info("Creating gRPC channel using connect string: {}", connectString);
        final ManagedChannel channel =
                Grpc.newChannelBuilder(connectString, InsecureChannelCredentials.create()).build();

        BenchmarkApiResponseStream benchmark = new BenchmarkApiResponseStream();

        final int[] totalNumPvsArray = {100, 500, 1000};
        final int[] numPvsPerRequestArray = {1, 10, 25, 50};
        final int[] numThreadsArray = {1, 3, 5, 7};

//        final int[] totalNumPvsArray = {1000};
//        final int[] numPvsPerRequestArray = {10};
//        final int[] numThreadsArray = {5};

        benchmark.queryExperiment(channel, totalNumPvsArray, numPvsPerRequestArray, numThreadsArray);

        // ManagedChannels use resources like threads and TCP connections. To prevent leaking these
        // resources the channel should be shut down when it will no longer be used. If it may be used
        // again leave it running.
        try {
            boolean awaitSuccess = channel.shutdownNow().awaitTermination(
                    TERMINATION_TIMEOUT_MINUTES, TimeUnit.SECONDS);
            if (!awaitSuccess) {
                LOGGER.error("timeout in channel.shutdownNow.awaitTermination");
            }
        } catch (InterruptedException e) {
            LOGGER.error("InterruptedException in channel.shutdownNow.awaitTermination: " + e.getMessage());
        }
    }

}
