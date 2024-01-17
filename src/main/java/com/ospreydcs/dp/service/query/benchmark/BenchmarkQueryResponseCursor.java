package com.ospreydcs.dp.service.query.benchmark;

import com.ospreydcs.dp.grpc.v1.query.DpQueryServiceGrpc;
import com.ospreydcs.dp.grpc.v1.query.QueryRequest;
import com.ospreydcs.dp.grpc.v1.query.QueryResponse;
import io.grpc.Channel;
import io.grpc.Grpc;
import io.grpc.InsecureChannelCredentials;
import io.grpc.ManagedChannel;
import io.grpc.stub.StreamObserver;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.time.Instant;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

public class BenchmarkQueryResponseCursor extends QueryBenchmarkBase {

    // static variables
    private static final Logger logger = LogManager.getLogger();

    private static class QueryResponseCursorTask extends QueryTask {
        public QueryResponseCursorTask(Channel channel, QueryTaskParams params) {
            super(channel, params);
        }
        public QueryTaskResult call() {
            QueryTaskResult result = sendQueryResponseCursor(this.channel, this.params);
            return result;
        }
    }

    protected QueryResponseCursorTask newQueryTask(Channel channel, QueryTaskParams params) {
        return new QueryResponseCursorTask(channel, params);
    }

    private static class QueryResponseCursorObserver implements StreamObserver<QueryResponse> {

        final private int streamNumber;
        final private QueryTaskParams params;
        final CountDownLatch finishLatch;
        private StreamObserver<QueryRequest> requestObserver;
        private AtomicBoolean isError = new AtomicBoolean(false);
        private AtomicInteger dataValuesReceived = new AtomicInteger(0);
        private AtomicInteger dataBytesReceived = new AtomicInteger(0);
        private AtomicInteger grpcBytesReceived = new AtomicInteger(0);
        private AtomicInteger numResponsesReceived = new AtomicInteger(0);
        private AtomicInteger numBucketsReceived = new AtomicInteger(0);

        public QueryResponseCursorObserver(int streamNumber, QueryTaskParams params, CountDownLatch finishLatch) {
            this.streamNumber = streamNumber;
            this.params = params;
            this.finishLatch = finishLatch;
        }

        public void setRequestObserver(StreamObserver<QueryRequest> requestObserver) {
            this.requestObserver = requestObserver;
        }

        @Override
        public void onNext(QueryResponse response) {

            final String responseType = response.getResponseType().name();
            logger.debug("stream: {} received response type: {}", streamNumber, responseType);

            boolean success = true;
            String msg = "";

            if (response.hasQueryReject()) {
                isError.set(true);
                success = false;
                msg = "stream: " + streamNumber
                        + " received reject with message: " + response.getQueryReject().getMessage();
                logger.error(msg);

            } else if (response.hasQueryReport()) {

                QueryResponse.QueryReport report = response.getQueryReport();

                if (report.hasQueryData()) {

                    grpcBytesReceived.getAndAdd(response.getSerializedSize());
                    numResponsesReceived.incrementAndGet();

                    QueryResponse.QueryReport.QueryData queryData = report.getQueryData();
                    int numResultBuckets = queryData.getDataBucketsCount();
                    logger.debug("stream: {} received data result numBuckets: {}", streamNumber, numResultBuckets);

                    for (QueryResponse.QueryReport.QueryData.DataBucket bucket : queryData.getDataBucketsList()) {
                        int dataValuesCount = bucket.getDataColumn().getDataValuesCount();
//                        LOGGER.debug(
//                                "stream: {} bucket column: {} startTime: {} numValues: {}",
//                                streamNumber,
//                                bucket.getDataColumn().getName(),
//                                GrpcUtility.dateFromTimestamp(bucket.getSamplingInterval().getStartTime()),
//                                dataValuesCount);

                        dataValuesReceived.addAndGet(dataValuesCount);
                        dataBytesReceived.addAndGet(dataValuesCount * Double.BYTES);
                        numBucketsReceived.incrementAndGet();
                    }

                } else if (report.hasQueryStatus()) {
                    final QueryResponse.QueryReport.QueryStatus status = report.getQueryStatus();

                    if (status.getQueryStatusType()
                            == QueryResponse.QueryReport.QueryStatus.QueryStatusType.QUERY_STATUS_ERROR) {
                        isError.set(true);
                        success = false;
                        final String errorMsg = status.getStatusMessage();
                        msg = "stream: " + streamNumber + " received error response: " + errorMsg;
                        logger.error(msg);

                    } else if (status.getQueryStatusType()
                            == QueryResponse.QueryReport.QueryStatus.QueryStatusType.QUERY_STATUS_EMPTY) {
                        isError.set(true);
                        success = false;
                        msg = "stream: " + streamNumber + " query returned no data";
                        logger.error(msg);
                    }

                } else {
                    isError.set(true);
                    success = false;
                    msg = "stream: " + streamNumber + " received QueryReport with unexpected content";
                    logger.error(msg);
                }

            } else {
                isError.set(true);
                success = false;
                msg = "stream: " + streamNumber + " received unexpected response";
                logger.error(msg);
            }

            if (success) {

                final int numBucketsExpected = params.columnNames.size() * 60;
                final int numBucketsReceivedValue = numBucketsReceived.get();

                if  ( numBucketsReceivedValue < numBucketsExpected) {
                    // send next request if we have not received all data
                    // TRYING DIFFERENT THREAD TO FIX ISSUE WITH RE-ENTERING ResponseCursorDispathcer.sendNextResponse() in same thread!
                    new Thread(() -> {
                        logger.debug("stream: {} requesting next batch of data");
                        QueryRequest nextRequest = buildNextRequest();
                        requestObserver.onNext(nextRequest);
                    }).start();

                } else {
                    // otherwise signal that we are done
                    logger.debug("stream: {} onNext received expected number of buckets", streamNumber);
                    finishLatch.countDown();
                }

            } else {
                // something went wrong, signal that we are done
                isError.set(true);
                logger.error("stream: {} onNext unexpected error");
                finishLatch.countDown();
            }

        }

        @Override
        public void onError(Throwable t) {
            isError.set(true);
            logger.error("stream: {} responseObserver.onError with msg: {}", streamNumber, t.getMessage());
            finishLatch.countDown();;
        }

        @Override
        public void onCompleted() {
            logger.debug("stream: {} responseObserver.onCompleted", streamNumber);
        }

    }

    private static QueryTaskResult sendQueryResponseCursor(
            Channel channel,
            QueryTaskParams params
    ) {
        final int streamNumber = params.streamNumber;
        final CountDownLatch finishLatch = new CountDownLatch(1);

        boolean success = true;
        String msg = "";

        long dataValuesReceived = 0;
        long dataBytesReceived = 0;
        long grpcBytesReceived = 0;

        // create observer for api response stream
        final QueryResponseCursorObserver responseObserver =
                new QueryResponseCursorObserver(streamNumber, params, finishLatch);

        // create observer for api request stream and open api connection
        final DpQueryServiceGrpc.DpQueryServiceStub asyncStub = DpQueryServiceGrpc.newStub(channel);
        StreamObserver<QueryRequest> requestObserver = asyncStub.queryResponseCursor(responseObserver);
        responseObserver.setRequestObserver(requestObserver);

        // send query request
        logger.debug("stream: {} sending QueryRequest", streamNumber);
        QueryRequest queryRequest = buildQueryRequest(params);
        requestObserver.onNext(queryRequest);

        // check if RPC already completed
        if (finishLatch.getCount() == 0) {
            // RPC completed or errored already
            return new QueryTaskResult(false, 0, 0, 0);
        }

        // otherwise wait for completion
        try {
            boolean awaitSuccess = finishLatch.await(AWAIT_TIMEOUT_MINUTES, TimeUnit.MINUTES);
            if (!awaitSuccess) {
                logger.error("stream: {} timeout waiting for finishLatch", streamNumber);
                return new QueryTaskResult(false, 0, 0, 0);
            }
        } catch (InterruptedException e) {
            logger.error("stream: {} InterruptedException waiting for finishLatch", streamNumber);
            return new QueryTaskResult(false, 0, 0, 0);
        }

        // mark the end of requests
        requestObserver.onCompleted();

        boolean taskError = responseObserver.isError.get();

        if (!taskError) {
            return new QueryTaskResult(
                    true,
                    responseObserver.dataValuesReceived.get(),
                    responseObserver.dataBytesReceived.get(),
                    responseObserver.grpcBytesReceived.get());
        } else {
            return new QueryTaskResult(false, 0, 0, 0);
        }

    }

    public static void main(final String[] args) {

        long startSeconds = Instant.now().getEpochSecond();

        // load data for use by the query benchmark
        loadBucketData(startSeconds);

        // Create a communication channel to the server, known as a Channel. Channels are thread-safe
        // and reusable. It is common to create channels at the beginning of your application and reuse
        // them until the application shuts down.
        //
        // For the example we use plaintext insecure credentials to avoid needing TLS certificates. To
        // use TLS, use TlsChannelCredentials instead.
        String connectString = getConnectString();
        logger.info("Creating gRPC channel using connect string: {}", connectString);
        final ManagedChannel channel =
                Grpc.newChannelBuilder(connectString, InsecureChannelCredentials.create()).build();

        BenchmarkQueryResponseCursor benchmark = new BenchmarkQueryResponseCursor();

        final int[] totalNumPvsArray = {100, 500, 1000};
        final int[] numPvsPerRequestArray = {1, 10, 25, 50};
        final int[] numThreadsArray = {1, 3, 5, 7};

//        final int[] totalNumPvsArray = {1000};
//        final int[] numPvsPerRequestArray = {10};
//        final int[] numThreadsArray = {7};

        benchmark.queryExperiment(channel, totalNumPvsArray, numPvsPerRequestArray, numThreadsArray, startSeconds);

        // ManagedChannels use resources like threads and TCP connections. To prevent leaking these
        // resources the channel should be shut down when it will no longer be used. If it may be used
        // again leave it running.
        try {
            boolean awaitSuccess = channel.shutdownNow().awaitTermination(
                    TERMINATION_TIMEOUT_MINUTES, TimeUnit.SECONDS);
            if (!awaitSuccess) {
                logger.error("timeout in channel.shutdownNow.awaitTermination");
            }
        } catch (InterruptedException e) {
            logger.error("InterruptedException in channel.shutdownNow.awaitTermination: " + e.getMessage());
        }
    }

}
