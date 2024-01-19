package com.ospreydcs.dp.service.query.benchmark;

import com.ospreydcs.dp.grpc.v1.query.DpQueryServiceGrpc;
import com.ospreydcs.dp.grpc.v1.query.QueryRequest;
import com.ospreydcs.dp.grpc.v1.query.QueryResponse;
import com.ospreydcs.dp.service.common.grpc.GrpcUtility;
import io.grpc.Channel;
import io.grpc.Grpc;
import io.grpc.InsecureChannelCredentials;
import io.grpc.ManagedChannel;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.time.Instant;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

public class BenchmarkQueryResponseStream extends QueryBenchmarkBase {

    // static variables
    private static final Logger logger = LogManager.getLogger();

    private static class QueryResponseStreamTask extends QueryTask {

        public QueryResponseStreamTask(Channel channel, QueryTaskParams params) {
            super(channel, params);
        }

        public QueryTaskResult call() {
            QueryTaskResult result = sendQueryResponseStream(this.channel, this.params);
            return result;
        }

        private QueryTaskResult sendQueryResponseStream(
                Channel channel,
                QueryTaskParams params) {

            final int streamNumber = params.streamNumber;
            final CountDownLatch finishLatch = new CountDownLatch(1);

            boolean success = true;
            String msg = "";
            long dataValuesReceived = 0;
            long dataBytesReceived = 0;
            long grpcBytesReceived = 0;
            int numBucketsReceived = 0;
            int numResponsesReceived = 0;

            // build query request
            QueryRequest request = buildQueryRequest(params);

            // call hook for subclasses to validate request
            try {
                onRequest(request);
            } catch (AssertionError assertionError) {
                System.err.println("stream: " + streamNumber + " assertion error");
                assertionError.printStackTrace(System.err);
                return new QueryTaskResult(false, 0, 0, 0);
            }

            // create observer for api response stream
            final QueryBenchmarkResponseObserver responseObserver =
                    new QueryBenchmarkResponseObserver(streamNumber, params, finishLatch, this);

            // create observer for api request stream and invoke api
            final DpQueryServiceGrpc.DpQueryServiceStub asyncStub = DpQueryServiceGrpc.newStub(channel);
            asyncStub.queryResponseStream(request, responseObserver);

            // wait for completion of response stream
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

            boolean taskError = responseObserver.isError.get();

            if (!taskError) {

                // call hook for subclasses to add validation
                try {
                    onCompleted();
                } catch (AssertionError assertionError) {
                    System.err.println("stream: " + streamNumber + " assertion error");
                    assertionError.printStackTrace(System.err);
                    return new QueryTaskResult(false, 0, 0, 0);
                }

                return new QueryTaskResult(
                        true,
                        responseObserver.dataValuesReceived.get(),
                        responseObserver.dataBytesReceived.get(),
                        responseObserver.grpcBytesReceived.get());

            } else {
                return new QueryTaskResult(false, 0, 0, 0);
            }
        }

    }

    protected QueryResponseStreamTask newQueryTask(Channel channel, QueryTaskParams params) {
        return new QueryResponseStreamTask(channel, params);
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

        BenchmarkQueryResponseStream benchmark = new BenchmarkQueryResponseStream();

        final int[] totalNumPvsArray = {100, 500, 1000};
        final int[] numPvsPerRequestArray = {1, 10, 25, 50};
        final int[] numThreadsArray = {1, 3, 5, 7};

//        final int[] totalNumPvsArray = {1000};
//        final int[] numPvsPerRequestArray = {10};
//        final int[] numThreadsArray = {5};

        benchmark.queryExperiment(
                channel, totalNumPvsArray, numPvsPerRequestArray, numThreadsArray, startSeconds);

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
