package com.ospreydcs.dp.service.ingest.benchmark;

import com.ospreydcs.dp.grpc.v1.common.ExceptionalResult;
import com.ospreydcs.dp.grpc.v1.ingestion.DpIngestionServiceGrpc;
import com.ospreydcs.dp.grpc.v1.ingestion.IngestDataRequest;
import com.ospreydcs.dp.grpc.v1.ingestion.IngestDataResponse;
import io.grpc.*;
import io.grpc.stub.StreamObserver;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

public class BenchmarkStreamingIngestion extends IngestionBenchmarkBase {

    // static variables
    private static final Logger logger = LogManager.getLogger();

    /**
     * Implements Callable interface for an executor service task that submits a stream
     * of ingestion requests of specified dimensions,
     * with one request per second for specified number of seconds.
     */
    protected static class StreamingIngestionTask extends IngestionTask {

        public StreamingIngestionTask(
                IngestionTaskParams params,
                IngestDataRequest.IngestionDataFrame.Builder templateDataFrameBuilder,
                Channel channel) {

            super(params, templateDataFrameBuilder, channel);
        }

        public IngestionTaskResult call() {
            IngestionTaskResult result = sendBidiStreamingIngestionRequest(
                    this.params, this.templdateDataFrameBuilder, this.channel);
            return result;
        }

        protected void onRequest(IngestDataRequest request) {
            // hook for subclasses to add validation, default is to do nothing so we don't slow down the benchmark
        }

        protected void onResponse(IngestDataResponse response) {
            // hook for subclasses to add validation, default is to do nothing so we don't slow down the benchmark
        }

        protected void onCompleted() {
            // hook for subclasses to add validation, default is to do nothing so we don't slow down the benchmark
        }

        /**
         * Invokes streamingIngestion gRPC API with request dimensions and properties
         * as specified in the params.
         * @param params
         * @return
         */
        private IngestionTaskResult sendBidiStreamingIngestionRequest(
                IngestionTaskParams params,
                IngestDataRequest.IngestionDataFrame.Builder templateDataTable,
                Channel channel) {

            final DpIngestionServiceGrpc.DpIngestionServiceStub asyncStub = DpIngestionServiceGrpc.newStub(channel);

            final int streamNumber = params.streamNumber;
            final int numSeconds = params.numSeconds;
            final int numRows = params.numRows;
            final int numColumns = params.numColumns;

            final CountDownLatch finishLatch = new CountDownLatch(1);
            final CountDownLatch responseLatch = new CountDownLatch(numSeconds);
//        AtomicInteger responseCount = new AtomicInteger(0);
            final boolean[] responseError = {false}; // must be final for access by inner class, but we need to modify the value, so final array
            final boolean[] runtimeError = {false}; // must be final for access by inner class, but we need to modify the value, so final array

            /**
             * Implements StreamObserver interface for API response stream.
             */
            StreamObserver<IngestDataResponse> responseObserver = new StreamObserver<IngestDataResponse>() {

                /**
                 * Handles an IngestionResponse object in the API response stream.  Checks properties
                 * of response are as expected.
                 * @param response
                 */
                @Override
                public void onNext(IngestDataResponse response) {

//                responseCount.incrementAndGet();
                    responseLatch.countDown();

                    if (!response.hasAckResult()) {
                        // unexpected response
                        if (response.getExceptionalResult().getExceptionalResultStatus()
                                == ExceptionalResult.ExceptionalResultStatus.RESULT_STATUS_REJECT) {
                            logger.error("received reject with msg: "
                                    + response.getExceptionalResult().getMessage());
                        } else {
                            logger.error("unexpected responseType: "
                                    + response.getExceptionalResult().getExceptionalResultStatus().getDescriptorForType());
                        }
                        responseError[0] = true;
                        finishLatch.countDown();
                        return;

                    } else {

                        int rowCount = response.getAckResult().getNumRows();
                        int colCount = response.getAckResult().getNumColumns();
                        String requestId = response.getClientRequestId();
                        logger.trace("stream: {} received response for requestId: {}", streamNumber, requestId);

                        if (rowCount != numRows) {
                            logger.error(
                                    "stream: {} response rowCount: {} doesn't match expected rowCount: {}",
                                    streamNumber, rowCount, numRows);
                            responseError[0] = true;
                            finishLatch.countDown();
                            return;

                        }
                        if (colCount != numColumns) {
                            logger.error(
                                    "stream: {} response colCount: {} doesn't match expected colCount: {}",
                                    streamNumber, colCount, numColumns);
                            responseError[0] = true;
                            finishLatch.countDown();
                            return;
                        }

                        // call hook for subclasses to add validation
                        try {
                            onResponse(response);
                        } catch (AssertionError assertionError) {
                            if (finishLatch.getCount() != 0) {
                                System.err.println("stream: " + streamNumber + " assertion error");
                                assertionError.printStackTrace(System.err);
                                finishLatch.countDown();
                            }
                            responseError[0] = true;
                            return;
                        }

                    }
                }

                /**
                 * Handles error in API response stream.  Logs error message and terminates stream.
                 * @param t
                 */
                @Override
                public void onError(Throwable t) {
                    Status status = Status.fromThrowable(t);
                    logger.error("stream: {} streamingIngestion() Failed status: {} message: {}",
                            streamNumber, status, t.getMessage());
                    runtimeError[0] = true;
                    finishLatch.countDown();
                }

                /**
                 * Handles completion of API response stream.  Logs message and terminates stream.
                 */
                @Override
                public void onCompleted() {
                    logger.trace("stream: {} Finished streamingIngestion()", streamNumber);
                    finishLatch.countDown();
                }
            };

            StreamObserver<IngestDataRequest> requestObserver = asyncStub.ingestDataBidiStream(responseObserver);

            IngestionTaskResult result = new IngestionTaskResult();

            long dataValuesSubmitted = 0;
            long dataBytesSubmitted = 0;
            long grpcBytesSubmitted = 0;
            boolean isError = false;
            try {
                for (int secondsOffset = 0; secondsOffset < numSeconds; secondsOffset++) {

                    final String requestId = String.valueOf(secondsOffset);

                    // build IngestionRequest for current second, record elapsed time so we can subtract from measurement
                    // final IngestionRequest request = buildIngestionRequest(secondsOffset, params);
                    final IngestDataRequest request = prepareIngestionRequest(templateDataTable, params, secondsOffset);

                    // call hook for subclasses to add validation
                    try {
                        onRequest(request);
                    } catch (AssertionError assertionError) {
                        System.err.println("stream: " + streamNumber + " assertion error");
                        assertionError.printStackTrace(System.err);
                        isError = true;
                        break;
                    }

                    // send grpc ingestion request
                    logger.trace("stream: {} sending secondsOffset: {}", streamNumber, secondsOffset);
                    requestObserver.onNext(request);

                    dataValuesSubmitted = dataValuesSubmitted + (numRows * numColumns);
                    dataBytesSubmitted = dataBytesSubmitted + (numRows * numColumns * Double.BYTES);
//                grpcBytesSubmitted = grpcBytesSubmitted + request.getSerializedSize(); // adds 2% performance overhead

                    if (finishLatch.getCount() == 0) {
                        // RPC completed or errored before we finished sending.
                        // Sending further requests won't error, but they will just be thrown away.
                        isError = true;
                        break;
                    }
                }
            } catch (RuntimeException e) {
                logger.error("stream: {} streamingIngestion() failed: {}", streamNumber, e.getMessage());
                // cancel rpc, onError() sets runtimeError[0]
                isError = true;
            }

            // don't wait for responses if there was already an error
            if (!isError) {
                try {
                    // wait until all responses received
                    boolean awaitSuccess = responseLatch.await(AWAIT_TIMEOUT_MINUTES, TimeUnit.MINUTES);
                    if (!awaitSuccess) {
                        logger.error("stream: {} timeout waiting for responseLatch", streamNumber);
                        result.setStatus(false);
                        return result;
                    }
                } catch (InterruptedException e) {
                    logger.error(
                            "stream: {} streamingIngestion InterruptedException waiting for responseLatch",
                            streamNumber);
                    result.setStatus(false);
                    return result;
                }
            }

            // mark the end of requests
            requestObserver.onCompleted();

            // receiving happens asynchronously
            try {
                boolean awaitSuccess = finishLatch.await(AWAIT_TIMEOUT_MINUTES, TimeUnit.MINUTES);
                if (!awaitSuccess) {
                    logger.error("stream: {} timeout waiting for finishLatch", streamNumber);
                    result.setStatus(false);
                    return result;
                }
            } catch (InterruptedException e) {
                logger.error(
                        "stream: {} streamingIngestion InterruptedException waiting for finishLatch",
                        streamNumber);
                result.setStatus(false);
                return result;
            }

            if (responseError[0]) {
                System.err.println("stream: " + streamNumber + " response error encountered");
                result.setStatus(false);
                return result;

            } else if (runtimeError[0]) {
                System.err.println("stream: " + streamNumber + " runtime error encountered");
                result.setStatus(false);
                return result;

            } else if (isError) {
                System.err.println("stream: " + streamNumber + " request error encountered");
                result.setStatus(false);
                return result;

            } else {

                try {
                    // call hook for subclasses to add validation
                    onCompleted();
                } catch (AssertionError assertionError) {
                    System.err.println("stream: " + streamNumber + " assertion error");
                    assertionError.printStackTrace(System.err);
                    result.setStatus(false);
                    return result;
                }

//            LOGGER.info("stream: {} responseCount: {}", streamNumber, responseCount);
                result.setStatus(true);
                result.setDataValuesSubmitted(dataValuesSubmitted);
                result.setDataBytesSubmitted(dataBytesSubmitted);
                result.setGrpcBytesSubmitted(grpcBytesSubmitted);
                return result;
            }
        }
    }

    protected StreamingIngestionTask newIngestionTask(
            IngestionTaskParams params, IngestDataRequest.IngestionDataFrame.Builder templateDataTable, Channel channel
    ) {
        return new StreamingIngestionTask(params, templateDataTable, channel);
    }


    public static void main(final String[] args) {

        // Create a communication channel to the server, known as a Channel. Channels are thread-safe
        // and reusable. It is common to create channels at the beginning of your application and reuse
        // them until the application shuts down.
        //
        // For the example we use plaintext insecure credentials to avoid needing TLS certificates. To
        // use TLS, use TlsChannelCredentials instead.
        String connectString = configMgr().getConfigString(CFG_KEY_GRPC_CONNECT_STRING, DEFAULT_GRPC_CONNECT_STRING);
        logger.info("Creating gRPC channel using connect string: {}", connectString);
        final ManagedChannel channel =
                Grpc.newChannelBuilder(connectString, InsecureChannelCredentials.create()).build();

        BenchmarkStreamingIngestion benchmark = new BenchmarkStreamingIngestion();

        benchmark.ingestionExperiment(channel);

        // ManagedChannels use resources like threads and TCP connections. To prevent leaking these
        // resources the channel should be shut down when it will no longer be used. If it may be used
        // again leave it running.
        try {
            boolean awaitSuccess = channel.shutdownNow().awaitTermination(TERMINATION_TIMEOUT_MINUTES, TimeUnit.SECONDS);
            if (!awaitSuccess) {
                logger.error("timeout in channel.shutdownNow.awaitTermination");
            }
        } catch (InterruptedException e) {
            logger.error("InterruptedException in channel.shutdownNow.awaitTermination: " + e.getMessage());
        }

    }
}
