package com.ospreydcs.dp.service.ingest.benchmark;

import com.ospreydcs.dp.common.config.ConfigurationManager;
import com.ospreydcs.dp.grpc.v1.common.*;
import com.ospreydcs.dp.grpc.v1.ingestion.*;
import com.ospreydcs.dp.service.common.grpc.GrpcUtility;
import com.ospreydcs.dp.service.ingest.service.IngestionServiceImpl;
import io.grpc.Grpc;
import io.grpc.InsecureChannelCredentials;
import io.grpc.ManagedChannel;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.text.DecimalFormat;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.*;
import java.util.concurrent.*;

public class IngestionPerformanceBenchmark {

    private static final Logger LOGGER = LogManager.getLogger();

    // constants
    private static final Integer AWAIT_TIMEOUT_MINUTES = 1;
    private static final Integer TERMINATION_TIMEOUT_MINUTES = 5;
    private static final String NAME_COLUMN_BASE = "pv_";

    // configuration
    public static final String CFG_KEY_GRPC_CONNECT_STRING = "Benchmark.grpcConnectString";
    public static final String DEFAULT_GRPC_CONNECT_STRING = "localhost:50051";
    public static final String CFG_KEY_START_SECONDS = "Benchmark.startSeconds";
    public static final Long DEFAULT_START_SECONDS = 1698767462L;

    /**
     * Encapsulates stats for an invocation of the streamingIngestion API. Includes boolean status
     * and details about data values and bytes sent in the stream.
     */
    static class IngestionStreamResult {

        private boolean status;
        private long dataValuesSubmitted = 0;
        private long dataBytesSubmitted = 0;
        private long grpcBytesSubmitted = 0;

        public boolean getStatus() {
            return status;
        }

        public void setStatus(boolean status) {
            this.status = status;
        }

        public long getDataValuesSubmitted() {
            return dataValuesSubmitted;
        }

        public void setDataValuesSubmitted(long dataValuesSubmitted) {
            this.dataValuesSubmitted = dataValuesSubmitted;
        }

        public long getDataBytesSubmitted() {
            return dataBytesSubmitted;
        }

        public void setDataBytesSubmitted(long dataBytesSubmitted) {
            this.dataBytesSubmitted = dataBytesSubmitted;
        }

        public long getGrpcBytesSubmitted() {
            return grpcBytesSubmitted;
        }

        public void setGrpcBytesSubmitted(long grpcBytesSubmitted) {
            this.grpcBytesSubmitted = grpcBytesSubmitted;
        }

    }

    /**
     * Defines dimensions and properties for IngestionRequest objects to send in an invocation
     * of the streamingIngestion gRPC API.
     */
    static class IngestionRequestStreamTaskParams {

        private long startSeconds;
        private int streamNumber;
        private int numSeconds;
        private int numColumns;
        private int numRows;
        private int firstColumnIndex;
        private int lastColumnIndex;

        public IngestionRequestStreamTaskParams(
                long startSeconds,
                int streamNumber,
                int numSeconds,
                int numColumns,
                int numRows,
                int firstColumnIndex,
                int lastColumnIndex) {

            this.startSeconds = startSeconds;
            this.streamNumber = streamNumber;
            this.numSeconds = numSeconds;
            this.numColumns = numColumns;
            this.numRows = numRows;
            this.firstColumnIndex = firstColumnIndex;
            this.lastColumnIndex = lastColumnIndex;
        }
    }

    private static ConfigurationManager configMgr() {
        return ConfigurationManager.getInstance();
    }

    /**
     * Generates IngestionRequest API object for specified parameters.
     *
     * @param params
     * @return
     */
    private static DataTable.Builder buildDataTableTemplate(IngestionRequestStreamTaskParams params) {

        DataTable.Builder dataTableBuilder = DataTable.newBuilder();

        // build list of Data objects (columns), each a list of Datum objects (cell values)
        for (int colIndex = params.firstColumnIndex; colIndex <= params.lastColumnIndex ; colIndex++) {
            DataColumn.Builder dataColumnBuilder = DataColumn.newBuilder();
            dataColumnBuilder.setName(NAME_COLUMN_BASE + colIndex);
            for (int rowIndex = 0 ; rowIndex < params.numRows ; rowIndex++) {
                double cellValue = rowIndex + (double) rowIndex /params.numRows;
                DataValue dataValue = DataValue.newBuilder().setFloatValue(cellValue).build();
                dataColumnBuilder.addDataValues(dataValue);
//                // use this commented code to look at serialized size of double data
//                int datumSize = rowDatum.getSerializedSize();
//                LOGGER.info("serialized double size: {}", datumSize);
            }
            dataColumnBuilder.build();
            dataTableBuilder.addDataColumns(dataColumnBuilder);
        }

        return dataTableBuilder;
    }

    private static IngestionRequest prepareIngestionRequest(
            DataTable.Builder dataTableBuilder, IngestionRequestStreamTaskParams params, Integer secondsOffset) {

        final int providerId = params.streamNumber;
        final String requestId = String.valueOf(secondsOffset);

        IngestionRequest.Builder requestBuilder = IngestionRequest.newBuilder();

        requestBuilder.setProviderId(providerId);
        requestBuilder.setClientRequestId(requestId);
        requestBuilder.setRequestTime(GrpcUtility.getTimestampNow());

        // build timestamp iterator for time spec
        Timestamp.Builder startTimeBuilder = Timestamp.newBuilder();
        startTimeBuilder.setEpochSeconds(params.startSeconds + secondsOffset);
        startTimeBuilder.setNanoseconds(0);
        startTimeBuilder.build();
        FixedIntervalTimestampSpec.Builder fixedIntervalSpecBuilder = FixedIntervalTimestampSpec.newBuilder();
        fixedIntervalSpecBuilder.setStartTime(startTimeBuilder);
        fixedIntervalSpecBuilder.setSampleIntervalNanos(1_000_000L);
        fixedIntervalSpecBuilder.setNumSamples(params.numRows);
        fixedIntervalSpecBuilder.build();
        DataTimeSpec.Builder timeSpecBuilder = DataTimeSpec.newBuilder();
        timeSpecBuilder.setFixedIntervalTimestampSpec(fixedIntervalSpecBuilder);
        timeSpecBuilder.build();
        dataTableBuilder.setDataTimeSpec(timeSpecBuilder);

        // add some attributes and event metadata
        EventMetadata.Builder eventMetadataBuilder = EventMetadata.newBuilder();
        eventMetadataBuilder.setEventDescription("calibration test");
        Timestamp.Builder eventTimeBuilder = Timestamp.newBuilder();
        eventTimeBuilder.setEpochSeconds(params.startSeconds);
        eventTimeBuilder.setNanoseconds(0);
        eventTimeBuilder.build();
        eventMetadataBuilder.setEventTimestamp(eventTimeBuilder);
        eventMetadataBuilder.build();
        requestBuilder.setEventMetadata(eventMetadataBuilder);
        Attribute.Builder subsystemAttributeBuilder = Attribute.newBuilder();
        subsystemAttributeBuilder.setName("subsystem");
        subsystemAttributeBuilder.setValue("vacuum");
        subsystemAttributeBuilder.build();
        requestBuilder.addAttributes(subsystemAttributeBuilder);
        Attribute.Builder sectorAttributeBuilder = Attribute.newBuilder();
        sectorAttributeBuilder.setName("sector");
        sectorAttributeBuilder.setValue("07");
        sectorAttributeBuilder.build();
        requestBuilder.addAttributes(sectorAttributeBuilder);

//        // build timestamp list
//        for (int i = 0 ; i < params.numRows ; i++) {
//            Timestamp.Builder timestampBuilder = Timestamp.newBuilder();
//            timestampBuilder.setEpochSeconds(startSeconds + secondsOffset);
//            long nanos = i * 1_000_000L;
//            timestampBuilder.setNanoseconds(nanos);
//            timestampBuilder.build();
//            dataTableBuilder.addTimestamps(timestampBuilder);
//        }

        dataTableBuilder.build();
        requestBuilder.setDataTable(dataTableBuilder);
        return requestBuilder.build();
    }

    /**
     * Invokes streamingIngestion gRPC API with request dimensions and properties
     * as specified in the params.
     * @param params
     * @return
     */
    private static IngestionStreamResult sendStreamingIngestionRequest(
            IngestionRequestStreamTaskParams params,
            DataTable.Builder templateDataTable,
            DpIngestionServiceGrpc.DpIngestionServiceStub asyncStub) {

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
        StreamObserver<IngestionResponse> responseObserver = new StreamObserver<IngestionResponse>() {

            /**
             * Handles an IngestionResponse object in the API response stream.  Checks properties
             * of response are as expected.
             * @param response
             */
            @Override
            public void onNext(IngestionResponse response) {

//                responseCount.incrementAndGet();
                responseLatch.countDown();

                boolean isError = false;
                ResponseType responseType = response.getResponseType();
                if (responseType != ResponseType.ACK_RESPONSE) {
                    // unexpected response
                    isError = true;
                    if (responseType == ResponseType.REJECT_RESPONSE) {
                        LOGGER.error("received reject with msg: "
                                + response.getRejectDetails().getMessage());
                    } else {
                        LOGGER.error("unexpected responseType: " + responseType.getDescriptorForType());
                    }
                }

                int rowCount = response.getAckDetails().getNumRows();
                int colCount = response.getAckDetails().getNumColumns();

                String requestId = response.getClientRequestId();
                LOGGER.debug("stream: {} received response for requestId: {}", streamNumber, requestId);

                if (rowCount != numRows) {
                    LOGGER.error("stream: {} response rowCount: {} doesn't match expected rowCount: {}", streamNumber, rowCount, numRows);
                    isError = true;
                }
                if (colCount != numColumns) {
                    LOGGER.error("stream: {} response colCount: {} doesn't match expected colCount: {}", streamNumber, colCount, numColumns);
                    isError = true;
                }

                if (isError) {
                    responseError[0] = true;
                }
            }

            /**
             * Handles error in API response stream.  Logs error message and terminates stream.
             * @param t
             */
            @Override
            public void onError(Throwable t) {
                Status status = Status.fromThrowable(t);
                LOGGER.error("stream: {} streamingIngestion() Failed status: {} message: {}",
                        streamNumber, status, t.getMessage());
                runtimeError[0] = true;
                finishLatch.countDown();
            }

            /**
             * Handles completion of API response stream.  Logs message and terminates stream.
             */
            @Override
            public void onCompleted() {
                LOGGER.debug("stream: {} Finished streamingIngestion()", streamNumber);
                finishLatch.countDown();
            }
        };

        StreamObserver<IngestionRequest> requestObserver = asyncStub.streamingIngestion(responseObserver);

        IngestionStreamResult result = new IngestionStreamResult();

        long dataValuesSubmitted = 0;
        long dataBytesSubmitted = 0;
        long grpcBytesSubmitted = 0;
        try {
            for (int secondsOffset = 0; secondsOffset < numSeconds; secondsOffset++) {

                final String requestId = String.valueOf(secondsOffset);

                // build IngestionRequest for current second, record elapsed time so we can subtract from measurement
                // final IngestionRequest request = buildIngestionRequest(secondsOffset, params);
                final IngestionRequest request = prepareIngestionRequest(templateDataTable, params, secondsOffset);

                // send grpc ingestion request
                LOGGER.debug("stream: {} sending secondsOffset: {}", streamNumber, secondsOffset);
                requestObserver.onNext(request);

                dataValuesSubmitted = dataValuesSubmitted + (numRows * numColumns);
                dataBytesSubmitted = dataBytesSubmitted + (numRows * numColumns * Double.BYTES);
//                grpcBytesSubmitted = grpcBytesSubmitted + request.getSerializedSize(); // adds 2% performance overhead

                if (finishLatch.getCount() == 0) {
                    // RPC completed or errored before we finished sending.
                    // Sending further requests won't error, but they will just be thrown away.
                    result.setStatus(false);
                    return result;
                }
            }
        } catch (RuntimeException e) {
            LOGGER.error("stream: {} streamingIngestion() failed: {}", streamNumber, e.getMessage());
            // cancel rpc, onError() sets runtimeError[0]
            requestObserver.onError(e);
            throw e;
        }

        try {
            // wait until all responses received
            boolean awaitSuccess = responseLatch.await(AWAIT_TIMEOUT_MINUTES, TimeUnit.MINUTES);
            if (!awaitSuccess) {
                LOGGER.error("stream: {} timeout waiting for responseLatch", streamNumber);
                result.setStatus(false);
                return result;
            }
        } catch (InterruptedException e) {
            LOGGER.error("stream: {} streamingIngestion InterruptedException waiting for responseLatch", streamNumber);
            result.setStatus(false);
            return result;
        }

        // mark the end of requests
        requestObserver.onCompleted();

        // receiving happens asynchronously
        try {
            boolean awaitSuccess = finishLatch.await(AWAIT_TIMEOUT_MINUTES, TimeUnit.MINUTES);
            if (!awaitSuccess) {
                LOGGER.error("stream: {} timeout waiting for finishLatch", streamNumber);
                result.setStatus(false);
                return result;
            }
        } catch (InterruptedException e) {
            LOGGER.error("stream: {} streamingIngestion InterruptedException waiting for finishLatch", streamNumber);
            result.setStatus(false);
            return result;
        }

        if (responseError[0]) {
            LOGGER.error("stream: {} streamingIngestion() response error encountered", streamNumber);
            result.setStatus(false);
            return result;
        } else if (runtimeError[0]) {
            LOGGER.error("stream: {} streamingIngestion() runtime error encountered", streamNumber);
            result.setStatus(false);
            return result;
        } else {
//            LOGGER.info("stream: {} responseCount: {}", streamNumber, responseCount);
            result.setStatus(true);
            result.setDataValuesSubmitted(dataValuesSubmitted);
            result.setDataBytesSubmitted(dataBytesSubmitted);
            result.setGrpcBytesSubmitted(grpcBytesSubmitted);
            return result;
        }
    }

    /**
     * Implements Callable interface for an executor service task that submits a stream
     * of ingestion requests of specified dimensions,
     * with one request per second for specified number of seconds.
     */
    static class IngestionRequestStreamTask implements Callable<IngestionStreamResult> {

        private IngestionRequestStreamTaskParams params = null;
        private DataTable.Builder templateDataTable = null;
        private DpIngestionServiceGrpc.DpIngestionServiceStub asyncStub = null;

        public IngestionRequestStreamTask (
                IngestionRequestStreamTaskParams params,
                DataTable.Builder templateDataTable,
                DpIngestionServiceGrpc.DpIngestionServiceStub asyncStub) {

            this.params = params;
            this.templateDataTable = templateDataTable;
            this.asyncStub = asyncStub;
        }

        /**
         *
         * @return
         * @throws Exception
         */
        public IngestionStreamResult call() throws Exception {
            IngestionStreamResult result = sendStreamingIngestionRequest(this.params, this.templateDataTable, this.asyncStub);
            return result;
        }
    }

    /**
     * Executes a multithreaded streaming ingestion scenario with specified properties.
     * Creates an executor service with a fixed size thread pool, and submits a list of
     * IngestionRequestStreamTasks for execution, each of which will call the streamingIngestion API
     * with a list of IngestionRequests.  Calculates and displays scenario performance stats.
     */
    public static double streamingIngestionScenario(
            DpIngestionServiceGrpc.DpIngestionServiceStub asyncStub,
            int numThreads,
            int numStreams,
            int numRows,
            int numColumns,
            int numSeconds) {

        boolean success = true;
        long dataValuesSubmitted = 0;
        long dataBytesSubmitted = 0;
        long grpcBytesSubmitted = 0;

        // create thread pool of specified size
        LOGGER.debug("creating thread pool of size: {}", numThreads);
        var executorService = Executors.newFixedThreadPool(numThreads);

        // create list of thread pool tasks, each to submit a stream of IngestionRequests
        // final long startSeconds = Instant.now().getEpochSecond();
        final long startSeconds = configMgr().getConfigLong(CFG_KEY_START_SECONDS, DEFAULT_START_SECONDS);
        List<IngestionRequestStreamTask> taskList = new ArrayList<>();
        int lastColumnIndex = 0;
        for (int i = 1 ; i <= numStreams ; i++) {
            final int firstColumnIndex = lastColumnIndex + 1;
            lastColumnIndex = lastColumnIndex + numColumns;
            IngestionRequestStreamTaskParams params = new IngestionRequestStreamTaskParams(
                    startSeconds, i, numSeconds, numColumns, numRows, firstColumnIndex, lastColumnIndex);
            DataTable.Builder templateDataTable = buildDataTableTemplate(params);
            IngestionRequestStreamTask task = new IngestionRequestStreamTask(params, templateDataTable, asyncStub);
            taskList.add(task);
        }

        // start performance measurment timer
        Instant t0 = Instant.now();

        // submit tasks to executor service, to send stream of IngestionRequests for each
        List<Future<IngestionStreamResult>> resultList = null;
        try {
            resultList = executorService.invokeAll(taskList);
            executorService.shutdown();
            if (executorService.awaitTermination(TERMINATION_TIMEOUT_MINUTES, TimeUnit.MINUTES)) {
                for (int i = 0 ; i < resultList.size() ; i++) {
                    Future<IngestionStreamResult> future = resultList.get(i);
                    IngestionStreamResult ingestionResult = future.get();
                    if (!ingestionResult.getStatus()) {
                        success = false;
                    }
                    dataValuesSubmitted = dataValuesSubmitted + ingestionResult.getDataValuesSubmitted();
                    dataBytesSubmitted = dataBytesSubmitted + ingestionResult.getDataBytesSubmitted();
                    grpcBytesSubmitted = grpcBytesSubmitted + ingestionResult.getGrpcBytesSubmitted();
                }
                if (!success) {
                    LOGGER.error("thread pool future for sendStreamingIngestionRequest() returned false");
                }
            } else {
                LOGGER.error("timeout reached in executorService.awaitTermination");
                executorService.shutdownNow();
            }
        } catch (InterruptedException | ExecutionException ex) {
            executorService.shutdownNow();
            LOGGER.warn("Data transmission Interrupted by exception: {}", ex.getMessage());
            Thread.currentThread().interrupt();
        }

        if (success) {

            // stop performance measurement timer, measure elapsed time and subtract time spent building requests
            Instant t1 = Instant.now();
            long dtMillis = t0.until(t1, ChronoUnit.MILLIS);
            double secondsElapsed = dtMillis / 1_000.0;

            String dataValuesSubmittedString = String.format("%,8d", dataValuesSubmitted);
            String dataBytesSubmittedString = String.format("%,8d", dataBytesSubmitted);
            String grpcBytesSubmittedString = String.format("%,8d", grpcBytesSubmitted);
            String grpcOverheadBytesString = String.format("%,8d", grpcBytesSubmitted - dataBytesSubmitted);
            LOGGER.debug("data values submitted: {}", dataValuesSubmittedString);
            LOGGER.debug("data bytes submitted: {}", dataBytesSubmittedString);
            LOGGER.debug("grpc bytes submitted: {}", grpcBytesSubmittedString);
            LOGGER.debug("grpc overhead bytes: {}", grpcOverheadBytesString);

            double dataValueRate = dataValuesSubmitted / secondsElapsed;
            double dataMByteRate = (dataBytesSubmitted / 1_000_000.0) / secondsElapsed;
            double grpcMByteRate = (grpcBytesSubmitted / 1_000_000.0) / secondsElapsed;
            DecimalFormat formatter = new DecimalFormat("#,###.00");
            String dtSecondsString = formatter.format(secondsElapsed);
            String dataValueRateString = formatter.format(dataValueRate);
            String dataMbyteRateString = formatter.format(dataMByteRate);
            String grpcMbyteRateString = formatter.format(grpcMByteRate);
            LOGGER.debug("execution time: {} seconds", dtSecondsString);
            LOGGER.debug("data value rate: {} values/sec", dataValueRateString);
            LOGGER.debug("data byte rate: {} MB/sec", dataMbyteRateString);
            LOGGER.debug("grpc byte rate: {} MB/sec", grpcMbyteRateString);

            return dataValueRate;

        } else {
            LOGGER.error("streaming ingestion scenario failed, performance data invalid");
        }

        return 0.0;
    }

    public static void streamingIngestionExperiment(ManagedChannel channel) {

        final DpIngestionServiceGrpc.DpIngestionServiceStub asyncStub = DpIngestionServiceGrpc.newStub(channel);

        // number of PVS, sampling rate, length of run time
        // one minute of data at 4000 PVs x 1000 samples per second for 60 seconds
        final int numPvs = 4000;
        final int samplesPerSecond = 1000;
        final int numSeconds = 60;


        // set up arrays of parameters to sweep
        final int[] numThreadsArray = {/*1, 3, 5,*/ 7};
        final int[] numStreamsArray = {20 /*, 50, 75, 100*/};
        Map<String, Double> writeRateMap = new TreeMap<>();
        for (int numThreads : numThreadsArray) {
            for (int numStreams : numStreamsArray) {

                String mapKey = "numThreads: " + numThreads + " numStreams: " + numStreams;

                // dimensions for each stream - smaller number of streams means bigger grpc messages
                final int numColumns = numPvs / numStreams;
                final int numRows = samplesPerSecond;

                LOGGER.info("running streaming ingestion scenario, numThreads: {} numStreams: {}",
                        numThreads, numStreams);
                double writeRate =
                        streamingIngestionScenario(asyncStub, numThreads, numStreams, numRows, numColumns, numSeconds);
                writeRateMap.put(mapKey, writeRate);
            }
        }

        // print results summary
        double maxRate = 0.0;
        double minRate = 100_000_000;
        System.out.println("======================================");
        System.out.println("Streaming Ingestion Experiment Results");
        System.out.println("======================================");
        final DecimalFormat formatter = new DecimalFormat("#,###.00");
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

        streamingIngestionExperiment(channel);

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
