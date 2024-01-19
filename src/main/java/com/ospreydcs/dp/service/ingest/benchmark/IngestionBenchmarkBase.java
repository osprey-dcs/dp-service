package com.ospreydcs.dp.service.ingest.benchmark;

import com.ospreydcs.dp.common.config.ConfigurationManager;
import com.ospreydcs.dp.grpc.v1.common.*;
import com.ospreydcs.dp.grpc.v1.ingestion.*;
import com.ospreydcs.dp.service.common.grpc.GrpcUtility;
import com.ospreydcs.dp.service.common.model.BenchmarkScenarioResult;
import io.grpc.*;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.text.DecimalFormat;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.*;
import java.util.concurrent.*;

public abstract class IngestionBenchmarkBase {

    private static final Logger logger = LogManager.getLogger();

    // constants
    protected static final Integer AWAIT_TIMEOUT_MINUTES = 1;
    protected static final Integer TERMINATION_TIMEOUT_MINUTES = 5;
    public static final String NAME_COLUMN_BASE = "dpTest_";

    // configuration
    public static final String CFG_KEY_GRPC_CONNECT_STRING = "IngestionBenchmark.grpcConnectString";
    public static final String DEFAULT_GRPC_CONNECT_STRING = "localhost:50051";
    public static final String CFG_KEY_START_SECONDS = "IngestionBenchmark.startSeconds";
    public static final Long DEFAULT_START_SECONDS = 1698767462L;

    /**
     * Defines dimensions and properties for IngestionRequest objects to send in an invocation
     * of the streamingIngestion gRPC API.
     */
    protected static class IngestionTaskParams {

        final public long startSeconds;
        final public int streamNumber;
        final public int numSeconds;
        final public int numColumns;
        final public int numRows;
        final public int firstColumnIndex;
        final public int lastColumnIndex;

        public IngestionTaskParams(
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

    /**
     * Encapsulates stats for an invocation of the streamingIngestion API. Includes boolean status
     * and details about data values and bytes sent in the stream.
     */
    protected static class IngestionTaskResult {

        protected boolean status;
        protected long dataValuesSubmitted = 0;
        protected long dataBytesSubmitted = 0;
        protected long grpcBytesSubmitted = 0;

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
     * Implements Callable interface for an executor service task that submits a stream
     * of ingestion requests of specified dimensions,
     * with one request per second for specified number of seconds.
     */
    protected static abstract class IngestionTask implements Callable<IngestionTaskResult> {

        protected final IngestionTaskParams params;
        protected final DataTable.Builder templateDataTable;
        protected final Channel channel;

        public IngestionTask(
                IngestionTaskParams params,
                DataTable.Builder templateDataTable,
                Channel channel) {

            this.params = params;
            this.templateDataTable = templateDataTable;
            this.channel = channel;
        }

        /**
         *
         * @return
         * @throws Exception
         */
        public abstract IngestionTaskResult call();
    }

    protected static ConfigurationManager configMgr() {
        return ConfigurationManager.getInstance();
    }

    /**
     * Generates IngestionRequest API object for specified parameters.
     *
     * @param params
     * @return
     */
    private static DataTable.Builder buildDataTableTemplate(IngestionTaskParams params) {

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

    protected static IngestionRequest prepareIngestionRequest(
            DataTable.Builder dataTableBuilder, IngestionTaskParams params, Integer secondsOffset) {

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

    protected abstract IngestionTask newIngestionTask(
            IngestionTaskParams params, DataTable.Builder templateDataTable, Channel channel);

    /**
     * Executes a multithreaded streaming ingestion scenario with specified properties.
     * Creates an executor service with a fixed size thread pool, and submits a list of
     * IngestionRequestStreamTasks for execution, each of which will call the streamingIngestion API
     * with a list of IngestionRequests.  Calculates and displays scenario performance stats.
     */
    public BenchmarkScenarioResult ingestionScenario(
            Channel channel,
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
        logger.trace("creating thread pool of size: {}", numThreads);
        var executorService = Executors.newFixedThreadPool(numThreads);

        // create list of thread pool tasks, each to submit a stream of IngestionRequests
        // final long startSeconds = Instant.now().getEpochSecond();
        final long startSeconds = configMgr().getConfigLong(CFG_KEY_START_SECONDS, DEFAULT_START_SECONDS);
        logger.trace("using startSeconds: {}", startSeconds);
        List<IngestionTask> taskList = new ArrayList<>();
        int lastColumnIndex = 0;
        for (int i = 1 ; i <= numStreams ; i++) {
            final int firstColumnIndex = lastColumnIndex + 1;
            lastColumnIndex = lastColumnIndex + numColumns;
            IngestionTaskParams params = new IngestionTaskParams(
                    startSeconds, i, numSeconds, numColumns, numRows, firstColumnIndex, lastColumnIndex);
            DataTable.Builder templateDataTable = buildDataTableTemplate(params);
            IngestionTask task = newIngestionTask(params, templateDataTable, channel);
            taskList.add(task);
        }

        // start performance measurment timer
        Instant t0 = Instant.now();

        // submit tasks to executor service, to send stream of IngestionRequests for each
        List<Future<IngestionTaskResult>> resultList = null;
        try {
            resultList = executorService.invokeAll(taskList);
            executorService.shutdown();
            if (executorService.awaitTermination(TERMINATION_TIMEOUT_MINUTES, TimeUnit.MINUTES)) {
                for (int i = 0 ; i < resultList.size() ; i++) {
                    Future<IngestionTaskResult> future = resultList.get(i);
                    IngestionTaskResult ingestionResult = future.get();
                    if (!ingestionResult.getStatus()) {
                        success = false;
                        System.err.println("ingestion task failed");
                    }
                    dataValuesSubmitted = dataValuesSubmitted + ingestionResult.getDataValuesSubmitted();
                    dataBytesSubmitted = dataBytesSubmitted + ingestionResult.getDataBytesSubmitted();
                    grpcBytesSubmitted = grpcBytesSubmitted + ingestionResult.getGrpcBytesSubmitted();
                }
            } else {
                logger.error("timeout reached in executorService.awaitTermination");
                executorService.shutdownNow();
            }
        } catch (InterruptedException | ExecutionException ex) {
            executorService.shutdownNow();
            logger.error("Data transmission Interrupted by exception: {}", ex.getMessage());
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
            logger.trace("ingestion scenario: {} data values submitted: {}", this.hashCode(), dataValuesSubmittedString);
            logger.trace("ingestion scenario: {} data bytes submitted: {}", this.hashCode(), dataBytesSubmittedString);
            logger.trace("ingestion scenario: {} grpc bytes submitted: {}", this.hashCode(), grpcBytesSubmittedString);
            logger.trace("ingestion scenario: {} grpc overhead bytes: {}", this.hashCode(), grpcOverheadBytesString);

            double dataValueRate = dataValuesSubmitted / secondsElapsed;
            double dataMByteRate = (dataBytesSubmitted / 1_000_000.0) / secondsElapsed;
            double grpcMByteRate = (grpcBytesSubmitted / 1_000_000.0) / secondsElapsed;
            DecimalFormat formatter = new DecimalFormat("#,###.00");
            String dtSecondsString = formatter.format(secondsElapsed);
            String dataValueRateString = formatter.format(dataValueRate);
            String dataMbyteRateString = formatter.format(dataMByteRate);
            String grpcMbyteRateString = formatter.format(grpcMByteRate);
            logger.debug("ingestion scenario: {} execution time: {} seconds", this.hashCode(), dtSecondsString);
            logger.debug("ingestion scenario: {} data value rate: {} values/sec", this.hashCode(), dataValueRateString);
            logger.debug("ingestion scenario: {} data byte rate: {} MB/sec", this.hashCode(), dataMbyteRateString);
            logger.debug("ingestion scenario: {} grpc byte rate: {} MB/sec", this.hashCode(), grpcMbyteRateString);

            return new BenchmarkScenarioResult(true, dataValueRate);

        } else {
            System.err.println("streaming ingestion scenario failed, performance data invalid");
            return new BenchmarkScenarioResult(false, 0.0);
        }
    }

    protected void ingestionExperiment(Channel channel) {

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

                logger.info("running streaming ingestion scenario, numThreads: {} numStreams: {}",
                        numThreads, numStreams);
                BenchmarkScenarioResult scenarioResult =
                        ingestionScenario(channel, numThreads, numStreams, numRows, numColumns, numSeconds);
                if (scenarioResult.success) {
                    writeRateMap.put(mapKey, scenarioResult.valuesPerSecond);
                } else {
                    System.err.println("error running scenario");
                    return;
                }
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

}
