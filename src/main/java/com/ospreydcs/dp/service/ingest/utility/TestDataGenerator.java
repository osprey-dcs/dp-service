package com.ospreydcs.dp.service.ingest.utility;

import com.ospreydcs.dp.service.ingest.benchmark.BenchmarkBidiStreamingIngestion;
import io.grpc.Grpc;
import io.grpc.InsecureChannelCredentials;
import io.grpc.ManagedChannel;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.concurrent.TimeUnit;

public class TestDataGenerator extends BenchmarkBidiStreamingIngestion {

    // static variables
    private static final Logger logger = LogManager.getLogger();

    public static void main(final String[] args) {

        TestDataGenerator benchmark = new TestDataGenerator();

        logger.info("generating test data");

        final String connectString = "localhost:50051";
        logger.info("Creating gRPC channel using connect string: {}", connectString);
        final ManagedChannel channel =
                Grpc.newChannelBuilder(connectString, InsecureChannelCredentials.create()).build();

        // number of PVS, sampling rate, length of run time
        // one minute of data at 4000 PVs x 1000 samples per second for 60 seconds
        final int numPvs = 4000;
        final int samplesPerSecond = 1000;
        final int numSeconds = 60;
        final int numThreads = 7;
        final int numStreams = 20;
        final int numColumns = numPvs / numStreams;
        final int numRows = samplesPerSecond;
        final boolean generateTimestampListRequests = true; // uses DataTimestamps with TimestampList for some requests

        logger.info("running streaming ingestion scenario, numThreads: {} numStreams: {}",
                numThreads, numStreams);
        benchmark.ingestionScenario(
                channel,
                numThreads,
                numStreams,
                numRows,
                numColumns,
                numSeconds,
                generateTimestampListRequests, false);

        try {
            boolean awaitSuccess = channel.shutdownNow().awaitTermination(TERMINATION_TIMEOUT_MINUTES, TimeUnit.SECONDS);
            if (!awaitSuccess) {
                logger.error("timeout in channel.shutdownNow.awaitTermination");
            }
        } catch (InterruptedException e) {
            logger.error("InterruptedException in channel.shutdownNow.awaitTermination: " + e.getMessage());
        }

        logger.info("test data generation complete");

    }

}
