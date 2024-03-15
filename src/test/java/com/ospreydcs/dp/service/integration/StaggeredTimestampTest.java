package com.ospreydcs.dp.service.integration;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

@RunWith(JUnit4.class)
public class StaggeredTimestampTest extends GrpcIntegrationTestBase {

    // static variables
    private static final Logger logger = LogManager.getLogger();

    // constants
    private static final int INGESTION_PROVIDER_ID = 1;
    public static final String CFG_KEY_START_SECONDS = "IngestionBenchmark.startSeconds";
    public static final Long DEFAULT_START_SECONDS = 1698767462L;

    @BeforeClass
    public static void setUp() throws Exception {
        GrpcIntegrationTestBase.setUp();
    }

    @AfterClass
    public static void tearDown() {
        GrpcIntegrationTestBase.tearDown();
    }

    @Test
    public void staggeredTimestampTest() {

        final long startSeconds = configMgr().getConfigLong(CFG_KEY_START_SECONDS, DEFAULT_START_SECONDS);
        final long startNanos = 0L;
        final int providerId = INGESTION_PROVIDER_ID;
        final String columnNameTenths = "TEST_TENTHS";
        final String columnNameTwentieths = "TEST_TWENTIETHS";
        final String columnNameQuarters = "TEST_QUARTERS";

        List<IngestionColumnInfo> columnInfoList = new ArrayList<>();

        {
            // ingest data with timestamps every tenth of a second
            final String requestIdBaseTenths = "TEN-";
            final long intervalTenths = 100_000_000L;
            final int numBucketsTenths = 30;
            final int numSecondsPerBucketTenths = 1;
            final IngestionColumnInfo columnInfoTenths =
                    new IngestionColumnInfo(
                            columnNameTenths,
                            requestIdBaseTenths,
                            intervalTenths,
                            numBucketsTenths,
                            numSecondsPerBucketTenths);
            columnInfoList.add(columnInfoTenths);
        }

        {
            // ingest data with timestamps every twentieth of a second
            final String requestIdBaseTwentieths = "TWENTIETH-";
            final long intervalTwentieths = 200_000_000L;
            final int numBucketsTwentieths = 10;
            final int numSecondsPerBucketTwentieths = 3;
            final IngestionColumnInfo columnInfoTwentieths =
                    new IngestionColumnInfo(
                            columnNameTwentieths,
                            requestIdBaseTwentieths,
                            intervalTwentieths,
                            numBucketsTwentieths,
                            numSecondsPerBucketTwentieths);
            columnInfoList.add(columnInfoTwentieths);
        }

        {
            // ingest data with timestamps every quarter of a second
            final String requestIdBaseQuarters = "QUARTER-";
            final long intervalQuarters = 250_000_000L;
            final int numBucketsQuarters = 6;
            final int numSecondsPerBucketQuarters = 5;
            final IngestionColumnInfo columnInfoQuarters =
                    new IngestionColumnInfo(
                            columnNameQuarters,
                            requestIdBaseQuarters,
                            intervalQuarters,
                            numBucketsQuarters,
                            numSecondsPerBucketQuarters);
            columnInfoList.add(columnInfoQuarters);
        }

        Map<String, IngestionStreamInfo> validationMap = null;
        {
            // perform ingestion for specified list of columns
            validationMap = ingestDataStreamFromColumn(columnInfoList, startSeconds, startNanos, providerId);
        }

        {
            // send table query for 5-second subset of ingested data,
            // starting one second offset from start of ingestion data
            final int queryNumSeconds = 5;
            final long queryStartSeconds = startSeconds + 1;
            final long queryStartNanos = 0;
            final long queryEndSeconds = queryStartSeconds + queryNumSeconds;
            final long queryEndNanos = 0;
            final List<String> queryColumnNames = List.of(columnNameTenths, columnNameTwentieths, columnNameQuarters);

            final int numRowsExpected = 12 * queryNumSeconds;

            sendAndVerifyQueryDataTable(
                    numRowsExpected,
                    queryColumnNames,
                    queryStartSeconds,
                    queryStartNanos,
                    queryEndSeconds,
                    queryEndNanos,
                    validationMap);
        }

        {
            // send bucket query for 5-second subset of ingested data,
            // starting one second offset from start of ingestion data.
            final List<String> queryColumnNamesBucket =
                    List.of(columnNameTenths, columnNameTwentieths, columnNameQuarters);
            final int queryNumSecondsBucket = 5;
            final long queryStartSecondsBucket = startSeconds + 1;
            final long queryStartNanosBucket = 0;
            final long queryEndSecondsBucket = queryStartSecondsBucket + queryNumSecondsBucket;
            final long queryEndNanosBucket = 0;

            // we expect 5 buckets for tenths (1 secs/bucket),
            // 2 buckets for twentieths (3 secs/bucket),
            // 2 buckets for quarters (5 secs/bucket)
            final int numBucketsExpected = 9;

            sendAndVerifyQueryDataStream(
                    numBucketsExpected,
                    queryColumnNamesBucket,
                    queryStartSecondsBucket,
                    queryStartNanosBucket,
                    queryEndSecondsBucket,
                    queryEndNanosBucket,
                    validationMap);
        }
    }

}
