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
public class MetadataQueryTest extends GrpcIntegrationTestBase {

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
    public void columnInfoQueryTest() {

        final long startSeconds = configMgr().getConfigLong(CFG_KEY_START_SECONDS, DEFAULT_START_SECONDS);
        final long startNanos = 0L;
        final int providerId = INGESTION_PROVIDER_ID;

        List<IngestionColumnInfo> ingestionColumnInfoList = new ArrayList<>();

        // create data for 10 sectors, each containing 3 gauges and 3 bpms
        for (int sectorIndex = 1 ; sectorIndex <= 10 ; ++sectorIndex) {
            final String sectorName = String.format("S%02d", sectorIndex);

            // create columns for 3 gccs in each sector
            for (int gccIndex = 1 ; gccIndex <= 3 ; ++ gccIndex) {
                final String gccName = sectorName + "-" + String.format("GCC%02d", gccIndex);
                final String requestIdBase = gccName + "-";
                final long interval = 100_000_000L;
                final int numBuckets = 10;
                final int numSecondsPerBucket = 1;
                final IngestionColumnInfo columnInfoTenths =
                        new IngestionColumnInfo(
                                gccName,
                                requestIdBase,
                                interval,
                                numBuckets,
                                numSecondsPerBucket);
                ingestionColumnInfoList.add(columnInfoTenths);
            }

            // create columns for 3 bpms in each sector
            for (int bpmIndex = 1 ; bpmIndex <= 3 ; ++ bpmIndex) {
                final String bpmName = sectorName + "-" + String.format("BPM%02d", bpmIndex);
                final String requestIdBase = bpmName + "-";
                final long interval = 100_000_000L;
                final int numBuckets = 10;
                final int numSecondsPerBucket = 1;
                final IngestionColumnInfo columnInfoTenths =
                        new IngestionColumnInfo(
                                bpmName,
                                requestIdBase,
                                interval,
                                numBuckets,
                                numSecondsPerBucket);
                ingestionColumnInfoList.add(columnInfoTenths);
            }
        }

        Map<String, IngestionStreamInfo> validationMap = null;
        {
            // perform ingestion for specified list of columns
            validationMap = ingestDataStreamFromColumn(ingestionColumnInfoList, startSeconds, startNanos, providerId);
        }

        {
            // send column info query for list of columns
            List<String> queryColumnNames = List.of("S01-GCC02", "S02-BPM03");
            sendAndVerifyQueryMetadata(
                    queryColumnNames, validationMap, false, null);
        }

        {
            // send column info query for column patter matching all "S01" devices
            String columnNamePattern = "S01";
            List<String> expectedColumnNameMatches =
                    List.of("S01-GCC01", "S01-GCC02", "S01-GCC03", "S01-BPM01", "S01-BPM02", "S01-BPM03");
            sendAndVerifyQueryMetadata(
                    columnNamePattern,
                    validationMap,
                    expectedColumnNameMatches,
                    false,
                    null);
        }

        {
            // send column info query for column patter matching all "GCC02" devices
            String columnNamePattern = "GCC02";
            List<String> expectedColumnNameMatches = new ArrayList<>();
            for (int i = 1 ; i <= 10 ; ++i) {
                final String sectorName = String.format("S%02d", i);
                expectedColumnNameMatches.add(sectorName + "-GCC02");
            }
            sendAndVerifyQueryMetadata(
                    columnNamePattern,
                    validationMap,
                    expectedColumnNameMatches,
                    false,
                    null);
        }

        {
            // test rejected column info query
            String columnNamePattern = ""; // send a blank string for name pattern
            List<String> expectedColumnNameMatches = new ArrayList<>();
            sendAndVerifyQueryMetadata(
                    columnNamePattern,
                    validationMap,
                    expectedColumnNameMatches,
                    true,
                    "QuerySpec.pvNamePattern.pattern must not be empty");
        }

    }
}
