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

    @BeforeClass
    public static void setUp() throws Exception {
        GrpcIntegrationTestBase.setUp();
    }

    @AfterClass
    public static void tearDown() {
        GrpcIntegrationTestBase.tearDown();
    }

    @Test
    public void queryMetadataTest() {

        // use request data contained by validationMap to verify query results
        Map<String, IngestionStreamInfo> validationMap;
        {
            // create some data for testing query APIs
            // create data for 10 sectors, each containing 3 gauges and 3 bpms
            // named with prefix "S%02d-" followed by "GCC%02d" or "BPM%02d"
            // with 10 measurements per bucket, 1 bucket per second, and 10 buckets per pv
            validationMap = simpleIngestionScenario();
        }

        {
            // send metadata query for list of columns
            final List<String> queryColumnNames = List.of("S01-GCC02", "S02-BPM03");
            sendAndVerifyQueryMetadata(
                    queryColumnNames, validationMap, false, null);
        }

        {
            // send metadata query for column pattern matching all "S01" devices
            final String columnNamePattern = "S01";
            final List<String> expectedColumnNameMatches =
                    List.of("S01-GCC01", "S01-GCC02", "S01-GCC03", "S01-BPM01", "S01-BPM02", "S01-BPM03");
            sendAndVerifyQueryMetadata(
                    columnNamePattern,
                    validationMap,
                    expectedColumnNameMatches,
                    false,
                    null);
        }

        {
            // send metadata query for column pattern matching all "GCC02" devices
            final String columnNamePattern = "GCC02";
            final List<String> expectedColumnNameMatches = new ArrayList<>();
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
            // test rejected metadata query due to blank PV name pattern
            final String columnNamePattern = ""; // send a blank string for name pattern
            final List<String> expectedColumnNameMatches = new ArrayList<>();
            sendAndVerifyQueryMetadata(
                    columnNamePattern,
                    validationMap,
                    expectedColumnNameMatches,
                    true,
                    "QueryMetadataRequest.pvNamePattern.pattern must not be empty");
        }

    }
}
