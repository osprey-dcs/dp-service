package com.ospreydcs.dp.service.integration.query;

import com.ospreydcs.dp.service.integration.GrpcIntegrationTestBase;
import com.ospreydcs.dp.service.integration.ingest.GrpcIntegrationIngestionServiceWrapper;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.*;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

@RunWith(JUnit4.class)
public class QueryPvMetadataIT extends GrpcIntegrationTestBase {

    // static variables
    private static final Logger logger = LogManager.getLogger();

    @Before
    public void setUp() throws Exception {
        super.setUp();
    }

    @After
    public void tearDown() {
        super.tearDown();
    }

    @Test
    public void queryPvMetadataTest() {

        // use request data contained by validationMap to verify query results
        GrpcIntegrationIngestionServiceWrapper.IngestionScenarioResult ingestionScenarioResult;
        {
            // create some data for testing query APIs
            // create data for 10 sectors, each containing 3 gauges and 3 bpms
            // named with prefix "S%02d-" followed by "GCC%02d" or "BPM%02d"
            // with 10 measurements per bucket, 1 bucket per second, and 10 buckets per pv
            ingestionScenarioResult = ingestionServiceWrapper.simpleIngestionScenario(Instant.now().getEpochSecond(), false);
        }

        // negative test case: test rejected metadata query due to blank PV name pattern
        {
            final String columnNamePattern = ""; // send a blank string for name pattern
            final List<String> expectedColumnNameMatches = new ArrayList<>();
            queryServiceWrapper.sendAndVerifyQueryPvMetadata(
                    columnNamePattern,
                    ingestionScenarioResult.validationMap(),
                    expectedColumnNameMatches,
                    true,
                    "QueryPvMetadataRequest.pvNamePattern.pattern must not be empty",
                    false);
        }

        // positive test case: empty query result with list of PV names
        {
            final List<String> pvNames = List.of("junk", "stuff");
            final boolean expectReject = false;
            final String expectedRejectMessage = "";
            final boolean expectEmpty = true;
            queryServiceWrapper.sendAndVerifyQueryPvMetadata(
                    pvNames,
                    ingestionScenarioResult.validationMap(),
                    expectReject,
                    expectedRejectMessage,
                    expectEmpty);
        }

        // positive test case: send metadata query for list of columns
        {
            final List<String> queryColumnNames = List.of("S01-GCC02", "S02-BPM03");
            queryServiceWrapper.sendAndVerifyQueryPvMetadata(
                    queryColumnNames,
                    ingestionScenarioResult.validationMap(),
                    false,
                    null,
                    false);
        }

        // positive test case: empty query result with PV name pattern
        {
            final String columnNamePattern = "junk";
            final List<String> expectedColumnNameMatches =
                    List.of();
            final boolean expectEmpty = true;
            queryServiceWrapper.sendAndVerifyQueryPvMetadata(
                    columnNamePattern,
                    ingestionScenarioResult.validationMap(),
                    expectedColumnNameMatches,
                    false,
                    null,
                    expectEmpty);
        }

        // positive test case: send metadata query for column pattern matching all "S01" devices
        {
            final String columnNamePattern = "S01";
            final List<String> expectedColumnNameMatches =
                    List.of("S01-BPM01", "S01-BPM02", "S01-BPM03", "S01-GCC01", "S01-GCC02", "S01-GCC03"); // use sorted order!
            queryServiceWrapper.sendAndVerifyQueryPvMetadata(
                    columnNamePattern,
                    ingestionScenarioResult.validationMap(),
                    expectedColumnNameMatches,
                    false,
                    null,
                    false);
        }

        // positive test case: send metadata query for column pattern matching all "GCC02" devices
        {
            final String columnNamePattern = "GCC02";
            final List<String> expectedColumnNameMatches = new ArrayList<>();
            for (int i = 1 ; i <= 10 ; ++i) {
                final String sectorName = String.format("S%02d", i);
                expectedColumnNameMatches.add(sectorName + "-GCC02");
            }
            queryServiceWrapper.sendAndVerifyQueryPvMetadata(
                    columnNamePattern,
                    ingestionScenarioResult.validationMap(),
                    expectedColumnNameMatches,
                    false,
                    null,
                    false);
        }

    }
}
