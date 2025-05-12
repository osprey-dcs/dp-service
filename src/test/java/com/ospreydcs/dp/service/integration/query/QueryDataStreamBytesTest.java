package com.ospreydcs.dp.service.integration.query;

import com.ospreydcs.dp.service.integration.GrpcIntegrationTestBase;
import com.ospreydcs.dp.service.query.QueryTestBase;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.List;

public class QueryDataStreamBytesTest extends GrpcIntegrationTestBase {

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
    public void testQueryDataStream() {

        // use request data contained by validationMap to verify query results
        IngestionScenarioResult ingestionScenarioResult;
        {
            // create some data for testing query APIs
            // create data for 10 sectors, each containing 3 gauges and 3 bpms
            // named with prefix "S%02d-" followed by "GCC%02d" or "BPM%02d"
            // with 10 measurements per bucket, 1 bucket per second, and 10 buckets per pv
            ingestionScenarioResult = simpleIngestionScenario();
        }

        // positive queryDataStream() test case, empty query result
        {
            final List<String> pvNames = List.of("junk", "stuff"); // bogus PV names

            // select 5 seconds of data for each pv
            final long startSeconds = configMgr().getConfigLong(CFG_KEY_START_SECONDS, DEFAULT_START_SECONDS);
            final long beginSeconds = startSeconds + 1;
            final long beginNanos = 0L;
            final long endSeconds = startSeconds + 6;
            final long endNanos = 0L;
            final boolean useSerializedDataColumns = true;

            final QueryTestBase.QueryDataRequestParams params =
                    new QueryTestBase.QueryDataRequestParams(
                            pvNames,
                            beginSeconds,
                            beginNanos,
                            endSeconds,
                            endNanos,
                            useSerializedDataColumns
                    );

            final int numBucketsExpected = 0; // we expect an empty response
            final boolean expectReject = false;
            final String expectedRejectMessage = "";

            sendAndVerifyQueryDataStream(
                    numBucketsExpected,
                    params,
                    ingestionScenarioResult.validationMap,
                    expectReject,
                    expectedRejectMessage
            );
        }

        // positive queryDataStream() test case
        {
            final List<String> pvNames = List.of("S01-GCC01", "S01-BPM01");

            // select 5 seconds of data for each pv
            final long startSeconds = configMgr().getConfigLong(CFG_KEY_START_SECONDS, DEFAULT_START_SECONDS);
            final long beginSeconds = startSeconds + 1;
            final long beginNanos = 0L;
            final long endSeconds = startSeconds + 6;
            final long endNanos = 0L;
            final boolean useSerializedDataColumns = true;

            // 2 pvs, 5 seconds, 1 bucket per second per pv
            final int numBucketsExpected = 10;

            final QueryTestBase.QueryDataRequestParams params =
                    new QueryTestBase.QueryDataRequestParams(pvNames,
                            beginSeconds,
                            beginNanos,
                            endSeconds,
                            endNanos,
                            useSerializedDataColumns
                    );

            sendAndVerifyQueryDataStream(
                    numBucketsExpected,
                    params,
                    ingestionScenarioResult.validationMap,
                    false,
                    ""
            );
        }
    }

}
