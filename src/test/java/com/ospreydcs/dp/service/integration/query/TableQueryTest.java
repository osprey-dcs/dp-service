package com.ospreydcs.dp.service.integration.query;

import com.ospreydcs.dp.service.integration.GrpcIntegrationTestBase;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.List;
import java.util.Map;

public class TableQueryTest extends GrpcIntegrationTestBase {

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
    public void queryTableTest() {

        final long startSeconds = configMgr().getConfigLong(CFG_KEY_START_SECONDS, DEFAULT_START_SECONDS);

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
            // test table query API with pvNameList and column-oriented result

            // send table query for 5-second subset of ingested data,
            // starting one second offset from start of ingestion data
            final int queryNumSeconds = 5;
            final long queryStartSeconds = startSeconds + 1;
            final long queryStartNanos = 0;
            final long queryEndSeconds = queryStartSeconds + queryNumSeconds;
            final long queryEndNanos = 0;
            final List<String> queryColumnNames = List.of("S01-GCC01", "S02-BPM02");

            // 5 buckets each with 10 rows for each PV, timestamps are aligned
            final int numRowsExpected = 10 * 5;

            sendAndVerifyQueryTablePvNameListColumnResult(
                    numRowsExpected,
                    queryColumnNames,
                    queryStartSeconds,
                    queryStartNanos,
                    queryEndSeconds,
                    queryEndNanos,
                    validationMap);
        }

        {
            // test table query API with pvNamePattern and column-oriented result

            // send table query for 5-second subset of ingested data,
            // starting one second offset from start of ingestion data
            final int queryNumSeconds = 5;
            final long queryStartSeconds = startSeconds + 1;
            final long queryStartNanos = 0;
            final long queryEndSeconds = queryStartSeconds + queryNumSeconds;
            final long queryEndNanos = 0;
            final String pvNamePattern = "S01";
            final List<String> expectedPvNameMatches =
                    List.of("S01-GCC01", "S01-GCC02", "S01-GCC03", "S01-BPM01", "S01-BPM02", "S01-BPM03");

            // 5 buckets each with 10 rows for each PV, timestamps are aligned
            final int numRowsExpected = 10 * 5;

            sendAndVerifyQueryTablePvNamePatternColumnResult(
                    numRowsExpected,
                    pvNamePattern,
                    expectedPvNameMatches,
                    queryStartSeconds,
                    queryStartNanos,
                    queryEndSeconds,
                    queryEndNanos,
                    validationMap);
        }

        {
            // test table query API with query time range offset from bucket begin/end times, column-oriented result

            // send table query for 5-second subset of ingested data,
            // starting one second offset from start of ingestion data
            final int queryNumSeconds = 5;
            final long queryStartSeconds = startSeconds + 1;
            final long queryStartNanos = 499_000_000L;  // start end middle of bucket
            final long queryEndSeconds = queryStartSeconds + queryNumSeconds;
            final long queryEndNanos = 499_000_000L;  // end in middle of bucket
            final List<String> queryColumnNames = List.of("S01-GCC01", "S02-BPM02");

            // still expect table with 50 rows, one for every tenth of a second, but offset from bucket times
            final int numRowsExpected = 10 * 5;

            sendAndVerifyQueryTablePvNameListColumnResult(
                    numRowsExpected,
                    queryColumnNames,
                    queryStartSeconds,
                    queryStartNanos,
                    queryEndSeconds,
                    queryEndNanos,
                    validationMap);
        }

        {
            // test table query API with pvNameList and row-oriented result

            // send table query for 5-second subset of ingested data,
            // starting one second offset from start of ingestion data
            final int queryNumSeconds = 5;
            final long queryStartSeconds = startSeconds + 1;
            final long queryStartNanos = 0;
            final long queryEndSeconds = queryStartSeconds + queryNumSeconds;
            final long queryEndNanos = 0;
            final List<String> queryColumnNames = List.of("S01-GCC01", "S02-BPM02");

            // 5 buckets each with 10 rows for each PV, timestamps are aligned
            final int numRowsExpected = 10 * 5;

            sendAndVerifyQueryTablePvNameListRowResult(
                    numRowsExpected,
                    queryColumnNames,
                    queryStartSeconds,
                    queryStartNanos,
                    queryEndSeconds,
                    queryEndNanos,
                    validationMap);
        }

        {
            // test table query API with pvNamePattern and row-oriented result

            // send table query for 5-second subset of ingested data,
            // starting one second offset from start of ingestion data
            final int queryNumSeconds = 5;
            final long queryStartSeconds = startSeconds + 1;
            final long queryStartNanos = 0;
            final long queryEndSeconds = queryStartSeconds + queryNumSeconds;
            final long queryEndNanos = 0;
            final String pvNamePattern = "S01";
            final List<String> expectedPvNameMatches =
                    List.of("S01-GCC01", "S01-GCC02", "S01-GCC03", "S01-BPM01", "S01-BPM02", "S01-BPM03");

            // 5 buckets each with 10 rows for each PV, timestamps are aligned
            final int numRowsExpected = 10 * 5;

            sendAndVerifyQueryTablePvNamePatternRowResult(
                    numRowsExpected,
                    pvNamePattern,
                    expectedPvNameMatches,
                    queryStartSeconds,
                    queryStartNanos,
                    queryEndSeconds,
                    queryEndNanos,
                    validationMap);
        }

        {
            // test table query API with query time range offset from bucket begin/end times, row-oriented result

            // send table query for 5-second subset of ingested data,
            // starting one second offset from start of ingestion data
            final int queryNumSeconds = 5;
            final long queryStartSeconds = startSeconds + 1;
            final long queryStartNanos = 499_000_000L;  // start end middle of bucket
            final long queryEndSeconds = queryStartSeconds + queryNumSeconds;
            final long queryEndNanos = 499_000_000L;  // end in middle of bucket
            final List<String> queryColumnNames = List.of("S01-GCC01", "S02-BPM02");

            // still expect table with 50 rows, one for every tenth of a second, but offset from bucket times
            final int numRowsExpected = 10 * 5;

            sendAndVerifyQueryTablePvNameListRowResult(
                    numRowsExpected,
                    queryColumnNames,
                    queryStartSeconds,
                    queryStartNanos,
                    queryEndSeconds,
                    queryEndNanos,
                    validationMap);
        }
    }

}
