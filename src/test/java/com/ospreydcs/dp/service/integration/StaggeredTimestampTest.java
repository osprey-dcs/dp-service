package com.ospreydcs.dp.service.integration;

import com.ospreydcs.dp.grpc.v1.common.*;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import static org.junit.Assert.*;

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
        
        // create data structure for validating query result
        Map<String, Map<Long, Map<Long, Double>>> validationMap = new TreeMap<>();

        // ingest data with timestamps every tenth of a second
        final String requestIdBaseTenths = "TEN-";
        final long intervalTenths = 100_000_000L;
        final int numBucketsTenths = 30;
        final int numSecondsPerBucketTenths = 1;
        final String columnNameTenths = "TEST_TENTHS";
        final Map<Long, Map<Long, Double>> valueMapTenths =
                streamingIngestion(
                        startSeconds,
                        startNanos,
                        providerId,
                        requestIdBaseTenths,
                        intervalTenths,
                        columnNameTenths,
                        numBucketsTenths,
                        numSecondsPerBucketTenths);
        validationMap.put(columnNameTenths, valueMapTenths);

        // ingest data with timestamps every twentieth of a second
        final String requestIdBaseTwentieths = "TWENTIETH-";
        final long intervalTwentieths = 200_000_000L;
        final int numBucketsTwentieths = 10;
        final int numSecondsPerBucketTwentieths = 3;
        final String columnNameTwentieths = "TEST_TWENTIETHS";
        final Map<Long, Map<Long, Double>> valueMapTwentieths =
                streamingIngestion(
                        startSeconds,
                        startNanos,
                        providerId,
                        requestIdBaseTwentieths,
                        intervalTwentieths,
                        columnNameTwentieths,
                        numBucketsTwentieths,
                        numSecondsPerBucketTwentieths);
        validationMap.put(columnNameTwentieths, valueMapTwentieths);

        // ingest data with timestamps every quarter of a second
        final String requestIdBaseQuarters = "QUARTER-";
        final long intervalQuarters = 250_000_000L;
        final int numBucketsQuarters = 6;
        final int numSecondsPerBucketQuarters = 5;
        final String columnNameQuarters = "TEST_QUARTERS";
        final Map<Long, Map<Long, Double>> valueMapQuarters =
                streamingIngestion(
                        startSeconds,
                        startNanos,
                        providerId,
                        requestIdBaseQuarters,
                        intervalQuarters,
                        columnNameQuarters,
                        numBucketsQuarters,
                        numSecondsPerBucketQuarters);
        validationMap.put(columnNameQuarters, valueMapQuarters);

        // send query for 5-second subset of ingested data
        final int queryNumSeconds = 5;
        final long queryStartSeconds = startSeconds;
        final long queryStartNanos = 0;
        final long queryEndSeconds = queryStartSeconds + queryNumSeconds;
        final long queryEndNanos = 0;
        final List<String> queryColumnNames = List.of(columnNameTenths, columnNameTwentieths, columnNameQuarters);
        final DataTable table =
                queryResponseTable(
                        queryColumnNames,
                        queryStartSeconds,
                        queryStartNanos,
                        queryEndSeconds,
                        queryEndNanos);

        // validate table contents
        final int numExpectedRows = 12 * queryNumSeconds;
        final List<Timestamp> timestampList = table.getDataTimeSpec().getTimestampList().getTimestampsList();
        assertEquals(numExpectedRows, timestampList.size());
        assertEquals(queryColumnNames.size(), table.getDataColumnsCount());
        int rowIndex = 0;
        for (Timestamp timestamp : timestampList) {
            final long timestampSeconds = timestamp.getEpochSeconds();
            final long timestampNanos = timestamp.getNanoseconds();
            for (DataColumn dataColumn : table.getDataColumnsList()) {
                // get column name and value from query result
                String columnName = dataColumn.getName();
                Double columnDataValue = dataColumn.getDataValues(rowIndex).getFloatValue();

                // get expected value from validation map
                final Map<Long, Map<Long, Double>> columnValueMap = validationMap.get(columnName);
                final Map<Long, Double> columnSecondMap = columnValueMap.get(timestampSeconds);
                assertNotNull(columnSecondMap);
                Double expectedColumnDataValue = columnSecondMap.get(timestampNanos);
                if (expectedColumnDataValue != null) {
                    assertEquals(expectedColumnDataValue, columnDataValue, 0.0);
                } else {
                    assertEquals(0.0, columnDataValue, 0.0);
                }
            }
            rowIndex = rowIndex + 1;
        }

    }

}
