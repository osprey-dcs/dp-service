package com.ospreydcs.dp.service.integration;

import com.ospreydcs.dp.grpc.v1.common.Array;
import com.ospreydcs.dp.grpc.v1.common.DataColumn;
import com.ospreydcs.dp.grpc.v1.common.DataValue;
import com.ospreydcs.dp.grpc.v1.ingestion.IngestDataRequest;
import com.ospreydcs.dp.grpc.v1.ingestion.IngestDataResponse;
import com.ospreydcs.dp.grpc.v1.query.QueryDataResponse;
import com.ospreydcs.dp.service.common.bson.RequestStatusDocument;
import com.ospreydcs.dp.service.common.bson.bucket.BucketDocument;
import com.ospreydcs.dp.service.ingest.IngestionTestBase;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;

import static org.junit.Assert.*;

public class IngestionDataTypesTest extends GrpcIntegrationTestBase {

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
    public void ingestionDataTypesTest() {

        final long startSeconds = configMgr().getConfigLong(CFG_KEY_START_SECONDS, DEFAULT_START_SECONDS);
        final long startNanos = 0L;
        final int providerId = INGESTION_PROVIDER_ID;

        {
            // ingest 2D array

            // create IngestionRequestParams
            final String requestId = "request-1";
            final List<String> pvNames = Arrays.asList(
                    "array_pv_01", "array_pv_02", "array_pv_03", "array_pv_04", "array_pv_05");
            final int numPvs = pvNames.size();
            final int numSamples = 5;
            final long samplePeriod = 1_000_000_000L / numSamples;
            final long endSeconds = startSeconds;
            final long endNanos = samplePeriod * (numSamples - 1);
            final IngestionTestBase.IngestionRequestParams params =
                    new IngestionTestBase.IngestionRequestParams(
                            providerId,
                            requestId,
                            null,
                            null,
                            null,
                            null,
                            startSeconds,
                            startNanos,
                            samplePeriod, // 5 values per second
                            numSamples, // each DataColumn must contain 5 DataValues
                            pvNames,
                            null,
                            null);

            // build list of DataColumns
            final List<DataColumn> dataColumnList = new ArrayList<>();
            for(int colIndex = 0 ; colIndex < numPvs ; ++colIndex) {

                final DataColumn.Builder dataColumnBuilder = DataColumn.newBuilder();
                final String pvName = pvNames.get(colIndex);
                dataColumnBuilder.setName(pvName);

                for (int rowIndex = 0 ; rowIndex < numSamples ; ++rowIndex) {
                    final DataValue.Builder outerArrayValueBuilder = DataValue.newBuilder();
                    final Array.Builder outerArrayBuilder = Array.newBuilder();

                    for (int arrayValueIndex = 0 ; arrayValueIndex < 5 ; ++arrayValueIndex) {
                        final DataValue.Builder innerArrayValueBuilder = DataValue.newBuilder();
                        final Array.Builder innerArrayBuilder = Array.newBuilder();

                        for (int arrayElementIndex = 0 ; arrayElementIndex < 5 ; ++arrayElementIndex) {
                            final String arrayElementValueString =
                                    pvName + ":" + rowIndex + ":" + arrayValueIndex + ":" + arrayElementIndex;
                            final DataValue arrayElementValue =
                                    DataValue.newBuilder().setStringValue(arrayElementValueString).build();
                            innerArrayBuilder.addDataValues(arrayElementValue);
                        }

                        innerArrayBuilder.build();
                        innerArrayValueBuilder.setArrayValue(innerArrayBuilder);
                        innerArrayValueBuilder.build();
                        outerArrayBuilder.addDataValues(innerArrayValueBuilder);
                    }

                    outerArrayBuilder.build();
                    outerArrayValueBuilder.setArrayValue(outerArrayBuilder);
                    outerArrayValueBuilder.build();
                    dataColumnBuilder.addDataValues(outerArrayValueBuilder);
                }

                dataColumnList.add(dataColumnBuilder.build());
            }

            // build request
            final IngestDataRequest ingestionRequest = IngestionTestBase.buildIngestionRequest(params, dataColumnList);
            final List<IngestDataRequest> requestList = Arrays.asList(ingestionRequest);

            // send request
            final List<IngestDataResponse> responseList = sendIngestDataStream(requestList);
            assertEquals(requestList.size(), responseList.size());
            for (IngestDataResponse response : responseList) {
                assertTrue(response.hasAckResult());
                final IngestDataResponse.AckResult ackResult = response.getAckResult();
                assertEquals(numPvs, ackResult.getNumColumns());
                assertEquals(numSamples, ackResult.getNumRows());
            }

            // verify database contents

            // validate RequestStatusDocument - sent one request that creates 5 buckets, one for each column
            final RequestStatusDocument statusDocument =
                    mongoClient.findRequestStatus(providerId, requestId);
            assertEquals(numPvs, statusDocument.getIdsCreated().size());
            final List<String> expectedBucketIds = new ArrayList<>();
            for (String pvName : pvNames) {
                final String expectedBucketId = pvName + "-" + startSeconds + "-" + startNanos;
                assertTrue(statusDocument.getIdsCreated().contains(expectedBucketId));
                expectedBucketIds.add(expectedBucketId);
            }

            // validate BucketDocument for each column
            int pvIndex = 0;
            for (String expectedBucketId : expectedBucketIds) {

                final BucketDocument bucketDocument = mongoClient.findBucket(expectedBucketId);

                assertNotNull(bucketDocument);
                final String pvName = pvNames.get(pvIndex);
                assertEquals(pvName, bucketDocument.getColumnName());
                assertEquals(expectedBucketId, bucketDocument.getId());
                assertEquals(numSamples, bucketDocument.getNumSamples());
                assertEquals(samplePeriod, bucketDocument.getSampleFrequency());
                assertEquals(startSeconds, bucketDocument.getFirstSeconds());
                assertEquals(startNanos, bucketDocument.getFirstNanos());
                assertEquals(
                        Date.from(Instant.ofEpochSecond(startSeconds, startNanos)),
                        bucketDocument.getFirstTime());
                assertEquals(endSeconds, bucketDocument.getLastSeconds());
                assertEquals(endNanos, bucketDocument.getLastNanos());
                assertEquals(
                        Date.from(Instant.ofEpochSecond(endSeconds, endNanos)),
                        bucketDocument.getLastTime());
                assertEquals(numSamples, bucketDocument.readDataColumnContent().getDataValuesList().size());

                // compare data value vectors
                final DataColumn bucketDataColumn = bucketDocument.readDataColumnContent();
                final DataColumn requestDataColumn = dataColumnList.get(pvIndex);
                assertEquals(requestDataColumn, bucketDataColumn); // this appears to compare individual values

                pvIndex = pvIndex + 1;
            }

            // perform query for single pv and verify results
            final List<String> queryPvNames = Arrays.asList("array_pv_01");
            final DataColumn requestColumn = dataColumnList.get(0);
            final List<QueryDataResponse.QueryData.DataBucket> queryBuckets = queryDataStream(
                    queryPvNames, startSeconds, startNanos, endSeconds, endNanos);
            assertEquals(queryPvNames.size(), queryBuckets.size());
            final QueryDataResponse.QueryData.DataBucket responseBucket = queryBuckets.get(0);
            assertEquals(
                    startSeconds,
                    responseBucket.getDataTimestamps().getSamplingClock().getStartTime().getEpochSeconds());
            assertEquals(
                    startNanos,
                    responseBucket.getDataTimestamps().getSamplingClock().getStartTime().getNanoseconds());
            assertEquals(
                    samplePeriod,
                    responseBucket.getDataTimestamps().getSamplingClock().getPeriodNanos());
            assertEquals(
                    numSamples,
                    responseBucket.getDataTimestamps().getSamplingClock().getCount());
            assertEquals(
                    requestColumn,
                    responseBucket.getDataColumn());
        }
    }
}
