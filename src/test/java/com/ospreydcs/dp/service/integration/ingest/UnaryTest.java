package com.ospreydcs.dp.service.integration.ingest;

import com.ospreydcs.dp.grpc.v1.common.ExceptionalResult;
import com.ospreydcs.dp.grpc.v1.ingestion.IngestDataRequest;
import com.ospreydcs.dp.grpc.v1.ingestion.IngestDataResponse;
import com.ospreydcs.dp.service.ingest.IngestionTestBase;
import com.ospreydcs.dp.service.integration.GrpcIntegrationTestBase;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.time.Instant;
import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class UnaryTest extends GrpcIntegrationTestBase {

    @BeforeClass
    public static void setUp() throws Exception {
        GrpcIntegrationTestBase.setUp();
    }

    @AfterClass
    public static void tearDown() {
        GrpcIntegrationTestBase.tearDown();
    }

    @Test
    public void testRejectEmptyPvNamesList() {

        // create request
        final int providerId = 1;
        final String requestId = "request-1";
        final List<String> columnNames = Arrays.asList(""); // empty PV list should cause rejection.
        final List<List<Object>> values = Arrays.asList(Arrays.asList(12.34));
        final Instant instantNow = Instant.now();
        final IngestionTestBase.IngestionRequestParams params =
                new IngestionTestBase.IngestionRequestParams(
                        providerId,
                        requestId,
                        null,
                        null,
                        null,
                        null,
                        instantNow.getEpochSecond(),
                        0L,
                        1_000_000L,
                        1,
                        columnNames,
                        IngestionTestBase.IngestionDataType.DOUBLE,
                        values);
        final IngestDataRequest request = IngestionTestBase.buildIngestionRequest(params);

        // send request and examine response
        final IngestDataResponse response = sendIngestData(request);
        assertTrue(response.getProviderId() == providerId);
        assertTrue(response.getClientRequestId().equals(requestId));
        assertTrue(response.hasExceptionalResult());
        assertEquals(
                ExceptionalResult.ExceptionalResultStatus.RESULT_STATUS_REJECT,
                response.getExceptionalResult().getExceptionalResultStatus());
        assertTrue(response.getResponseTime().getEpochSeconds() > 0);
        assertTrue(response.getExceptionalResult().getMessage().equals("name must be specified for all data columns"));
    }

    @Test
    public void testSuccessSimpleDouble() {

        // create request
        final int providerId = 1;
        final String requestId = "request-1";
        final List<String> columnNames = Arrays.asList("PV_01"); // empty PV list should cause rejection.
        final List<List<Object>> values = Arrays.asList(Arrays.asList(12.34));
        final Instant instantNow = Instant.now();
        final IngestionTestBase.IngestionRequestParams params =
                new IngestionTestBase.IngestionRequestParams(
                        providerId,
                        requestId,
                        null,
                        null,
                        null,
                        null,
                        instantNow.getEpochSecond(),
                        0L,
                        1_000_000L,
                        1,
                        columnNames,
                        IngestionTestBase.IngestionDataType.DOUBLE,
                        values);
        final IngestDataRequest request = IngestionTestBase.buildIngestionRequest(params);

        // send request and examine response
        final IngestDataResponse response = sendIngestData(request);
        assertTrue(response.getProviderId() == providerId);
        assertTrue(response.getClientRequestId().equals(requestId));
        assertTrue(response.hasAckResult());
        final IngestDataResponse.AckResult ackResult = response.getAckResult();
        assertEquals(1, ackResult.getNumColumns());
        assertEquals(1, ackResult.getNumRows());
    }

}
