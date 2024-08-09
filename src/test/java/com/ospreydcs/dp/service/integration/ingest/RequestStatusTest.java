package com.ospreydcs.dp.service.integration.ingest;

import com.ospreydcs.dp.grpc.v1.ingestion.IngestionRequestStatus;
import com.ospreydcs.dp.grpc.v1.ingestion.QueryRequestStatusResponse;
import com.ospreydcs.dp.service.ingest.IngestionTestBase;
import com.ospreydcs.dp.service.integration.GrpcIntegrationTestBase;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

/*
 * Provides coverage for the request status query API.
 */
public class RequestStatusTest extends GrpcIntegrationTestBase {

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
    public void requestStatusTest() {

        {
            // create some data for testing query APIs
            // create data for 10 sectors, each containing 3 gauges and 3 bpms
            // named with prefix "S%02d-" followed by "GCC%02d" or "BPM%02d"
            // with 10 measurements per bucket, 1 bucket per second, and 10 buckets per pv
            simpleIngestionScenario();
        }

        {
            // send request status query
            final Integer providerId = 1;
            final String providerName = null;
            final String requestId = "S01-GCC01-3";
            final IngestionRequestStatus status = null;
            final Long beginSeconds = null;
            final Long beginNanos = null;
            final Long endSeconds = null;
            final Long endNanos = null;

            final IngestionTestBase.QueryRequestStatusParams params = new IngestionTestBase.QueryRequestStatusParams(
                    providerId,
                    providerName,
                    requestId,
                    status,
                    beginSeconds,
                    beginNanos,
                    endSeconds,
                    endNanos
            );

            // Create map of expected responses for use in verification
            final IngestionTestBase.QueryRequestStatusExpectedResponseMap expectedResponseMap =
                    new IngestionTestBase.QueryRequestStatusExpectedResponseMap();
            final IngestionRequestStatus expectedStatus = IngestionRequestStatus.INGESTION_REQUEST_STATUS_SUCCESS;
            final String expectedMessage = "";
            final List<String> expectedIdsCreated = Arrays.asList("S01-GCC01-1698767465-0");
            final IngestionTestBase.QueryRequestStatusExpectedResponse expectedResponse =
                    new IngestionTestBase.QueryRequestStatusExpectedResponse(
                            providerId, requestId, expectedStatus, expectedMessage, expectedIdsCreated);
            expectedResponseMap.addExpectedResponse(expectedResponse);

            sendAndVerifyQueryRequestStatus(params, expectedResponseMap, false, "");
        }
    }

}
