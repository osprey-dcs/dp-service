package com.ospreydcs.dp.service.integration.ingestionstream;

import com.ospreydcs.dp.service.ingest.IngestionTestBase;
import com.ospreydcs.dp.service.integration.GrpcIntegrationTestBase;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.List;
import java.util.Map;

public class SubscribeDataTest extends GrpcIntegrationTestBase {

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
    public void subscribeDataTest() {

        {
            // negative test case for subscribeData() with empty list of PV names

            final List<String> subscriptionPvNames = List.of();
            final int expectedResponseCount = 30;
            final boolean expectReject = true;
            final String expectedRejectMessage = "SubscribeDataRequest.pvNames list must not be empty";
            final IngestionTestBase.SubscribeDataResponseObserver responseObserver =
                    initiateSubscribeDataRequest(
                            subscriptionPvNames, expectedResponseCount, expectReject, expectedRejectMessage);
        }

        {
            // positive test case for subscribeData()

            final List<String> subscriptionPvNames = List.of("S01-GCC01", "S02-GCC02", "S03-BPM03");
            final int expectedResponseCount = 30;
            final boolean expectReject = false;
            final String expectedRejectMessage = "";
            final IngestionTestBase.SubscribeDataResponseObserver responseObserver =
                    initiateSubscribeDataRequest(
                            subscriptionPvNames, expectedResponseCount, expectReject, expectedRejectMessage);

            // use request data contained by validationMap to verify query results
            Map<String, IngestionStreamInfo> validationMap;
            {
                // create some data for testing query APIs
                // create data for 10 sectors, each containing 3 gauges and 3 bpms
                // named with prefix "S%02d-" followed by "GCC%02d" or "BPM%02d"
                // with 10 measurements per bucket, 1 bucket per second, and 10 buckets per pv
                validationMap = simpleIngestionScenario();
            }

            verifySubscribeDataResponse(responseObserver, subscriptionPvNames, validationMap);
        }
    }

}
