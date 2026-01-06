package com.ospreydcs.dp.service.integration.ingest;

import com.ospreydcs.dp.grpc.v1.ingestion.SubscribeDataRequest;
import com.ospreydcs.dp.service.ingest.IngestionTestBase;
import com.ospreydcs.dp.service.ingest.utility.SubscribeDataUtility;
import com.ospreydcs.dp.service.integration.annotation.AnnotationIntegrationTestIntermediate;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.*;

import java.time.Instant;
import java.util.List;
import java.util.Map;

public class SubscribeDataBytesIT extends AnnotationIntegrationTestIntermediate {

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
    public void testSubscribeDataBytes() {

        final long startSeconds = Instant.now().getEpochSecond();
        final long startNanos = 0L;

        {
            // Pre-populate some data in the archive for the PVs that we will be using.
            // This is necessary because validation is performed that data exists in the archive for the
            // PV names in subscribeData() requests.
            annotationIngestionScenario(startSeconds-600);
        }

        // Provides coverage for subscribeData() where the ingestion requests use SerializedDataColumns,
        // so that the SubscribeDataResponse messages also contain byte data.  The scenario uses
        // annotationIngestionScenario() to send such ingestion requests instead of simpleIngestionScenario().
        // That's the difference between this test class and SubscribeDataIT.
        {
            // positive test cases for subscribeData() with 3 different alternatives for canceling subscription:
            // 1) implicit close on ingestion server shutdown
            // 2) explicit CancelSubscription message in request stream and
            // 3) client closing request stream without canceling.

            SubscribeDataUtility.SubscribeDataCall subscribeDataCall1;
            List<String> subscriptionPvNames1;
            SubscribeDataUtility.SubscribeDataCall subscribeDataCall2;
            List<String> subscriptionPvNames2;
            SubscribeDataUtility.SubscribeDataCall subscribeDataCall3;
            List<String> subscriptionPvNames3;

            {
                // 1) invoke subscribeData with call that will be closed implicitly by server
                subscriptionPvNames1 = List.of("S01-GCC01", "S02-GCC01", "S03-BPM01");
                final int expectedResponseCount = 30;
                final boolean expectReject = false;
                final String expectedRejectMessage = "";
                subscribeDataCall1 =
                        ingestionServiceWrapper.initiateSubscribeDataRequest(
                                subscriptionPvNames1, expectedResponseCount, expectReject, expectedRejectMessage);
            }

            {
                // 2) invoke subscribeData with call that will be closed explicitly by client with cancel message
                subscriptionPvNames2 = List.of("S01-GCC02", "S02-GCC02", "S03-BPM02");
                final int expectedResponseCount = 30;
                final boolean expectReject = false;
                final String expectedRejectMessage = "";
                subscribeDataCall2 =
                        ingestionServiceWrapper.initiateSubscribeDataRequest(
                                subscriptionPvNames2, expectedResponseCount, expectReject, expectedRejectMessage);
            }

            {
                // 3) invoke subscribeData with call that will be closed explicitly by client with cancel message
                subscriptionPvNames3 = List.of("S01-GCC03", "S02-GCC03", "S03-BPM03");
                final int expectedResponseCount = 30;
                final boolean expectReject = false;
                final String expectedRejectMessage = "";
                subscribeDataCall3 =
                        ingestionServiceWrapper.initiateSubscribeDataRequest(
                                subscriptionPvNames3, expectedResponseCount, expectReject, expectedRejectMessage);
            }

            // run a simple ingestion scenario that will publish to all 3 subscriptions
            Map<String, GrpcIntegrationIngestionServiceWrapper.IngestionStreamInfo> ingestionValidationMap;
            {
                // create some data for testing query APIs
                // create data for 10 sectors, each containing 3 gauges and 3 bpms
                // named with prefix "S%02d-" followed by "GCC%02d" or "BPM%02d"
                // with 10 measurements per bucket, 1 bucket per second, and 10 buckets per pv
                ingestionValidationMap = annotationIngestionScenario(startSeconds);
            }

            // verify all 3 subscriptions received expected messages
            ingestionServiceWrapper.verifySubscribeDataResponse(
                    (IngestionTestBase.SubscribeDataResponseObserver) subscribeDataCall1.responseObserver(),
                    subscriptionPvNames1,
                    ingestionValidationMap,
                    30);
            ingestionServiceWrapper.verifySubscribeDataResponse(
                    (IngestionTestBase.SubscribeDataResponseObserver) subscribeDataCall2.responseObserver(),
                    subscriptionPvNames2,
                    ingestionValidationMap,
                    30);
            ingestionServiceWrapper.verifySubscribeDataResponse(
                    (IngestionTestBase.SubscribeDataResponseObserver) subscribeDataCall3.responseObserver(),
                    subscriptionPvNames3,
                    ingestionValidationMap,
                    30);

            // 2) cancel subscription with explicit cancel message
            ingestionServiceWrapper.cancelSubscribeDataCall(subscribeDataCall2);

            // 3) cancel subscription by closing client request stream
            ingestionServiceWrapper.closeSubscribeDataCall(subscribeDataCall3);
        }
    }

    public static SubscribeDataRequest buildSubscribeDataRequestNoPayload() {
        return SubscribeDataRequest.newBuilder().build();
    }

}
