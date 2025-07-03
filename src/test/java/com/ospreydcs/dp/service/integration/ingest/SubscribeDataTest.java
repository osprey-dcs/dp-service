package com.ospreydcs.dp.service.integration.ingest;

import com.ospreydcs.dp.grpc.v1.ingestion.SubscribeDataRequest;
import com.ospreydcs.dp.service.ingest.IngestionTestBase;
import com.ospreydcs.dp.service.ingest.utility.SubscribeDataUtility;
import com.ospreydcs.dp.service.integration.GrpcIntegrationTestBase;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.List;

import static org.junit.Assert.assertTrue;

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
    public void testRejectEmptyPvNamesList() {
        {
            // negative test case for subscribeData() with empty list of PV names

            final List<String> subscriptionPvNames = List.of();
            final int expectedResponseCount = 30;
            final boolean expectReject = true;
            final String expectedRejectMessage = "SubscribeDataRequest.NewSubscription.pvNames list must not be empty";
            final SubscribeDataUtility.SubscribeDataCall subscribeDataCall =
                    initiateSubscribeDataRequest(
                            subscriptionPvNames, expectedResponseCount, expectReject, expectedRejectMessage);
        }
    }

    @Test
    public void testRejectNoRequestPayload() {
        {
            // negative test case for subscribeData() with no request payload (NewSubscription or CancelSubscription)

            final List<String> subscriptionPvNames = List.of("S01-GCC01", "S02-GCC01", "S03-BPM01");
            final int expectedResponseCount = 30;
            final boolean expectReject = true;
            final String expectedRejectMessage =
                    "received unknown request, expected NewSubscription or CancelSubscription";
            final SubscribeDataRequest request = buildSubscribeDataRequestNoPayload();
            final SubscribeDataUtility.SubscribeDataCall subscribeDataCall =
                    initiateSubscribeDataRequest(
                            request, expectedResponseCount, expectReject, expectedRejectMessage);
        }
    }

    @Test
    public void testRejectMultipleNewSubscriptionMessages() {

        // negative test case for subscribeData(), sending multiple NewSubscription messages causes reject

        final List<String> subscriptionPvNames = List.of("S09-GCC01", "S09-GCC01", "S09-BPM01");
        final int expectedResponseCount = 30;
        final boolean expectReject = false;
        final String expectedRejectMessage = "";
        final SubscribeDataUtility.SubscribeDataCall subscribeDataCall =
                initiateSubscribeDataRequest(
                        subscriptionPvNames, expectedResponseCount, expectReject, expectedRejectMessage);
        final SubscribeDataRequest duplicateRequest =
                SubscribeDataUtility.buildSubscribeDataRequest(subscriptionPvNames);

        // send duplicate NewSubscription message in request stream
        new Thread(() -> {
            subscribeDataCall.requestObserver().onNext(duplicateRequest);
        }).start();

        final String expectedDuplicateRejectMessage =
                "multiple NewSubscription messages not supported in request stream";
        final IngestionTestBase.SubscribeDataResponseObserver responseObserver =
                (IngestionTestBase.SubscribeDataResponseObserver) subscribeDataCall.responseObserver();
        responseObserver.awaitCloseLatch();
        assertTrue(responseObserver.isError());
        assertTrue(responseObserver.getErrorMessage().contains(expectedDuplicateRejectMessage));
    }

    @Test
    public void testSubscribeDataPositive() {

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
                        initiateSubscribeDataRequest(
                                subscriptionPvNames1, expectedResponseCount, expectReject, expectedRejectMessage);
            }

            {
                // 2) invoke subscribeData with call that will be closed explicitly by client with cancel message
                subscriptionPvNames2 = List.of("S01-GCC02", "S02-GCC02", "S03-BPM02");
                final int expectedResponseCount = 30;
                final boolean expectReject = false;
                final String expectedRejectMessage = "";
                subscribeDataCall2 =
                        initiateSubscribeDataRequest(
                                subscriptionPvNames2, expectedResponseCount, expectReject, expectedRejectMessage);
            }

            {
                // 3) invoke subscribeData with call that will be closed explicitly by client with cancel message
                subscriptionPvNames3 = List.of("S01-GCC03", "S02-GCC03", "S03-BPM03");
                final int expectedResponseCount = 30;
                final boolean expectReject = false;
                final String expectedRejectMessage = "";
                subscribeDataCall3 =
                        initiateSubscribeDataRequest(
                                subscriptionPvNames3, expectedResponseCount, expectReject, expectedRejectMessage);
            }

            // run a simple ingestion scenario that will publish to all 3 subscriptions
            IngestionScenarioResult ingestionScenarioResult;
            {
                // create some data for testing query APIs
                // create data for 10 sectors, each containing 3 gauges and 3 bpms
                // named with prefix "S%02d-" followed by "GCC%02d" or "BPM%02d"
                // with 10 measurements per bucket, 1 bucket per second, and 10 buckets per pv
                ingestionScenarioResult = simpleIngestionScenario(null);
            }

            // verify all 3 subscriptions received expected messages
            verifySubscribeDataResponse(
                    (IngestionTestBase.SubscribeDataResponseObserver) subscribeDataCall1.responseObserver(),
                    subscriptionPvNames1,
                    ingestionScenarioResult.validationMap(),
                    0);
            verifySubscribeDataResponse(
                    (IngestionTestBase.SubscribeDataResponseObserver) subscribeDataCall2.responseObserver(),
                    subscriptionPvNames2,
                    ingestionScenarioResult.validationMap(),
                    0);
            verifySubscribeDataResponse(
                    (IngestionTestBase.SubscribeDataResponseObserver) subscribeDataCall3.responseObserver(),
                    subscriptionPvNames3,
                    ingestionScenarioResult.validationMap(),
                    0);

            // 2) cancel subscription with explicit cancel message
            cancelSubscribeDataCall(subscribeDataCall2);

            // 3) cancel subscription by closing client request stream
            closeSubscribeDataCall(subscribeDataCall3);
        }
    }

    public static SubscribeDataRequest buildSubscribeDataRequestNoPayload() {
        return SubscribeDataRequest.newBuilder().build();
    }

}
