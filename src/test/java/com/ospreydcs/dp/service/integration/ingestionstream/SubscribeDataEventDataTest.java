package com.ospreydcs.dp.service.integration.ingestionstream;

import com.ospreydcs.dp.grpc.v1.common.DataValue;
import com.ospreydcs.dp.grpc.v1.common.Timestamp;
import com.ospreydcs.dp.grpc.v1.ingestionstream.PvConditionTrigger;
import com.ospreydcs.dp.grpc.v1.ingestionstream.SubscribeDataEventResponse;
import com.ospreydcs.dp.service.ingestionstream.IngestionStreamTestBase;
import com.ospreydcs.dp.service.integration.GrpcIntegrationTestBase;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class SubscribeDataEventDataTest extends GrpcIntegrationTestBase {

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
    public void testSubscribeDataEventData() {

        // 1. request 1. positive subscribeDataEvent() test: single trigger with value = 5.0 for PV S01-BPM01
        // Specify DataEventOperation that includes: a 3 second negative offset, a 5 second duration,
        // for target PVs S01-BPM02, S01-BPM03.
        IngestionStreamTestBase.SubscribeDataEventCall subscribeDataEventCall;
        IngestionStreamTestBase.SubscribeDataEventRequestParams requestParams1;
        Map<PvConditionTrigger, List<SubscribeDataEventResponse.Event>> expectedEventResponses1 = new HashMap<>();
        Map<SubscribeDataEventResponse.Event, Map<String, List<Instant>>> expectedEventDataResponses1 = new HashMap<>();
        int expectedEventResponseCount1 = 0;
        {
            // create list of triggers for request
            List<PvConditionTrigger> requestTriggers = new ArrayList<>();

            // create trigger
            SubscribeDataEventResponse.Event event1;
            {
                PvConditionTrigger trigger = PvConditionTrigger.newBuilder()
                        .setPvName("S01-BPM01")
                        .setCondition(PvConditionTrigger.PvCondition.PV_CONDITION_EQUAL_TO)
                        .setValue(DataValue.newBuilder().setDoubleValue(5.0).build())
                        .build();
                requestTriggers.add(trigger);

                // add entry to response verification map with trigger and expected Event responses
                final List<SubscribeDataEventResponse.Event> triggerExpectedEvents = new ArrayList<>();
                final DataValue eventDataValue = DataValue.newBuilder().setDoubleValue(5.0).build();
                event1 = SubscribeDataEventResponse.Event.newBuilder()
                        .setTrigger(trigger)
                        .setDataValue(eventDataValue)
                        .setEventTime(Timestamp.newBuilder().setEpochSeconds(1698767467).build())
                        .build();
                triggerExpectedEvents.add(event1);
                expectedEventResponses1.put(trigger, triggerExpectedEvents);
                expectedEventResponseCount1 += triggerExpectedEvents.size();
            }

            // DataEventOperation details for params
            final String pvName1 = "S01-BPM02";
            final String pvName2 = "S01-BPM03";
            final List<String> targetPvs = List.of(pvName1, pvName2);
            final long offset = -3_000_000_000L; // 3 seconds negative trigger time offset
            final long duration = 5_000_000_000L; // 5 second duration

            // add entry for event1 to response verification map with details about expected EventData responses
            final int expectedDataBucketCount = 10;
            final List<Instant> instantList = List.of(
                    Instant.ofEpochSecond(1698767462L + 2),
                    Instant.ofEpochSecond(1698767462L + 3),
                    Instant.ofEpochSecond(1698767462L + 4),
                    Instant.ofEpochSecond(1698767462L + 5),
                    Instant.ofEpochSecond(1698767462L + 6)
            );
            final Map<String, List<Instant>> pvInstantMap = new HashMap<>();
            expectedEventDataResponses1.put(event1, pvInstantMap);
            pvInstantMap.put(pvName1, instantList);
            pvInstantMap.put(pvName2, instantList);

            // create params object (including trigger params list) for building protobuf request from params
            requestParams1 =
                    new IngestionStreamTestBase.SubscribeDataEventRequestParams(
                            requestTriggers,
                            targetPvs,
                            offset,
                            duration);

            // call subscribeDataEvent() to initiate subscription before running ingestion
            final boolean expectReject = false;
            final String expectedRejectMessage = "";
            subscribeDataEventCall =
                    initiateSubscribeDataEventRequest(
                            requestParams1,
                            expectedEventResponseCount1,
                            expectedDataBucketCount,
                            expectReject,
                            expectedRejectMessage);
        }

        // run a simple ingestion scenario that will publish data relevant to subscriptions
        IngestionScenarioResult ingestionScenarioResult;
        {
            // create some data for testing query APIs
            // create data for 10 sectors, each containing 3 gauges and 3 bpms
            // named with prefix "S%02d-" followed by "GCC%02d" or "BPM%02d"
            // with 10 measurements per bucket, 1 bucket per second, and 10 buckets per pv
            ingestionScenarioResult = simpleIngestionScenario();
        }

        // request 1: verify subscribeDataEvent() responses and close request stream
        verifySubscribeDataEventResponse(
                (IngestionStreamTestBase.SubscribeDataEventResponseObserver) subscribeDataEventCall.responseObserver(),
                requestParams1,
                ingestionScenarioResult.validationMap(),
                expectedEventResponses1,
                expectedEventDataResponses1);
        subscribeDataEventCall.requestObserver().onCompleted();

    }

}
