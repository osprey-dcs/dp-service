package com.ospreydcs.dp.service.integration.ingestionstream;

import com.ospreydcs.dp.grpc.v1.common.DataValue;
import com.ospreydcs.dp.grpc.v1.common.Timestamp;
import com.ospreydcs.dp.grpc.v1.ingestionstream.PvConditionTrigger;
import com.ospreydcs.dp.grpc.v1.ingestionstream.SubscribeDataEventResponse;
import com.ospreydcs.dp.service.ingest.IngestionTestBase;
import com.ospreydcs.dp.service.ingest.utility.SubscribeDataUtility;
import com.ospreydcs.dp.service.ingestionstream.IngestionStreamTestBase;
import com.ospreydcs.dp.service.integration.GrpcIntegrationTestBase;
import io.grpc.stub.StreamObserver;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertTrue;

public class SubscribeDataEventTest extends GrpcIntegrationTestBase {

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
    public void testSubscribeDataEvent() {

        {
            IngestionStreamTestBase.SubscribeDataEventCall subscribeDataEventCall1;
            IngestionStreamTestBase.SubscribeDataEventRequestParams requestParams1;
            Map<PvConditionTrigger, List<SubscribeDataEventResponse.Event>> expectedEventResponses = new HashMap<>();
            // invoke subscribeDataEvent()
            {
                // create list of triggers for subscription
                final List<IngestionStreamTestBase.PvConditionTriggerParams> triggerParamsList = new ArrayList<>();

                // create trigger
                final IngestionStreamTestBase.PvConditionTriggerParams triggerParams =
                        new IngestionStreamTestBase.PvConditionTriggerParams(
                            "S01-BPM01",
                            PvConditionTrigger.PvCondition.PV_CONDITION_EQUAL_TO,
                            DataValue.newBuilder().setDoubleValue(5.0).build());
                triggerParamsList.add(triggerParams);
                PvConditionTrigger trigger = PvConditionTrigger.newBuilder()
                        .setPvName(triggerParams.pvName())
                        .setCondition(triggerParams.condition())
                        .setValue(triggerParams.value())
                        .build();

                // create list of expected Events for trigger
                final List<SubscribeDataEventResponse.Event> triggerExpectedEvents = new ArrayList<>();
                final DataValue eventDataValue = DataValue.newBuilder().setDoubleValue(5.0).build();
                final SubscribeDataEventResponse.Event event = SubscribeDataEventResponse.Event.newBuilder()
                        .setTrigger(trigger)
                        .setDataValue(eventDataValue)
                        .setEventTime(Timestamp.newBuilder().setEpochSeconds(1698767467).build())
                        .build();
                triggerExpectedEvents.add(event);
                expectedEventResponses.put(trigger, triggerExpectedEvents);
                requestParams1 =
                        new IngestionStreamTestBase.SubscribeDataEventRequestParams(triggerParamsList);
                final int expectedResponseCount = 1; // expect one event message
                final boolean expectReject = false;
                final String expectedRejectMessage = "";
                subscribeDataEventCall1 =
                        initiateSubscribeDataEventRequest(
                                requestParams1,
                                expectedResponseCount,
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

            // verify response messages for subscribeDataEvent() responses
            verifySubscribeDataEventResponse(
                    (IngestionStreamTestBase.SubscribeDataEventResponseObserver) subscribeDataEventCall1.responseObserver(),
                    requestParams1,
                    ingestionScenarioResult.validationMap(),
                    expectedEventResponses);

            subscribeDataEventCall1.requestObserver().onCompleted();
        }
    }

}
