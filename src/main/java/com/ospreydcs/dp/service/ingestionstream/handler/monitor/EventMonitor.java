package com.ospreydcs.dp.service.ingestionstream.handler.monitor;

import com.ospreydcs.dp.grpc.v1.common.DataColumn;
import com.ospreydcs.dp.grpc.v1.common.DataTimestamps;
import com.ospreydcs.dp.grpc.v1.common.DataValue;
import com.ospreydcs.dp.grpc.v1.common.Timestamp;
import com.ospreydcs.dp.grpc.v1.ingestion.SubscribeDataResponse;
import com.ospreydcs.dp.grpc.v1.ingestionstream.PvConditionTrigger;
import com.ospreydcs.dp.grpc.v1.ingestionstream.SubscribeDataEventRequest;
import com.ospreydcs.dp.grpc.v1.ingestionstream.SubscribeDataEventResponse;
import com.ospreydcs.dp.service.common.protobuf.DataTimestampsUtility;
import com.ospreydcs.dp.service.common.protobuf.TimestampUtility;
import com.ospreydcs.dp.service.ingestionstream.handler.DataEventSubscriptionManager;
import com.ospreydcs.dp.service.ingestionstream.service.IngestionStreamServiceImpl;
import io.grpc.stub.StreamObserver;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.time.Instant;
import java.util.*;

public class EventMonitor {

    // static variables
    private static final Logger logger = LogManager.getLogger();

    // instance variables
    protected final SubscribeDataEventRequest request;
    protected final StreamObserver<SubscribeDataEventResponse> responseObserver;
    protected final DataEventSubscriptionManager subscriptionManager;
    protected final Map<String, PvConditionTrigger> pvTriggerMap = new HashMap<>();
    protected final List<Event> triggeredEvents = new ArrayList<>();
    protected final Set<String> targetPvNames = new HashSet<>();

    // local type defs

    private static record TriggerResult(
            boolean isTriggered,
            boolean isError,
            String errorMsg
    ) {}

    protected static class Event {

        private final Timestamp triggerTimestamp;
        private final PvConditionTrigger trigger;
        private final SubscribeDataEventRequest.DataEventOperation operation;

        public Event(
                Timestamp triggerTimestamp,
                PvConditionTrigger trigger,
                SubscribeDataEventRequest.DataEventOperation operation
        ) {
            this.triggerTimestamp = triggerTimestamp;
            this.trigger = trigger;
            this.operation = operation;
        }
    }

    public EventMonitor(
            SubscribeDataEventRequest request,
            StreamObserver<SubscribeDataEventResponse> responseObserver,
            DataEventSubscriptionManager subscriptionManager
    ) {
        this.request = request;
        this.responseObserver = responseObserver;
        this.subscriptionManager = subscriptionManager;
        this.initialize(request);
    }

    private void initialize(SubscribeDataEventRequest request) {

        // initialize pvTriggerMap from request
        for (PvConditionTrigger trigger : request.getTriggersList()) {
            pvTriggerMap.put(trigger.getPvName(), trigger);
        }

        // initialize targetPvNames from request
        targetPvNames.addAll(request.getOperation().getTargetPvsList());
    }

    protected void handleError(
            String errorMsg
    ) {
        logger.debug("handleError msg: {}", errorMsg);
        subscriptionManager.cancelEventMonitor(this);
        IngestionStreamServiceImpl.sendSubscribeDataEventResponseError(errorMsg, responseObserver);
    }

    private Set<String> triggerPvNames() {
        return pvTriggerMap.keySet();
    }

    private Set<String> targetPvNames() {
        return targetPvNames;
    }

    public Set<String> getPvNames() {

        final Set<String> pvNames = new HashSet<>();

        // add names for trigger PVs
        pvNames.addAll(triggerPvNames());

        // add names for target PVs
        pvNames.addAll(targetPvNames());

        return pvNames;
    }

    private static <T extends Comparable<T>> TriggerResult checkTrigger(
            T typedDataValue,
            T typedTriggerValue,
            PvConditionTrigger.PvCondition triggerCondition
    ) {
        final int compareResult = typedDataValue.compareTo(typedTriggerValue);

        switch (triggerCondition) {
            case PV_CONDITION_UNSPECIFIED -> {
                final String errorMsg = "PvConditionTrigger.condition must be specified";
                return new TriggerResult(false, true, errorMsg);
            }
            case PV_CONDITION_EQUAL_TO -> {
                return new TriggerResult(compareResult == 0, false, "");
            }
            case PV_CONDITION_GREATER -> {
                return new TriggerResult(compareResult > 0, false, "");
            }
            case PV_CONDITION_GREATER_EQ -> {
                return new TriggerResult(compareResult >= 0, false, "");
            }
            case PV_CONDITION_LESS -> {
                return new TriggerResult(compareResult < 0, false, "");
            }
            case PV_CONDITION_LESS_EQ -> {
                return new TriggerResult(compareResult <= 0, false, "");
            }
            case UNRECOGNIZED -> {
                final String errorMsg = "PvConditionTrigger.condition unrecognized enum value";
                return new TriggerResult(false, true, errorMsg);
            }
        }

        final String errorMsg = "PvConditionTrigger.condition unhandled condition: " + triggerCondition;
        return new TriggerResult(false, true, errorMsg);
    }

    private void handleTriggerPVData(
            PvConditionTrigger trigger,
            DataColumn dataColumn,
            DataTimestamps dataTimestamps
    ) {
        final String columnPvName = dataColumn.getName();

        // check if each column data value triggers the event
        int columnValueIndex = 0;
        for (DataValue dataValue : dataColumn.getDataValuesList()) {

            final PvConditionTrigger.PvCondition triggerCondition = trigger.getCondition();
            final DataValue triggerValue = trigger.getValue();

            // check for type mismatch between column data value and trigger value
            if (dataValue.getValueCase() != triggerValue.getValueCase()) {
                final String errorMsg = "PvConditionTrigger type mismatch PV name: " + columnPvName
                        + " PV data type: " + dataValue.getValueCase().name()
                        + " trigger value data type: " + triggerValue.getValueCase().name();
                handleError(errorMsg);
                return;
            }

            // check if event condition is triggered by data value
            boolean isTriggered = false;
            boolean isError = false;
            String resultErrorMsg = "";
            TriggerResult triggerResult = null;
            switch (dataValue.getValueCase()) {
                
                case STRINGVALUE -> {
                    final String typedDataValue = dataValue.getStringValue();
                    final String typedTriggerValue = triggerValue.getStringValue();
                    triggerResult = checkTrigger(typedDataValue, typedTriggerValue, triggerCondition);
                }
                
                case BOOLEANVALUE -> {
                    final Boolean typedDataValue = dataValue.getBooleanValue();
                    final Boolean typedTriggerValue = triggerValue.getBooleanValue();
                    triggerResult = checkTrigger(typedDataValue, typedTriggerValue, triggerCondition);
                }
                
                case UINTVALUE -> {
                    final int typedDataValue = dataValue.getUintValue();
                    final int typedTriggerValue = triggerValue.getUintValue();
                    triggerResult = checkTrigger(typedDataValue, typedTriggerValue, triggerCondition);
                }
                
                case ULONGVALUE -> {
                    final long typedDataValue = dataValue.getUlongValue();
                    final long typedTriggerValue = triggerValue.getUlongValue();
                    triggerResult = checkTrigger(typedDataValue, typedTriggerValue, triggerCondition);
                }
                
                case INTVALUE -> {
                    final int typedDataValue = dataValue.getIntValue();
                    final int typedTriggerValue = triggerValue.getIntValue();
                    triggerResult = checkTrigger(typedDataValue, typedTriggerValue, triggerCondition);
                }

                case LONGVALUE -> {
                    final long typedDataValue = dataValue.getLongValue();
                    final long typedTriggerValue = triggerValue.getLongValue();
                    triggerResult = checkTrigger(typedDataValue, typedTriggerValue, triggerCondition);
                }
                
                case FLOATVALUE -> {
                    final float typedDataValue = dataValue.getFloatValue();
                    final float typedTriggerValue = triggerValue.getFloatValue();
                    triggerResult = checkTrigger(typedDataValue, typedTriggerValue, triggerCondition);
                }

                case DOUBLEVALUE -> {
                    final double typedDataValue = dataValue.getDoubleValue();
                    final double typedTriggerValue = triggerValue.getDoubleValue();
                    triggerResult = checkTrigger(typedDataValue, typedTriggerValue, triggerCondition);
                }

                case TIMESTAMPVALUE -> {
                    final Instant dataValueInstant =
                            TimestampUtility.instantFromTimestamp(dataValue.getTimestampValue());
                    final Instant triggerValueInstant =
                            TimestampUtility.instantFromTimestamp(triggerValue.getTimestampValue());
                    triggerResult = checkTrigger(dataValueInstant, triggerValueInstant, triggerCondition);
                }

                case BYTEARRAYVALUE, ARRAYVALUE, STRUCTUREVALUE, IMAGEVALUE -> {
                    resultErrorMsg = "PvConditionTrigger PV data type not supported: " + columnPvName
                            + " PV data type: " + dataValue.getValueCase().name();
                    isError = true;
                }

                case VALUE_NOT_SET -> {
                    resultErrorMsg = "PvConditionTrigger PV data type not specified: " + columnPvName;
                    isError = true;
                }

            }

            if (triggerResult != null) {
                isTriggered = triggerResult.isTriggered();
                isError = triggerResult.isError();
                resultErrorMsg = triggerResult.errorMsg();
            }

            if (isError) {
                final String errorMsg =
                        "PvConditionTrigger error comparing data value for PV name: " + columnPvName
                                + " msg: " + resultErrorMsg;
                handleError(errorMsg);
                return;
            }

            if (isTriggered) {
                final Timestamp triggerTimestamp =
                        DataTimestampsUtility.timestampForIndex(dataTimestamps, columnValueIndex);
                if (triggerTimestamp == null) {
                    final String errorMsg = "PvConditionTrigger error getting timestamp for PV: " + columnPvName;
                    handleError(errorMsg);
                }
                addEvent(triggerTimestamp, trigger, dataValue);
            }

            columnValueIndex = columnValueIndex + 1;
        }
    }

    private void addEvent(
            Timestamp triggerTimestamp,
            PvConditionTrigger trigger,
            DataValue dataValue
    ) {
        final Event event = new Event(triggerTimestamp, trigger, request.getOperation());
        this.triggeredEvents.add(event);
        IngestionStreamServiceImpl.sendSubscribeDataEventResponseEvent(
                triggerTimestamp,
                trigger,
                dataValue,
                this.responseObserver);
    }

    private void handleTargetPvData(
            DataColumn dataColumn,
            DataTimestamps resultTimestamps
    ) {
    }

    public void handleSubscribeDataResponse(SubscribeDataResponse.SubscribeDataResult result) {

        final DataTimestamps resultTimestamps = result.getDataTimestamps();

        for (DataColumn dataColumn : result.getDataColumnsList()) {

            final String columnPvName = dataColumn.getName();

            final PvConditionTrigger pvConditionTrigger = pvTriggerMap.get(columnPvName);
            if (pvConditionTrigger != null) {
                handleTriggerPVData(pvConditionTrigger, dataColumn, resultTimestamps);

            } else if (targetPvNames().contains(columnPvName)) {
                handleTargetPvData(dataColumn, resultTimestamps);

            } else {
                // this shouldn't happen, indicates we subscribed the EventMonitor to the wrong PVs...
                final String errorMsg = "unexpected PV received by EventMonitor: " + columnPvName;
                handleError(errorMsg);
                return;
            }
        }
    }

}
