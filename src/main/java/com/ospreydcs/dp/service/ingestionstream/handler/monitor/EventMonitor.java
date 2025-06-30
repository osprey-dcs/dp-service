package com.ospreydcs.dp.service.ingestionstream.handler.monitor;

import com.ospreydcs.dp.grpc.v1.common.DataBucket;
import com.ospreydcs.dp.grpc.v1.common.DataColumn;
import com.ospreydcs.dp.grpc.v1.common.DataTimestamps;
import com.ospreydcs.dp.grpc.v1.common.DataValue;
import com.ospreydcs.dp.grpc.v1.common.Timestamp;
import com.ospreydcs.dp.grpc.v1.ingestion.SubscribeDataResponse;
import com.ospreydcs.dp.grpc.v1.ingestionstream.DataEventOperation;
import com.ospreydcs.dp.grpc.v1.ingestionstream.PvConditionTrigger;
import com.ospreydcs.dp.grpc.v1.ingestionstream.SubscribeDataEventRequest;
import com.ospreydcs.dp.grpc.v1.ingestionstream.SubscribeDataEventResponse;
import com.ospreydcs.dp.service.common.protobuf.DataTimestampsUtility;
import com.ospreydcs.dp.service.common.protobuf.TimestampUtility;
import com.ospreydcs.dp.service.ingestionstream.handler.DataBuffer;
import com.ospreydcs.dp.service.ingestionstream.handler.DataBufferManager;
import com.ospreydcs.dp.service.ingestionstream.handler.EventMonitorSubscriptionManager;
import com.ospreydcs.dp.service.ingestionstream.service.IngestionStreamServiceImpl;
import io.grpc.stub.StreamObserver;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.time.Instant;
import java.util.*;

public class EventMonitor {

    // constants
    private static final long MAX_MESSAGE_SIZE_BYTES = 4 * 1024 * 1024; // 4MB message size limit
    private static final long DEFAULT_FLUSH_INTERVAL = 500L; // 500ms flush interval
    private static final long DEFAULT_MAX_BUFFER_BYTES = 512 * 1024L; // 512KB max buffer size
    private static final int DEFAULT_MAX_BUFFER_ITEMS = 50; // 50 max items
    private static final long DEFAULT_BUFFER_AGE_LIMIT_NANOS = DataBuffer.DataBufferConfig.secondsToNanos(2); // 2 seconds max item age

    // static variables
    private static final Logger logger = LogManager.getLogger();

    // instance variables
    protected final SubscribeDataEventRequest.NewSubscription requestSubscription;
    public final StreamObserver<SubscribeDataEventResponse> responseObserver;
    protected final EventMonitorSubscriptionManager subscriptionManager;
    protected final Map<String, PvConditionTrigger> pvTriggerMap = new HashMap<>();
    protected final List<Event> triggeredEvents = new ArrayList<>();
    protected final Set<String> targetPvNames = new HashSet<>();
    protected final DataBufferManager bufferManager;

    // local type defs

    private static record TriggerResult(
            boolean isTriggered,
            boolean isError,
            String errorMsg
    ) {}

    protected static class Event {

        private final Timestamp triggerTimestamp;
        private final PvConditionTrigger trigger;
        private final DataEventOperation operation;
        private final DataValue dataValue;
        private final Instant beginTime;
        private final Instant endTime;
        private final SubscribeDataEventResponse.Event event;

        public Event(
                Timestamp triggerTimestamp,
                PvConditionTrigger trigger,
                DataEventOperation operation,
                DataValue dataValue
        ) {
            this.triggerTimestamp = triggerTimestamp;
            this.trigger = trigger;
            this.operation = operation;
            this.dataValue = dataValue;

            // set begin/end times from operation offset and duration
            final Instant triggerInstant = TimestampUtility.instantFromTimestamp(triggerTimestamp);
            beginTime = triggerInstant.plusNanos(operation.getWindow().getTimeInterval().getOffset());
            endTime = beginTime.plusNanos(operation.getWindow().getTimeInterval().getDuration());

            // create a protobuf Event object for DataEvent responses
            event = IngestionStreamServiceImpl.newEvent(triggerTimestamp, trigger, dataValue);
        }

        public boolean isTargetedByData(DataBuffer.BufferedData bufferedData) {
            // check if data item time targets this event (data begin time is before event end time and
            // data end time is after or equal to the event begin time)
            final Instant dataFirstInstant = bufferedData.getFirstInstant();
            final Instant dataLastInstant = bufferedData.getLastInstant();
            return ((dataFirstInstant.isBefore(this.endTime))
                    && (dataLastInstant.equals(this.beginTime) || (dataLastInstant.isAfter(this.beginTime))));
        }
    }

    public EventMonitor(
            SubscribeDataEventRequest.NewSubscription requestSubscription,
            StreamObserver<SubscribeDataEventResponse> responseObserver,
            EventMonitorSubscriptionManager subscriptionManager
    ) {
        // use negative offset value from request to determine buffer data age limit
        long negativeOffset = 0L;
        if (requestSubscription.hasOperation()) {
            final DataEventOperation operation = requestSubscription.getOperation();
            if (operation.hasWindow()) {
                final DataEventOperation.DataEventWindow window = operation.getWindow();
                if (window.hasTimeInterval()) {
                    final DataEventOperation.DataEventWindow.TimeInterval interval = window.getTimeInterval();
                    negativeOffset = Math.abs(interval.getOffset());
                }
            }
        }
        long bufferAgeLimit = Math.max(negativeOffset, DEFAULT_BUFFER_AGE_LIMIT_NANOS);
//        bufferAgeLimit = bufferAgeLimit + DataBuffer.DataBufferConfig.secondsToNanos(10);

        // create buffer config using age limit
        final DataBuffer.DataBufferConfig dataBufferConfig = new DataBuffer.DataBufferConfig(
                DEFAULT_FLUSH_INTERVAL,
                DEFAULT_MAX_BUFFER_BYTES,
                DEFAULT_MAX_BUFFER_ITEMS,
                bufferAgeLimit);

        this.requestSubscription = requestSubscription;
        this.responseObserver = responseObserver;
        this.subscriptionManager = subscriptionManager;

        // Create buffer manager with callback to process data
        this.bufferManager = new DataBufferManager(this::processBufferedData, dataBufferConfig);

        this.initialize(requestSubscription);
    }

    private static DataBuffer.DataBufferConfig createDefaultBufferConfig() {
        return new DataBuffer.DataBufferConfig(
            500L,      // 500ms flush interval - faster for event monitoring
            512 * 1024L, // 512KB max buffer size
            50,        // 50 max items
            DataBuffer.DataBufferConfig.secondsToNanos(2)  // 2 seconds max item age - faster processing for events
        );
    }

    private void initialize(SubscribeDataEventRequest.NewSubscription request) {

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
                handleTriggeredEvent(triggerTimestamp, trigger, dataValue);
            }

            columnValueIndex = columnValueIndex + 1;
        }
    }

    private void handleTriggeredEvent(
            Timestamp triggerTimestamp,
            PvConditionTrigger trigger,
            DataValue dataValue
    ) {
        // only add an event to triggered event list if the request includes a DataOperation
        if (requestSubscription.hasOperation()) {
            final Event event = new Event(triggerTimestamp, trigger, requestSubscription.getOperation(), dataValue);
            this.triggeredEvents.add(event);
        }

        // send an event message in the response stream
        IngestionStreamServiceImpl.sendSubscribeDataEventResponseEvent(
                triggerTimestamp,
                trigger,
                dataValue,
                this.responseObserver);
    }

    private void handleTargetPvData(
            String pvName,
            List<DataBuffer.BufferedData> bufferedDataList
    ) {
        if (bufferedDataList.isEmpty()) {
            return;
        }

        // iterate through each triggered event, dispatching messages in the response stream for data targeting that event
        for (Event event : this.triggeredEvents) {

            List<DataBucket> currentDataBuckets = new ArrayList<>();
            long currentMessageSize = 0;
            long baseMessageOverhead = 200; // Base overhead for EventData message structure

            // iterate through each data item, check if the data time targets the event
            for (DataBuffer.BufferedData bufferedData : bufferedDataList) {

                // only dispatch bufferedData in response stream if the data time targets this Event
                if ( ! event.isTargetedByData(bufferedData)) {
                    continue;
                }

                // Create DataBucket for this BufferedData item
                DataBucket dataBucket = DataBucket.newBuilder()
                        .setDataTimestamps(bufferedData.getDataTimestamps())
                        .setDataColumn(bufferedData.getDataColumn())
                        .build();

                long bucketSize = bufferedData.getEstimatedSize();

                // Check if adding this bucket would exceed message size limit
                if (!currentDataBuckets.isEmpty() &&
                        (currentMessageSize + bucketSize + baseMessageOverhead) > MAX_MESSAGE_SIZE_BYTES) {

                    // Send current batch and start a new one
                    sendDataEventMessage(event.event, currentDataBuckets);
                    currentDataBuckets.clear();
                    currentMessageSize = 0;
                }

                currentDataBuckets.add(dataBucket);
                currentMessageSize += bucketSize;

                logger.debug("Added DataBucket for PV: {}, current message size: {} bytes, {} buckets",
                        pvName, currentMessageSize, currentDataBuckets.size());
            }

            // Send any remaining data buckets
            if (!currentDataBuckets.isEmpty()) {
                sendDataEventMessage(event.event, currentDataBuckets);
            }
        }
    }

    private void sendDataEventMessage(
            SubscribeDataEventResponse.Event event,
            List<DataBucket> dataBuckets
    ) {
        if (dataBuckets.isEmpty()) {
            logger.debug("sendDataEventMessage received empty dataBuckets list");
            return;
        }

        // Create EventData with Event placeholder and DataBuckets
        SubscribeDataEventResponse.EventData eventData = SubscribeDataEventResponse.EventData.newBuilder()
                .setEvent(event)
                .addAllDataBuckets(dataBuckets)
                .build();

        IngestionStreamServiceImpl.sendSubscribeDataEventResponseEventData(eventData, responseObserver);
        
        logger.debug("Sent EventData message with {} data buckets", dataBuckets.size());
    }

    public void handleSubscribeDataResponse(SubscribeDataResponse.SubscribeDataResult result) {
        // Buffer the data instead of processing immediately
        for (DataColumn dataColumn : result.getDataColumnsList()) {
            bufferManager.bufferData(dataColumn.getName(), dataColumn, result.getDataTimestamps());
        }
    }

    private void processBufferedData(String pvName, List<DataBuffer.BufferedData> results) {

        // handle trigger PVs
        final PvConditionTrigger pvConditionTrigger = pvTriggerMap.get(pvName);
        if (pvConditionTrigger != null) {
            for (DataBuffer.BufferedData bufferedData : results) {
                final DataColumn dataColumn = bufferedData.getDataColumn();
                final DataTimestamps dataTimestamps = bufferedData.getDataTimestamps();
                handleTriggerPVData(pvConditionTrigger, dataColumn, dataTimestamps);
            }

        } else if (targetPvNames().contains(pvName)){
            // handle target PVs
            handleTargetPvData(pvName, results);
        } else {
            // this shouldn't happen, indicates we subscribed the EventMonitor to the wrong PVs...
            final String errorMsg = "unexpected PV received by EventMonitor: " + pvName;
            handleError(errorMsg);
        }
    }

    public void requestCancel() {
//        // use AtomicBoolean flag to control cancel, we only need one caller thread cleaning things up
//        if (canceled.compareAndSet(false, true)) {
//            handler.removeSourceMonitor(this);
//        }
        shutdown();
    }

    public void shutdown() {
        if (bufferManager != null) {
            bufferManager.shutdown();
        }
    }

    public DataBufferManager getBufferManager() {
        return bufferManager;
    }

}
