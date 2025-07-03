package com.ospreydcs.dp.service.ingestionstream.handler;

import com.ospreydcs.dp.grpc.v1.common.DataValue;
import com.ospreydcs.dp.grpc.v1.common.Timestamp;
import com.ospreydcs.dp.grpc.v1.ingestionstream.DataEventOperation;
import com.ospreydcs.dp.grpc.v1.ingestionstream.PvConditionTrigger;
import com.ospreydcs.dp.grpc.v1.ingestionstream.SubscribeDataEventResponse;
import com.ospreydcs.dp.service.common.protobuf.TimestampUtility;
import com.ospreydcs.dp.service.ingestionstream.service.IngestionStreamServiceImpl;

import java.time.Instant;

public class TriggeredEvent {

    private final Timestamp triggerTimestamp;
    private final PvConditionTrigger trigger;
    private final DataEventOperation operation;
    private final DataValue dataValue;
    private final Instant beginTime;
    private final Instant endTime;
    private final Instant expirationTime;
    private final SubscribeDataEventResponse.Event event;

    public TriggeredEvent(
            Timestamp triggerTimestamp,
            PvConditionTrigger trigger,
            DataEventOperation operation,
            DataValue dataValue,
            long expirationDelayNanos
    ) {
        this.triggerTimestamp = triggerTimestamp;
        this.trigger = trigger;
        this.operation = operation;
        this.dataValue = dataValue;

        // set begin/end times from operation offset and duration
        final Instant triggerInstant = TimestampUtility.instantFromTimestamp(triggerTimestamp);
        beginTime = triggerInstant.plusNanos(operation.getWindow().getTimeInterval().getOffset());
        endTime = beginTime.plusNanos(operation.getWindow().getTimeInterval().getDuration());
        
        // set expiration time to endTime plus configurable delay
        expirationTime = endTime.plusNanos(expirationDelayNanos);

        // create a protobuf TriggeredEvent object for DataEvent responses
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

    public boolean isExpired() {
        return Instant.now().isAfter(expirationTime);
    }

    // Getters
    public Timestamp getTriggerTimestamp() { return triggerTimestamp; }
    public PvConditionTrigger getTrigger() { return trigger; }
    public DataEventOperation getOperation() { return operation; }
    public DataValue getDataValue() { return dataValue; }
    public Instant getBeginTime() { return beginTime; }
    public Instant getEndTime() { return endTime; }
    public Instant getExpirationTime() { return expirationTime; }
    public SubscribeDataEventResponse.Event getEvent() { return event; }
}