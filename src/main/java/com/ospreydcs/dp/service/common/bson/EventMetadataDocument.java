package com.ospreydcs.dp.service.common.bson;

import com.ospreydcs.dp.grpc.v1.common.EventMetadata;
import com.ospreydcs.dp.grpc.v1.common.Timestamp;

public class EventMetadataDocument {

    // instance variables
    private String description;
    private TimestampDocument startTime;
    private TimestampDocument stopTime;

    public String getDescription() {
        return description;
    }

    public void setDescription(String description) {
        this.description = description;
    }

    public TimestampDocument getStartTime() {
        return startTime;
    }

    public void setStartTime(TimestampDocument startTime) {
        this.startTime = startTime;
    }

    public TimestampDocument getStopTime() {
        return stopTime;
    }

    public void setStopTime(TimestampDocument stopTime) {
        this.stopTime = stopTime;
    }

    public static EventMetadataDocument fromEventMetadata(final EventMetadata eventMetadata) {

        final EventMetadataDocument eventMetadataDocument = new EventMetadataDocument();

        if (eventMetadata.getDescription() != null) {
            final String eventDescription = eventMetadata.getDescription();
            eventMetadataDocument.setDescription(eventDescription);
        }
        if (eventMetadata.hasStartTimestamp()) {
            final TimestampDocument startTimestampDocument =
                    TimestampDocument.fromTimestamp(eventMetadata.getStartTimestamp());
            eventMetadataDocument.setStartTime(startTimestampDocument);
        }
        if (eventMetadata.hasStopTimestamp()) {
            final TimestampDocument stopTimeDocument =
                    TimestampDocument.fromTimestamp(eventMetadata.getStopTimestamp());
            eventMetadataDocument.setStopTime(stopTimeDocument);
        }

        return eventMetadataDocument;
    }

    public EventMetadata toEventMetadata() {
        return toEventMetadata(this);
    }

    public static EventMetadata toEventMetadata(final EventMetadataDocument eventMetadataDocument) {

        final EventMetadata.Builder eventMetadataBuilder = EventMetadata.newBuilder();

        if (eventMetadataDocument.getDescription() != null) {
            final String eventDescription = eventMetadataDocument.getDescription();
            eventMetadataBuilder.setDescription(eventDescription);
        } else {
            eventMetadataBuilder.setDescription("");
        }

        if (eventMetadataDocument.getStartTime() != null) {
            final Timestamp startTimestamp = eventMetadataDocument.getStartTime().toTimestamp();
            eventMetadataBuilder.setStartTimestamp(startTimestamp);
        } else {
            eventMetadataBuilder.setStartTimestamp(Timestamp.newBuilder().build());
        }

        if (eventMetadataDocument.getStopTime() != null) {
            final Timestamp stopTimestamp = eventMetadataDocument.getStopTime().toTimestamp();
            eventMetadataBuilder.setStopTimestamp(stopTimestamp);
        } else {
            eventMetadataBuilder.setStopTimestamp(Timestamp.newBuilder().build());
        }

        return eventMetadataBuilder.build();
    }
    
}
