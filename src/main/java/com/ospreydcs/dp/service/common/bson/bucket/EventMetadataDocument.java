package com.ospreydcs.dp.service.common.bson.bucket;

import com.ospreydcs.dp.grpc.v1.common.EventMetadata;
import com.ospreydcs.dp.grpc.v1.common.Timestamp;

public class EventMetadataDocument {

    // instance variables
    private String description;
    private long startSeconds;
    private long startNanos;
    private long stopSeconds;
    private long stopNanos;

    public String getDescription() {
        return description;
    }

    public void setDescription(String description) {
        this.description = description;
    }

    public long getStartSeconds() {
        return startSeconds;
    }

    public void setStartSeconds(long startSeconds) {
        this.startSeconds = startSeconds;
    }

    public long getStartNanos() {
        return startNanos;
    }

    public void setStartNanos(long startNanos) {
        this.startNanos = startNanos;
    }

    public long getStopSeconds() {
        return stopSeconds;
    }

    public void setStopSeconds(long stopSeconds) {
        this.stopSeconds = stopSeconds;
    }

    public long getStopNanos() {
        return stopNanos;
    }

    public void setStopNanos(long stopNanos) {
        this.stopNanos = stopNanos;
    }
    
    public static EventMetadataDocument fromEventMetadata(final EventMetadata eventMetadata) {
        
        String eventDescription = "";
        long eventStartSeconds = 0;
        long eventStartNanos = 0;
        long eventStopSeconds = 0;
        long eventStopNanos = 0;

        if (eventMetadata.getDescription() != null) {
            eventDescription = eventMetadata.getDescription();
        }
        if (eventMetadata.hasStartTimestamp()) {
            eventStartSeconds = eventMetadata.getStartTimestamp().getEpochSeconds();
            eventStartNanos = eventMetadata.getStartTimestamp().getNanoseconds();
        }
        if (eventMetadata.hasStopTimestamp()) {
            eventStopSeconds = eventMetadata.getStopTimestamp().getEpochSeconds();
            eventStopNanos = eventMetadata.getStopTimestamp().getNanoseconds();
        }

        final EventMetadataDocument eventMetadataDocument = new EventMetadataDocument();
        eventMetadataDocument.setDescription(eventDescription);
        eventMetadataDocument.setStartSeconds(eventStartSeconds);
        eventMetadataDocument.setStartNanos(eventStartNanos);
        eventMetadataDocument.setStopSeconds(eventStopSeconds);
        eventMetadataDocument.setStopNanos(eventStopNanos);

        return eventMetadataDocument;
    }

    public static EventMetadata toEventMetadata(final EventMetadataDocument eventMetadataDocument) {
        
        final Timestamp startTimestamp = Timestamp.newBuilder()
                .setEpochSeconds(eventMetadataDocument.getStartSeconds())
                .setNanoseconds(eventMetadataDocument.getStartNanos())
                .build();

        final Timestamp stopTimestamp = Timestamp.newBuilder()
                .setEpochSeconds(eventMetadataDocument.getStopSeconds())
                .setNanoseconds(eventMetadataDocument.getStopNanos())
                .build();

        final EventMetadata eventMetadata = EventMetadata.newBuilder()
                .setDescription(eventMetadataDocument.getDescription())
                .setStartTimestamp(startTimestamp)
                .setStopTimestamp(stopTimestamp)
                .build();

        return eventMetadata;
    }
    
}
