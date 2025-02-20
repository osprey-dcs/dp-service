package com.ospreydcs.dp.service.common.protobuf;

import com.ospreydcs.dp.grpc.v1.common.EventMetadata;
import com.ospreydcs.dp.grpc.v1.common.Timestamp;

public class EventMetadataUtility {

    public static class EventMetadataParams {

        // instance variables
        public final String description;
        public final Long startSeconds;
        public final Long startNanos;
        public final Long stopSeconds;
        public final Long stopNanos;

        public EventMetadataParams(String description, Long startSeconds, Long startNanos, Long stopSeconds, Long stopNanos) {
            this.description = description;
            this.startSeconds = startSeconds;
            this.startNanos = startNanos;
            this.stopSeconds = stopSeconds;
            this.stopNanos = stopNanos;
        }
    }

    public static EventMetadata eventMetadataFromParams(final EventMetadataParams eventMetadataParams) {
        
        final EventMetadata.Builder eventMetadataBuilder = EventMetadata.newBuilder();
        
        if (eventMetadataParams.description != null) {
            eventMetadataBuilder.setDescription(eventMetadataParams.description);
        }
        
        if ((eventMetadataParams.startSeconds != null) && (eventMetadataParams.startNanos != null)) {
            final Timestamp startTimestamp = Timestamp.newBuilder()
                    .setEpochSeconds(eventMetadataParams.startSeconds)
                    .setNanoseconds(eventMetadataParams.startNanos)
                    .build();
            eventMetadataBuilder.setStartTimestamp(startTimestamp);
        }

        if ((eventMetadataParams.stopSeconds != null) && (eventMetadataParams.stopNanos != null)) {
            final Timestamp stopTimestamp = Timestamp.newBuilder()
                    .setEpochSeconds(eventMetadataParams.stopSeconds)
                    .setNanoseconds(eventMetadataParams.stopNanos)
                    .build();
            eventMetadataBuilder.setStopTimestamp(stopTimestamp);
        }

        return eventMetadataBuilder.build();
    }
    
}
