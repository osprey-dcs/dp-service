package com.ospreydcs.dp.service.common.bson.bucket;

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
}
