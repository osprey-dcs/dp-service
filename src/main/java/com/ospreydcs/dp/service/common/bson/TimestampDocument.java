package com.ospreydcs.dp.service.common.bson;

public class TimestampDocument {

    // instance variables
    long seconds;
    long nanos;

    public long getSeconds() {
        return seconds;
    }

    public void setSeconds(long seconds) {
        this.seconds = seconds;
    }

    public long getNanos() {
        return nanos;
    }

    public void setNanos(long nanos) {
        this.nanos = nanos;
    }
}
