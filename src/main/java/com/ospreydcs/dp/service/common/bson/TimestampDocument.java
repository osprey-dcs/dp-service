package com.ospreydcs.dp.service.common.bson;

import com.ospreydcs.dp.grpc.v1.common.Timestamp;
import com.ospreydcs.dp.service.common.protobuf.TimestampUtility;

import java.util.Date;

public class TimestampDocument {

    // instance variables
    private long seconds;
    private long nanos;
    private Date dateTime;

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

    public Date getDateTime() {
        return dateTime;
    }

    public void setDateTime(Date dateTime) {
        this.dateTime = dateTime;
    }

    public static TimestampDocument fromTimestamp(Timestamp timestamp) {

        final TimestampDocument timestampDocument = new TimestampDocument();
        timestampDocument.setSeconds(timestamp.getEpochSeconds());
        timestampDocument.setNanos(timestamp.getNanoseconds());
        timestampDocument.setDateTime(TimestampUtility.dateFromTimestamp(timestamp));

        return timestampDocument;
    }

    public Timestamp toTimestamp() {
        return Timestamp.newBuilder()
                .setEpochSeconds(this.getSeconds())
                .setNanoseconds(this.getNanos())
                .build();
    }
}
