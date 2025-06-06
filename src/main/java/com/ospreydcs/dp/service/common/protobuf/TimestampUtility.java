package com.ospreydcs.dp.service.common.protobuf;

import com.ospreydcs.dp.grpc.v1.common.Timestamp;

import java.time.Instant;
import java.util.Date;

public class TimestampUtility {


    public static Timestamp timestampFromSeconds(long epochSecs, long nanos) {
        return Timestamp.newBuilder().setEpochSeconds(epochSecs).setNanoseconds(nanos).build();
    }

    public static Date dateFromTimestamp(Timestamp timestamp) {
        final Instant timestampInstant = Instant.ofEpochSecond(
                timestamp.getEpochSeconds(), timestamp.getNanoseconds());
        return Date.from(timestampInstant);
    }

    public static Instant instantFromTimestamp(Timestamp timestamp) {
        return Instant.ofEpochSecond(timestamp.getEpochSeconds(), timestamp.getNanoseconds());
    }

    public static int compare(Timestamp timestamp1, Timestamp timestamp2) {
        final Instant timestamp1Instant = Instant.ofEpochSecond(
                timestamp1.getEpochSeconds(), timestamp1.getNanoseconds());
        final Instant timestamp2Instant = Instant.ofEpochSecond(
                timestamp2.getEpochSeconds(), timestamp2.getNanoseconds());
        return timestamp1Instant.compareTo(timestamp2Instant);
    }

    public static Timestamp getTimestampFromInstant(Instant instant) {
        return Timestamp.newBuilder()
                .setEpochSeconds(instant.getEpochSecond())
                .setNanoseconds(instant.getNano())
                .build();
    }

    public static Timestamp getTimestampNow() {
        Instant instantNow = Instant.now();
        return getTimestampFromInstant(instantNow);
    }

}
