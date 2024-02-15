package com.ospreydcs.dp.service.ingest.model;

import com.ospreydcs.dp.grpc.v1.common.DataTimestamps;
import com.ospreydcs.dp.grpc.v1.common.SamplingClock;
import com.ospreydcs.dp.grpc.v1.common.Timestamp;
import com.ospreydcs.dp.service.common.grpc.GrpcUtility;

import java.time.Instant;
import java.util.List;

public class DataTimestampsModel {

    private List<Timestamp> timestampList = null;
    private SamplingClock samplingClock = null;

    public DataTimestampsModel(DataTimestamps dataTimestamps) {
        switch (dataTimestamps.getValueCase()) {
            case SAMPLINGCLOCK -> {
                samplingClock = dataTimestamps.getSamplingClock();
            }
            case TIMESTAMPLIST -> {
                timestampList = dataTimestamps.getTimestampList().getTimestampsList();
            }
        }
    }

    public Timestamp getFirstTimestamp() {
        if (timestampList != null) {
            return timestampList.get(0);
        } else if (samplingClock != null) {
            return samplingClock.getStartTime();
        } else {
            return null;
        }
    }

    public Timestamp getLastTimestamp() {
        if (timestampList != null) {
            return timestampList.get(timestampList.size() - 1);
        } else if (samplingClock != null) {
            long startSeconds = samplingClock.getStartTime().getEpochSeconds();
            long startNanos = samplingClock.getStartTime().getNanoseconds();
            Instant startInstant = Instant.ofEpochSecond(startSeconds, startNanos);
            Instant lastInstant =
                    startInstant.plusNanos(samplingClock.getPeriodNanos() * (samplingClock.getCount()-1));
            return GrpcUtility.getTimestampFromInstant(lastInstant);
        }
        return null;
    }

    public Long getSamplePeriodNanos() {
        if (samplingClock != null) {
            return samplingClock.getPeriodNanos();
        } else {
            return null;
        }
    }

    public Integer getSampleCount() {
        if (samplingClock != null) {
            return samplingClock.getCount();
        } else {
            return null;
        }
    }

}
