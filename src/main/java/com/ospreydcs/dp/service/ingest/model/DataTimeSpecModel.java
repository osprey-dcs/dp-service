package com.ospreydcs.dp.service.ingest.model;

import com.ospreydcs.dp.grpc.v1.common.DataTimeSpec;
import com.ospreydcs.dp.grpc.v1.common.Timestamp;
import com.ospreydcs.dp.grpc.v1.common.FixedIntervalTimestampSpec;
import com.ospreydcs.dp.service.ingest.service.IngestionServiceImpl;

import java.time.Instant;
import java.util.List;

public class DataTimeSpecModel {

    private List<Timestamp> timestampList = null;
    private FixedIntervalTimestampSpec fixedIntervalSpec = null;

    public DataTimeSpecModel(DataTimeSpec timeSpec) {
        switch (timeSpec.getValueOneofCase()) {
            case FIXEDINTERVALTIMESTAMPSPEC -> {
                fixedIntervalSpec = timeSpec.getFixedIntervalTimestampSpec();
            }
            case TIMESTAMPLIST -> {
                timestampList = timeSpec.getTimestampList().getTimestampsList();
            }
        }
    }

    public Timestamp getFirstTimestamp() {
        if (timestampList != null) {
            return timestampList.get(0);
        } else if (fixedIntervalSpec != null) {
            return fixedIntervalSpec.getStartTime();
        } else {
            return null;
        }
    }

    public Timestamp getLastTimestamp() {
        if (timestampList != null) {
            return timestampList.get(timestampList.size() - 1);
        } else if (fixedIntervalSpec != null) {
            long startSeconds = fixedIntervalSpec.getStartTime().getEpochSeconds();
            long startNanos = fixedIntervalSpec.getStartTime().getNanoseconds();
            Instant startInstant = Instant.ofEpochSecond(startSeconds, startNanos);
            Instant lastInstant =
                    startInstant.plusNanos(fixedIntervalSpec.getSampleIntervalNanos() * (fixedIntervalSpec.getNumSamples()-1));
            return IngestionServiceImpl.getTimestampFromInstant(lastInstant);
        }
        return null;
    }

    public Long getSampleFrequency() {
        if (fixedIntervalSpec != null) {
            return fixedIntervalSpec.getSampleIntervalNanos();
        } else {
            return null;
        }
    }

    public Integer getNumSamples() {
        if (fixedIntervalSpec != null) {
            return fixedIntervalSpec.getNumSamples();
        } else {
            return null;
        }
    }

}
