package com.ospreydcs.dp.service.common.grpc;

import com.ospreydcs.dp.grpc.v1.common.DataTimestamps;
import com.ospreydcs.dp.grpc.v1.common.SamplingClock;
import com.ospreydcs.dp.grpc.v1.common.Timestamp;
import com.ospreydcs.dp.grpc.v1.common.TimestampList;

import java.time.Instant;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

public class DataTimestampsUtility {

    public static class DataTimestampsModel {

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
                return TimestampUtility.getTimestampFromInstant(lastInstant);
            }
            return null;
        }

        public Long getSamplePeriodNanos() {
            if (samplingClock != null) {
                return samplingClock.getPeriodNanos();
            } else {
                return 0L;
            }
        }

        public Integer getSampleCount() {
            if (samplingClock != null) {
                return samplingClock.getCount();
            } else {
                return timestampList.size();
            }
        }
    }

    public static interface DataTimestampsIterator extends Iterator<Timestamp> {}

    public static class SamplingClockIterator implements DataTimestampsIterator {

        private final int sampleCount;
        private final long samplePeriod;

        private int iteratorCount = 0;
        private long iteratorSeconds;
        private long iteratorNanos;

        public SamplingClockIterator(SamplingClock samplingClock) {
            iteratorSeconds = samplingClock.getStartTime().getEpochSeconds();
            iteratorNanos = samplingClock.getStartTime().getNanoseconds();
            sampleCount = samplingClock.getCount();
            samplePeriod = samplingClock.getPeriodNanos();
        }

        @Override
        public boolean hasNext() {
            return iteratorCount < sampleCount;
        }

        @Override
        public Timestamp next() {

            // check if we are done
            if (!hasNext()) {
                throw new NoSuchElementException();
            }

            // build Timestamp for current seconds / nanos
            final Timestamp timestamp = Timestamp.newBuilder()
                    .setEpochSeconds(iteratorSeconds)
                    .setNanoseconds(iteratorNanos)
                    .build();

            // increment nanos, and increment seconds if nanos rolled over one billion
            iteratorNanos = iteratorNanos + samplePeriod;
            if (iteratorNanos >= 1_000_000_000) {
                iteratorSeconds = iteratorSeconds + 1;
                iteratorNanos = iteratorNanos - 1_000_000_000;
            }

            // increment iteratorCount so we know when we are done
            iteratorCount = iteratorCount + 1;

            return timestamp;
        }

    }

    public static class TimestampListIterator implements DataTimestampsIterator {

        private final Iterator<Timestamp> timestampIterator;

        public TimestampListIterator(TimestampList timestampList) {
            timestampIterator = timestampList.getTimestampsList().iterator();
        }

        @Override
        public boolean hasNext() {
            return timestampIterator.hasNext();
        }

        @Override
        public Timestamp next() {
            return timestampIterator.next();
        }

    }

    public static DataTimestampsIterator dataTimestampsIterator(DataTimestamps dataTimestamps) {

        if (dataTimestamps.hasSamplingClock()) {
            return new SamplingClockIterator(dataTimestamps.getSamplingClock());

        } else if (dataTimestamps.hasTimestampList()) {
            return new TimestampListIterator(dataTimestamps.getTimestampList());

        } else {
            return null;
        }
    }

}
