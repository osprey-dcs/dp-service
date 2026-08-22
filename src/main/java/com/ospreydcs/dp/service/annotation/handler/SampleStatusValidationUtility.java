package com.ospreydcs.dp.service.annotation.handler;

import com.ospreydcs.dp.grpc.v1.annotation.DeleteSampleStatusesRequest;
import com.ospreydcs.dp.grpc.v1.annotation.QuerySampleStatusesRequest;
import com.ospreydcs.dp.grpc.v1.annotation.SaveSampleStatusesRequest;
import com.ospreydcs.dp.grpc.v1.common.DataTimestamps;
import com.ospreydcs.dp.grpc.v1.common.SampleStatusColumn;
import com.ospreydcs.dp.grpc.v1.common.SampleStatusFrame;
import com.ospreydcs.dp.grpc.v1.common.SamplingClock;
import com.ospreydcs.dp.grpc.v1.common.TimeRange;
import com.ospreydcs.dp.grpc.v1.common.Timestamp;
import com.ospreydcs.dp.service.common.model.ResultStatus;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Validation for the Sample Status API requests, per the contract in annotation.proto. Requests
 * are validated and rejected as a whole: nothing is persisted on rejection.
 *
 * <p>Note: unlike ingestion's data frames, sample status frames have no maximum time span — sparse
 * labeling over an arbitrarily wide range (e.g. a TimestampList naming a handful of suspect
 * points across months) is first-class. The only size limit is the configurable cap on total
 * statuses per request.
 */
public class SampleStatusValidationUtility {

    /**
     * Largest epochSeconds whose conversion to signed 64-bit epoch-nanos does not overflow
     * (~year 2262). Every Sample Status timestamp is validated against this, because the storage
     * and query paths key on epoch-nanos scalars throughout.
     */
    static final long MAX_EPOCH_SECONDS = Long.MAX_VALUE / 1_000_000_000L;

    public static ResultStatus validateSaveSampleStatusesRequest(
            SaveSampleStatusesRequest request,
            long maxStatusesPerRequest
    ) {
        if (request.getFramesList().isEmpty()) {
            return new ResultStatus(true,
                    "SaveSampleStatusesRequest.frames must contain at least one SampleStatusFrame");
        }

        long totalStatuses = 0;
        for (int frameIndex = 0; frameIndex < request.getFramesCount(); frameIndex++) {
            final SampleStatusFrame frame = request.getFrames(frameIndex);
            final String framePath = "SaveSampleStatusesRequest.frames[" + frameIndex + "]";

            if (frame.getDomain().isBlank()) {
                return new ResultStatus(true, framePath + ".domain must be specified");
            }
            if (frame.getLayer().isBlank()) {
                return new ResultStatus(true, framePath + ".layer must be specified");
            }
            if (!frame.hasDataTimestamps()) {
                return new ResultStatus(true, framePath + ".dataTimestamps must be provided");
            }

            final ResultStatus timestampsStatus =
                    validateFrameDataTimestamps(framePath, frame.getDataTimestamps());
            if (timestampsStatus.isError) {
                return timestampsStatus;
            }
            final int timestampCount = timestampCount(frame.getDataTimestamps());

            if (frame.getStatusColumnsList().isEmpty()) {
                return new ResultStatus(true, framePath + " must contain at least one statusColumn");
            }

            final Set<String> framePvNames = new HashSet<>();
            for (int columnIndex = 0; columnIndex < frame.getStatusColumnsCount(); columnIndex++) {
                final SampleStatusColumn column = frame.getStatusColumns(columnIndex);
                final String columnPath = framePath + ".statusColumns[" + columnIndex + "]";

                if (column.getPvName().isBlank()) {
                    return new ResultStatus(true, columnPath + ".pvName must be specified");
                }
                if (!framePvNames.add(column.getPvName())) {
                    return new ResultStatus(true, framePath
                            + " contains more than one statusColumn for PV: " + column.getPvName());
                }
                if (column.getStatusCodesCount() != timestampCount) {
                    return new ResultStatus(true, columnPath + ".statusCodes.length mismatch: expected "
                            + timestampCount + ", got: " + column.getStatusCodesCount());
                }
                if (column.getConfidenceCount() != 0 && column.getConfidenceCount() != timestampCount) {
                    return new ResultStatus(true, columnPath + ".confidence must be empty or contain "
                            + "exactly one entry per timestamp: expected 0 or " + timestampCount
                            + ", got: " + column.getConfidenceCount());
                }
                if (column.getReasonsCount() != 0 && column.getReasonsCount() != timestampCount) {
                    return new ResultStatus(true, columnPath + ".reasons must be empty or contain "
                            + "exactly one entry per timestamp: expected 0 or " + timestampCount
                            + ", got: " + column.getReasonsCount());
                }
            }

            totalStatuses += (long) timestampCount * frame.getStatusColumnsCount();
        }

        if (totalStatuses > maxStatusesPerRequest) {
            return new ResultStatus(true, "SaveSampleStatusesRequest exceeds maximum statuses per "
                    + "request: " + totalStatuses + " > " + maxStatusesPerRequest);
        }

        return new ResultStatus(false, "");
    }

    private static int timestampCount(DataTimestamps dataTimestamps) {
        return switch (dataTimestamps.getValueCase()) {
            case SAMPLINGCLOCK -> dataTimestamps.getSamplingClock().getCount();
            case TIMESTAMPLIST -> dataTimestamps.getTimestampList().getTimestampsCount();
            default -> 0;
        };
    }

    private static ResultStatus validateFrameDataTimestamps(String framePath, DataTimestamps dataTimestamps) {

        switch (dataTimestamps.getValueCase()) {

            case SAMPLINGCLOCK -> {
                final SamplingClock clock = dataTimestamps.getSamplingClock();
                if (clock.getCount() < 1) {
                    return new ResultStatus(true, framePath
                            + ".dataTimestamps.samplingClock.count must be >= 1, got: " + clock.getCount());
                }
                if (clock.getPeriodNanos() <= 0) {
                    return new ResultStatus(true, framePath
                            + ".dataTimestamps.samplingClock.periodNanos must be > 0, got: "
                            + clock.getPeriodNanos());
                }
                if (!clock.hasStartTime()) {
                    return new ResultStatus(true, framePath
                            + ".dataTimestamps.samplingClock.startTime must be provided");
                }
                final ResultStatus startStatus = validateTimestamp(
                        framePath + ".dataTimestamps.samplingClock.startTime", clock.getStartTime());
                if (startStatus.isError) {
                    return startStatus;
                }
                // startTime itself is already range-checked by validateTimestamp above; the axis
                // can still run off the end of the epoch-nanos range at its last sample
                try {
                    Math.addExact(
                            validatedEpochNanos(clock.getStartTime()),
                            Math.multiplyExact((long) clock.getCount() - 1, clock.getPeriodNanos()));
                } catch (ArithmeticException ex) {
                    return new ResultStatus(true, framePath
                            + ".dataTimestamps.samplingClock time axis exceeds representable range");
                }
            }

            case TIMESTAMPLIST -> {
                final List<Timestamp> timestamps = dataTimestamps.getTimestampList().getTimestampsList();
                if (timestamps.isEmpty()) {
                    return new ResultStatus(true, framePath
                            + ".dataTimestamps.timestampList.timestamps cannot be empty");
                }
                Timestamp previous = null;
                for (int i = 0; i < timestamps.size(); i++) {
                    final Timestamp current = timestamps.get(i);
                    final ResultStatus timestampStatus = validateTimestamp(
                            framePath + ".dataTimestamps.timestampList.timestamps[" + i + "]", current);
                    if (timestampStatus.isError) {
                        return timestampStatus;
                    }
                    // strictly increasing: equal timestamps would collapse identity keys within the frame
                    if (previous != null
                            && (current.getEpochSeconds() < previous.getEpochSeconds()
                            || (current.getEpochSeconds() == previous.getEpochSeconds()
                            && current.getNanoseconds() <= previous.getNanoseconds()))) {
                        return new ResultStatus(true, framePath
                                + ".dataTimestamps.timestampList.timestamps[" + i + "] is not strictly increasing: "
                                + "previous=" + previous.getEpochSeconds() + "." + previous.getNanoseconds()
                                + ", current=" + current.getEpochSeconds() + "." + current.getNanoseconds());
                    }
                    previous = current;
                }
            }

            default -> {
                return new ResultStatus(true, framePath
                        + ".dataTimestamps must specify either SamplingClock or TimestampList");
            }
        }

        return new ResultStatus(false, "");
    }

    public static ResultStatus validateQuerySampleStatusesRequest(QuerySampleStatusesRequest request) {
        return validateTimeRange("QuerySampleStatusesRequest", request.hasTimeRange(), request.getTimeRange());
    }

    public static ResultStatus validateDeleteSampleStatusesRequest(DeleteSampleStatusesRequest request) {

        final ResultStatus timeRangeStatus =
                validateTimeRange("DeleteSampleStatusesRequest", request.hasTimeRange(), request.getTimeRange());
        if (timeRangeStatus.isError) {
            return timeRangeStatus;
        }
        if (request.getDomain().isBlank()) {
            return new ResultStatus(true, "DeleteSampleStatusesRequest.domain must be specified");
        }
        if (request.getLayer().isBlank()) {
            return new ResultStatus(true, "DeleteSampleStatusesRequest.layer must be specified");
        }
        return new ResultStatus(false, "");
    }

    private static ResultStatus validateTimeRange(String requestPath, boolean hasTimeRange, TimeRange timeRange) {

        if (!hasTimeRange) {
            return new ResultStatus(true, requestPath + ".timeRange must be provided");
        }
        if (!timeRange.hasBeginTime()) {
            return new ResultStatus(true, requestPath + ".timeRange.beginTime must be provided");
        }
        if (!timeRange.hasEndTime()) {
            return new ResultStatus(true, requestPath + ".timeRange.endTime must be provided");
        }
        final ResultStatus beginStatus =
                validateTimestamp(requestPath + ".timeRange.beginTime", timeRange.getBeginTime());
        if (beginStatus.isError) {
            return beginStatus;
        }
        final ResultStatus endStatus =
                validateTimestamp(requestPath + ".timeRange.endTime", timeRange.getEndTime());
        if (endStatus.isError) {
            return endStatus;
        }

        final Timestamp begin = timeRange.getBeginTime();
        final Timestamp end = timeRange.getEndTime();
        final boolean beginBeforeEnd = begin.getEpochSeconds() < end.getEpochSeconds()
                || (begin.getEpochSeconds() == end.getEpochSeconds()
                && begin.getNanoseconds() < end.getNanoseconds());
        if (!beginBeforeEnd) {
            return new ResultStatus(true, requestPath + ".timeRange.beginTime must be before endTime");
        }
        return new ResultStatus(false, "");
    }

    /**
     * Validates a single timestamp's field ranges and its representability as epoch-nanos.
     *
     * <p>The epoch-nanos check is not cosmetic: the storage and query paths convert every
     * timestamp to a signed 64-bit epoch-nanos scalar (firstTimeNanos/lastTimeNanos, the overlap
     * predicates, the keyset paging token). An epochSeconds above
     * {@link #MAX_EPOCH_SECONDS} (~year 2262) silently wraps negative, which would write a
     * document that no subsequent overlap query can find, or build a time range that matches the
     * wrong set. Rejecting here keeps that class of silent wrong answer out of the collection.
     */
    private static ResultStatus validateTimestamp(String fieldPath, Timestamp timestamp) {
        if (timestamp.getEpochSeconds() < 0
                || timestamp.getNanoseconds() < 0
                || timestamp.getNanoseconds() >= 1_000_000_000) {
            return new ResultStatus(true, fieldPath + " has invalid values: seconds="
                    + timestamp.getEpochSeconds() + ", nanos=" + timestamp.getNanoseconds());
        }
        if (timestamp.getEpochSeconds() > MAX_EPOCH_SECONDS) {
            return new ResultStatus(true, fieldPath + " exceeds representable epoch-nanos range: seconds="
                    + timestamp.getEpochSeconds() + ", maximum=" + MAX_EPOCH_SECONDS);
        }
        return new ResultStatus(false, "");
    }

    /**
     * Converts a validated timestamp to epoch-nanos. Only safe on a timestamp that has passed
     * {@link #validateTimestamp}.
     */
    private static long validatedEpochNanos(Timestamp timestamp) {
        return timestamp.getEpochSeconds() * 1_000_000_000L + timestamp.getNanoseconds();
    }
}
