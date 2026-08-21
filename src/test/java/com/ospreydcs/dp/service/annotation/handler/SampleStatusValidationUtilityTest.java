package com.ospreydcs.dp.service.annotation.handler;

import com.ospreydcs.dp.grpc.v1.annotation.DeleteSampleStatusesRequest;
import com.ospreydcs.dp.grpc.v1.annotation.QuerySampleStatusesRequest;
import com.ospreydcs.dp.grpc.v1.annotation.SaveSampleStatusesRequest;
import com.ospreydcs.dp.grpc.v1.common.DataTimestamps;
import com.ospreydcs.dp.grpc.v1.common.SampleStatusColumn;
import com.ospreydcs.dp.grpc.v1.common.SampleStatusFrame;
import com.ospreydcs.dp.grpc.v1.common.TimeRange;
import com.ospreydcs.dp.grpc.v1.common.Timestamp;
import com.ospreydcs.dp.service.common.model.ResultStatus;
import com.ospreydcs.dp.service.common.protobuf.DataTimestampsUtility;
import org.junit.Test;

import java.util.List;

import static org.junit.Assert.*;

public class SampleStatusValidationUtilityTest {

    private static final long MAX_STATUSES = 1_000_000L;
    private static final long START_SECONDS = 1_700_000_000L;
    private static final long PERIOD = 100_000_000L;

    private static Timestamp timestamp(long seconds, long nanos) {
        return Timestamp.newBuilder().setEpochSeconds(seconds).setNanoseconds(nanos).build();
    }

    private static DataTimestamps clockAxis(int count) {
        return DataTimestampsUtility.dataTimestampsWithSamplingClock(START_SECONDS, 0, PERIOD, count);
    }

    private static SampleStatusColumn column(String pvName, int count) {
        final SampleStatusColumn.Builder builder = SampleStatusColumn.newBuilder().setPvName(pvName);
        for (int i = 0; i < count; i++) {
            builder.addStatusCodes(i);
        }
        return builder.build();
    }

    private static SaveSampleStatusesRequest saveRequest(SampleStatusFrame... frames) {
        return SaveSampleStatusesRequest.newBuilder().addAllFrames(List.of(frames)).build();
    }

    private static SampleStatusFrame.Builder frameBuilder(int count) {
        return SampleStatusFrame.newBuilder()
                .setDomain("data_quality")
                .setLayer("layer_a")
                .setDataTimestamps(clockAxis(count))
                .addStatusColumns(column("pv_01", count));
    }

    private static void assertRejected(ResultStatus status, String expectedFragment) {
        assertTrue(status.isError);
        assertTrue("expected message containing '" + expectedFragment + "' but was: " + status.msg,
                status.msg.contains(expectedFragment));
    }

    // ------------------- saveSampleStatuses ---------------------------

    @Test
    public void testSaveValidRequestAccepted() {
        final ResultStatus status = SampleStatusValidationUtility.validateSaveSampleStatusesRequest(
                saveRequest(frameBuilder(3).build()), MAX_STATUSES);
        assertFalse(status.isError);
    }

    @Test
    public void testSaveEmptyFramesRejected() {
        final ResultStatus status = SampleStatusValidationUtility.validateSaveSampleStatusesRequest(
                SaveSampleStatusesRequest.newBuilder().build(), MAX_STATUSES);
        assertRejected(status, "at least one SampleStatusFrame");
    }

    @Test
    public void testSaveBlankDomainRejected() {
        final ResultStatus status = SampleStatusValidationUtility.validateSaveSampleStatusesRequest(
                saveRequest(frameBuilder(2).setDomain("").build()), MAX_STATUSES);
        assertRejected(status, "domain must be specified");
    }

    @Test
    public void testSaveBlankLayerRejected() {
        final ResultStatus status = SampleStatusValidationUtility.validateSaveSampleStatusesRequest(
                saveRequest(frameBuilder(2).setLayer(" ").build()), MAX_STATUSES);
        assertRejected(status, "layer must be specified");
    }

    @Test
    public void testSaveMissingDataTimestampsRejected() {
        final SampleStatusFrame frame = SampleStatusFrame.newBuilder()
                .setDomain("data_quality")
                .setLayer("layer_a")
                .addStatusColumns(column("pv_01", 2))
                .build();
        final ResultStatus status = SampleStatusValidationUtility.validateSaveSampleStatusesRequest(
                saveRequest(frame), MAX_STATUSES);
        assertRejected(status, "dataTimestamps must be provided");
    }

    @Test
    public void testSaveNoStatusColumnsRejected() {
        final SampleStatusFrame frame = SampleStatusFrame.newBuilder()
                .setDomain("data_quality")
                .setLayer("layer_a")
                .setDataTimestamps(clockAxis(2))
                .build();
        final ResultStatus status = SampleStatusValidationUtility.validateSaveSampleStatusesRequest(
                saveRequest(frame), MAX_STATUSES);
        assertRejected(status, "at least one statusColumn");
    }

    @Test
    public void testSaveBlankPvNameRejected() {
        final SampleStatusFrame frame = frameBuilder(2)
                .clearStatusColumns()
                .addStatusColumns(column("", 2))
                .build();
        final ResultStatus status = SampleStatusValidationUtility.validateSaveSampleStatusesRequest(
                saveRequest(frame), MAX_STATUSES);
        assertRejected(status, "pvName must be specified");
    }

    @Test
    public void testSaveDuplicatePvInFrameRejected() {
        final SampleStatusFrame frame = frameBuilder(2)
                .addStatusColumns(column("pv_01", 2))
                .build();
        final ResultStatus status = SampleStatusValidationUtility.validateSaveSampleStatusesRequest(
                saveRequest(frame), MAX_STATUSES);
        assertRejected(status, "more than one statusColumn for PV");
    }

    @Test
    public void testSaveStatusCodesLengthMismatchRejected() {
        final SampleStatusFrame frame = frameBuilder(3)
                .clearStatusColumns()
                .addStatusColumns(column("pv_01", 2))
                .build();
        final ResultStatus status = SampleStatusValidationUtility.validateSaveSampleStatusesRequest(
                saveRequest(frame), MAX_STATUSES);
        assertRejected(status, "statusCodes.length mismatch");
    }

    @Test
    public void testSaveConfidenceLengthMismatchRejected() {
        final SampleStatusColumn badColumn = SampleStatusColumn.newBuilder()
                .setPvName("pv_01")
                .addAllStatusCodes(List.of(1, 2, 3))
                .addAllConfidence(List.of(0.5f))
                .build();
        final SampleStatusFrame frame = frameBuilder(3)
                .clearStatusColumns()
                .addStatusColumns(badColumn)
                .build();
        final ResultStatus status = SampleStatusValidationUtility.validateSaveSampleStatusesRequest(
                saveRequest(frame), MAX_STATUSES);
        assertRejected(status, "confidence must be empty or contain");
    }

    @Test
    public void testSaveReasonsLengthMismatchRejected() {
        final SampleStatusColumn badColumn = SampleStatusColumn.newBuilder()
                .setPvName("pv_01")
                .addAllStatusCodes(List.of(1, 2, 3))
                .addAllReasons(List.of("a", "b"))
                .build();
        final SampleStatusFrame frame = frameBuilder(3)
                .clearStatusColumns()
                .addStatusColumns(badColumn)
                .build();
        final ResultStatus status = SampleStatusValidationUtility.validateSaveSampleStatusesRequest(
                saveRequest(frame), MAX_STATUSES);
        assertRejected(status, "reasons must be empty or contain");
    }

    @Test
    public void testSaveFullLengthCompanionsAccepted() {
        final SampleStatusColumn goodColumn = SampleStatusColumn.newBuilder()
                .setPvName("pv_01")
                .addAllStatusCodes(List.of(1, 2))
                .addAllConfidence(List.of(0.5f, 0.6f))
                .addAllReasons(List.of("", "spike"))
                .build();
        final SampleStatusFrame frame = frameBuilder(2)
                .clearStatusColumns()
                .addStatusColumns(goodColumn)
                .build();
        final ResultStatus status = SampleStatusValidationUtility.validateSaveSampleStatusesRequest(
                saveRequest(frame), MAX_STATUSES);
        assertFalse(status.isError);
    }

    @Test
    public void testSaveClockCountZeroRejected() {
        final DataTimestamps axis =
                DataTimestampsUtility.dataTimestampsWithSamplingClock(START_SECONDS, 0, PERIOD, 0);
        final SampleStatusFrame frame = frameBuilder(0).setDataTimestamps(axis).build();
        final ResultStatus status = SampleStatusValidationUtility.validateSaveSampleStatusesRequest(
                saveRequest(frame), MAX_STATUSES);
        assertRejected(status, "count must be >= 1");
    }

    @Test
    public void testSaveClockPeriodZeroRejected() {
        // periodNanos = 0 would collapse identity keys
        final DataTimestamps axis =
                DataTimestampsUtility.dataTimestampsWithSamplingClock(START_SECONDS, 0, 0, 3);
        final SampleStatusFrame frame = frameBuilder(3).setDataTimestamps(axis).build();
        final ResultStatus status = SampleStatusValidationUtility.validateSaveSampleStatusesRequest(
                saveRequest(frame), MAX_STATUSES);
        assertRejected(status, "periodNanos must be > 0");
    }

    @Test
    public void testSaveEmptyTimestampListRejected() {
        final DataTimestamps axis = DataTimestampsUtility.dataTimestampsWithTimestampList(List.of());
        final SampleStatusFrame frame = frameBuilder(0).setDataTimestamps(axis).build();
        final ResultStatus status = SampleStatusValidationUtility.validateSaveSampleStatusesRequest(
                saveRequest(frame), MAX_STATUSES);
        assertRejected(status, "timestamps cannot be empty");
    }

    @Test
    public void testSaveNonIncreasingTimestampListRejected() {
        final DataTimestamps axis = DataTimestampsUtility.dataTimestampsWithTimestampList(List.of(
                timestamp(START_SECONDS, 100), timestamp(START_SECONDS, 50)));
        final SampleStatusFrame frame = frameBuilder(2).setDataTimestamps(axis).build();
        final ResultStatus status = SampleStatusValidationUtility.validateSaveSampleStatusesRequest(
                saveRequest(frame), MAX_STATUSES);
        assertRejected(status, "not strictly increasing");
    }

    @Test
    public void testSaveDuplicateTimestampListEntriesRejected() {
        // equal timestamps would collapse identity keys within the frame: strictly increasing required
        final DataTimestamps axis = DataTimestampsUtility.dataTimestampsWithTimestampList(List.of(
                timestamp(START_SECONDS, 100), timestamp(START_SECONDS, 100)));
        final SampleStatusFrame frame = frameBuilder(2).setDataTimestamps(axis).build();
        final ResultStatus status = SampleStatusValidationUtility.validateSaveSampleStatusesRequest(
                saveRequest(frame), MAX_STATUSES);
        assertRejected(status, "not strictly increasing");
    }

    @Test
    public void testSaveInvalidTimestampNanosRejected() {
        final DataTimestamps axis = DataTimestampsUtility.dataTimestampsWithTimestampList(List.of(
                timestamp(START_SECONDS, 1_000_000_000L)));
        final SampleStatusFrame frame = frameBuilder(1).setDataTimestamps(axis).build();
        final ResultStatus status = SampleStatusValidationUtility.validateSaveSampleStatusesRequest(
                saveRequest(frame), MAX_STATUSES);
        assertRejected(status, "has invalid values");
    }

    @Test
    public void testSaveMissingClockStartTimeRejected() {
        final DataTimestamps axis = DataTimestamps.newBuilder()
                .setSamplingClock(com.ospreydcs.dp.grpc.v1.common.SamplingClock.newBuilder()
                        .setPeriodNanos(PERIOD)
                        .setCount(2))
                .build();
        final SampleStatusFrame frame = frameBuilder(2).setDataTimestamps(axis).build();
        final ResultStatus status = SampleStatusValidationUtility.validateSaveSampleStatusesRequest(
                saveRequest(frame), MAX_STATUSES);
        assertRejected(status, "startTime must be provided");
    }

    @Test
    public void testSaveBatchCapExceededRejected() {
        // 10 timestamps x 1 column = 10 statuses; cap of 9 rejects
        final ResultStatus status = SampleStatusValidationUtility.validateSaveSampleStatusesRequest(
                saveRequest(frameBuilder(10).build()), 9);
        assertRejected(status, "exceeds maximum statuses per request");
    }

    @Test
    public void testSaveMultipleValidFramesAccepted() {
        final ResultStatus status = SampleStatusValidationUtility.validateSaveSampleStatusesRequest(
                saveRequest(frameBuilder(3).build(), frameBuilder(2).setLayer("layer_b").build()),
                MAX_STATUSES);
        assertFalse(status.isError);
    }

    // ------------------- querySampleStatuses ---------------------------

    private static QuerySampleStatusesRequest queryRequest(TimeRange timeRange) {
        final QuerySampleStatusesRequest.Builder builder = QuerySampleStatusesRequest.newBuilder();
        if (timeRange != null) {
            builder.setTimeRange(timeRange);
        }
        return builder.build();
    }

    @Test
    public void testQueryValidRequestAccepted() {
        final ResultStatus status = SampleStatusValidationUtility.validateQuerySampleStatusesRequest(
                queryRequest(TimeRange.newBuilder()
                        .setBeginTime(timestamp(START_SECONDS, 0))
                        .setEndTime(timestamp(START_SECONDS + 60, 0))
                        .build()));
        assertFalse(status.isError);
    }

    @Test
    public void testQueryMissingTimeRangeRejected() {
        final ResultStatus status =
                SampleStatusValidationUtility.validateQuerySampleStatusesRequest(queryRequest(null));
        assertRejected(status, "timeRange must be provided");
    }

    @Test
    public void testQueryMissingBeginTimeRejected() {
        final ResultStatus status = SampleStatusValidationUtility.validateQuerySampleStatusesRequest(
                queryRequest(TimeRange.newBuilder().setEndTime(timestamp(START_SECONDS, 0)).build()));
        assertRejected(status, "beginTime must be provided");
    }

    @Test
    public void testQueryMissingEndTimeRejected() {
        final ResultStatus status = SampleStatusValidationUtility.validateQuerySampleStatusesRequest(
                queryRequest(TimeRange.newBuilder().setBeginTime(timestamp(START_SECONDS, 0)).build()));
        assertRejected(status, "endTime must be provided");
    }

    @Test
    public void testQueryBeginEqualsEndRejected() {
        final ResultStatus status = SampleStatusValidationUtility.validateQuerySampleStatusesRequest(
                queryRequest(TimeRange.newBuilder()
                        .setBeginTime(timestamp(START_SECONDS, 500))
                        .setEndTime(timestamp(START_SECONDS, 500))
                        .build()));
        assertRejected(status, "beginTime must be before endTime");
    }

    @Test
    public void testQueryBeginAfterEndRejected() {
        final ResultStatus status = SampleStatusValidationUtility.validateQuerySampleStatusesRequest(
                queryRequest(TimeRange.newBuilder()
                        .setBeginTime(timestamp(START_SECONDS + 60, 0))
                        .setEndTime(timestamp(START_SECONDS, 0))
                        .build()));
        assertRejected(status, "beginTime must be before endTime");
    }

    // ------------------- deleteSampleStatuses ---------------------------

    private static DeleteSampleStatusesRequest.Builder deleteRequestBuilder() {
        return DeleteSampleStatusesRequest.newBuilder()
                .setTimeRange(TimeRange.newBuilder()
                        .setBeginTime(timestamp(START_SECONDS, 0))
                        .setEndTime(timestamp(START_SECONDS + 60, 0)))
                .setDomain("data_quality")
                .setLayer("layer_a");
    }

    @Test
    public void testDeleteValidRequestAccepted() {
        final ResultStatus status = SampleStatusValidationUtility.validateDeleteSampleStatusesRequest(
                deleteRequestBuilder().build());
        assertFalse(status.isError);
    }

    @Test
    public void testDeleteMissingTimeRangeRejected() {
        final ResultStatus status = SampleStatusValidationUtility.validateDeleteSampleStatusesRequest(
                deleteRequestBuilder().clearTimeRange().build());
        assertRejected(status, "timeRange must be provided");
    }

    @Test
    public void testDeleteBlankDomainRejected() {
        final ResultStatus status = SampleStatusValidationUtility.validateDeleteSampleStatusesRequest(
                deleteRequestBuilder().setDomain("").build());
        assertRejected(status, "domain must be specified");
    }

    @Test
    public void testDeleteBlankLayerRejected() {
        final ResultStatus status = SampleStatusValidationUtility.validateDeleteSampleStatusesRequest(
                deleteRequestBuilder().setLayer("").build());
        assertRejected(status, "layer must be specified");
    }
}
