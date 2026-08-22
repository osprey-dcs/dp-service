package com.ospreydcs.dp.service.common.bson.samplestatus;

import com.ospreydcs.dp.grpc.v1.common.DataTimestamps;
import com.ospreydcs.dp.grpc.v1.common.SampleStatusColumn;
import com.ospreydcs.dp.grpc.v1.common.Timestamp;
import com.ospreydcs.dp.service.common.bson.samplestatus.SampleStatusDocumentUtility.RemovalResult;
import com.ospreydcs.dp.service.common.bson.samplestatus.SampleStatusDocumentUtility.StatusPoint;
import com.ospreydcs.dp.service.common.exception.DpException;
import com.ospreydcs.dp.service.common.protobuf.DataTimestampsUtility;
import org.junit.Test;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import static org.junit.Assert.*;

public class SampleStatusDocumentUtilityTest {

    private static final long SECOND_NANOS = 1_000_000_000L;
    private static final long START_SECONDS = 1_700_000_000L;
    private static final long START_NANOS_EPOCH = START_SECONDS * SECOND_NANOS;
    private static final long PERIOD = 100_000_000L; // 100ms

    private static DataTimestamps clockAxis(long startSeconds, long startNanos, long periodNanos, int count) {
        return DataTimestampsUtility.dataTimestampsWithSamplingClock(startSeconds, startNanos, periodNanos, count);
    }

    private static DataTimestamps listAxis(long... epochNanos) {
        final List<Timestamp> timestamps = new ArrayList<>();
        for (long nanos : epochNanos) {
            timestamps.add(SampleStatusDocumentUtility.timestampFromNanos(nanos));
        }
        return DataTimestampsUtility.dataTimestampsWithTimestampList(timestamps);
    }

    private static SampleStatusBucketDocument document(
            DataTimestamps axis, List<Integer> codes, List<Float> confidence, List<String> reasons) {
        final SampleStatusColumn.Builder column = SampleStatusColumn.newBuilder()
                .setPvName("pv_01")
                .addAllStatusCodes(codes);
        if (confidence != null) {
            column.addAllConfidence(confidence);
        }
        if (reasons != null) {
            column.addAllReasons(reasons);
        }
        return SampleStatusBucketDocument.fromSampleStatusColumn(
                "data_quality", "layer_a", axis, column.build(), "source-1", "user-1", Instant.now());
    }

    // ------------------- timestampNanosList / timestampFromNanos ---------------------------

    @Test
    public void testTimestampNanosListSamplingClock() {
        final List<Long> nanos = SampleStatusDocumentUtility.timestampNanosList(
                clockAxis(START_SECONDS, 0, PERIOD, 5));
        assertEquals(5, nanos.size());
        assertEquals(START_NANOS_EPOCH, (long) nanos.get(0));
        assertEquals(START_NANOS_EPOCH + 4 * PERIOD, (long) nanos.get(4));
    }

    @Test
    public void testTimestampNanosListTimestampList() {
        final List<Long> nanos = SampleStatusDocumentUtility.timestampNanosList(
                listAxis(START_NANOS_EPOCH, START_NANOS_EPOCH + 7, START_NANOS_EPOCH + SECOND_NANOS));
        assertEquals(List.of(START_NANOS_EPOCH, START_NANOS_EPOCH + 7, START_NANOS_EPOCH + SECOND_NANOS), nanos);
    }

    @Test
    public void testTimestampFromNanosRoundTrip() {
        final long nanos = START_NANOS_EPOCH + 123_456_789L;
        final Timestamp timestamp = SampleStatusDocumentUtility.timestampFromNanos(nanos);
        assertEquals(START_SECONDS, timestamp.getEpochSeconds());
        assertEquals(123_456_789L, timestamp.getNanoseconds());
        assertEquals(nanos, SampleStatusDocumentUtility.timestampNanos(timestamp));
    }

    // ------------------- expandDocument ---------------------------

    @Test
    public void testExpandDocumentWithCompanions() throws DpException {
        final SampleStatusBucketDocument document = document(
                clockAxis(START_SECONDS, 0, PERIOD, 3),
                List.of(1, 2, 3),
                List.of(0.1f, 0.2f, 0.3f),
                List.of("a", "", "c"));
        final List<StatusPoint> points = SampleStatusDocumentUtility.expandDocument(document);
        assertEquals(3, points.size());
        assertEquals(START_NANOS_EPOCH + PERIOD, points.get(1).timestampNanos());
        assertEquals(2, points.get(1).statusCode());
        assertEquals(0.2f, points.get(1).confidence(), 0.0f);
        assertEquals("", points.get(1).reason());
    }

    @Test
    public void testExpandDocumentWithoutCompanions() throws DpException {
        final SampleStatusBucketDocument document = document(
                clockAxis(START_SECONDS, 0, PERIOD, 2), List.of(7, 8), null, null);
        final List<StatusPoint> points = SampleStatusDocumentUtility.expandDocument(document);
        assertNull(points.get(0).confidence());
        assertNull(points.get(0).reason());
    }

    @Test
    public void testExpandDocumentMismatchedStatusCodesThrows() {
        final SampleStatusBucketDocument document = document(
                clockAxis(START_SECONDS, 0, PERIOD, 3), List.of(1, 2, 3), null, null);
        document.setStatusCodes(List.of(1, 2)); // corrupt: 2 codes for 3 timestamps
        assertThrows(DpException.class, () -> SampleStatusDocumentUtility.expandDocument(document));
    }

    @Test
    public void testExpandDocumentMissingDataTimestampsThrows() {
        final SampleStatusBucketDocument document = document(
                clockAxis(START_SECONDS, 0, PERIOD, 2), List.of(1, 2), null, null);
        document.setDataTimestamps(null);
        assertThrows(DpException.class, () -> SampleStatusDocumentUtility.expandDocument(document));
    }

    // ------------------- buildDataTimestamps ---------------------------

    @Test
    public void testBuildDataTimestampsEvenlySpacedYieldsClock() {
        final List<StatusPoint> points = List.of(
                new StatusPoint(START_NANOS_EPOCH, 1, null, null),
                new StatusPoint(START_NANOS_EPOCH + PERIOD, 2, null, null),
                new StatusPoint(START_NANOS_EPOCH + 2 * PERIOD, 3, null, null));
        final DataTimestamps axis = SampleStatusDocumentUtility.buildDataTimestamps(points);
        assertTrue(axis.hasSamplingClock());
        assertEquals(PERIOD, axis.getSamplingClock().getPeriodNanos());
        assertEquals(3, axis.getSamplingClock().getCount());
        assertEquals(START_SECONDS, axis.getSamplingClock().getStartTime().getEpochSeconds());
    }

    @Test
    public void testBuildDataTimestampsIrregularYieldsList() {
        final List<StatusPoint> points = List.of(
                new StatusPoint(START_NANOS_EPOCH, 1, null, null),
                new StatusPoint(START_NANOS_EPOCH + PERIOD, 2, null, null),
                new StatusPoint(START_NANOS_EPOCH + 3 * PERIOD, 3, null, null));
        final DataTimestamps axis = SampleStatusDocumentUtility.buildDataTimestamps(points);
        assertTrue(axis.hasTimestampList());
        assertEquals(3, axis.getTimestampList().getTimestampsCount());
    }

    @Test
    public void testBuildDataTimestampsSinglePointYieldsList() {
        final DataTimestamps axis = SampleStatusDocumentUtility.buildDataTimestamps(
                List.of(new StatusPoint(START_NANOS_EPOCH, 1, null, null)));
        assertTrue(axis.hasTimestampList());
        assertEquals(1, axis.getTimestampList().getTimestampsCount());
    }

    // ------------------- removeTimestamps (save-path carve) ---------------------------

    @Test
    public void testRemoveTimestampsNoCollisionUntouched() throws DpException {
        final SampleStatusBucketDocument document = document(
                clockAxis(START_SECONDS, 0, PERIOD, 3), List.of(1, 2, 3), null, null);
        // timestamps interleaved between the clock ticks: no exact collision
        final RemovalResult result = SampleStatusDocumentUtility.removeTimestamps(
                document, Set.of(START_NANOS_EPOCH + 1, START_NANOS_EPOCH + PERIOD + 1));
        assertEquals(0, result.removedCount());
        assertTrue(result.replacementDocuments().isEmpty());
    }

    @Test
    public void testRemoveTimestampsAllCollideNoReplacement() throws DpException {
        final SampleStatusBucketDocument document = document(
                clockAxis(START_SECONDS, 0, PERIOD, 3), List.of(1, 2, 3), null, null);
        final RemovalResult result = SampleStatusDocumentUtility.removeTimestamps(
                document, Set.of(START_NANOS_EPOCH, START_NANOS_EPOCH + PERIOD, START_NANOS_EPOCH + 2 * PERIOD));
        assertEquals(3, result.removedCount());
        assertTrue(result.replacementDocuments().isEmpty());
    }

    @Test
    public void testRemoveTimestampsPrefixSurvivorsStayClock() throws DpException {
        // removing the first two ticks of a 5-tick clock leaves an evenly spaced run
        final SampleStatusBucketDocument document = document(
                clockAxis(START_SECONDS, 0, PERIOD, 5), List.of(1, 2, 3, 4, 5), null, null);
        final RemovalResult result = SampleStatusDocumentUtility.removeTimestamps(
                document, Set.of(START_NANOS_EPOCH, START_NANOS_EPOCH + PERIOD));
        assertEquals(2, result.removedCount());
        assertEquals(1, result.replacementDocuments().size());

        final SampleStatusBucketDocument replacement = result.replacementDocuments().get(0);
        assertEquals(List.of(3, 4, 5), replacement.getStatusCodes());
        assertEquals(START_NANOS_EPOCH + 2 * PERIOD, replacement.getFirstTimeNanos());
        assertEquals(START_NANOS_EPOCH + 4 * PERIOD, replacement.getLastTimeNanos());
        final DataTimestamps axis = replacement.getDataTimestamps().toDataTimestamps();
        assertTrue(axis.hasSamplingClock());
        assertEquals(3, axis.getSamplingClock().getCount());
    }

    @Test
    public void testRemoveTimestampsInteriorSurvivorsBecomeList() throws DpException {
        // carving an interior tick leaves an unevenly spaced run: single TimestampList replacement
        final SampleStatusBucketDocument document = document(
                clockAxis(START_SECONDS, 0, PERIOD, 4), List.of(1, 2, 3, 4), null, null);
        final RemovalResult result = SampleStatusDocumentUtility.removeTimestamps(
                document, Set.of(START_NANOS_EPOCH + PERIOD));
        assertEquals(1, result.removedCount());
        assertEquals(1, result.replacementDocuments().size());

        final SampleStatusBucketDocument replacement = result.replacementDocuments().get(0);
        assertEquals(List.of(1, 3, 4), replacement.getStatusCodes());
        assertTrue(replacement.getDataTimestamps().toDataTimestamps().hasTimestampList());
    }

    @Test
    public void testRemoveTimestampsPreservesCompanions() throws DpException {
        final SampleStatusBucketDocument document = document(
                clockAxis(START_SECONDS, 0, PERIOD, 3),
                List.of(1, 2, 3),
                List.of(0.1f, 0.2f, 0.3f),
                List.of("a", "b", "c"));
        final RemovalResult result = SampleStatusDocumentUtility.removeTimestamps(
                document, Set.of(START_NANOS_EPOCH + PERIOD));
        final SampleStatusBucketDocument replacement = result.replacementDocuments().get(0);
        assertEquals(List.of(0.1f, 0.3f), replacement.getConfidence());
        assertEquals(List.of("a", "c"), replacement.getReasons());
    }

    @Test
    public void testRemoveTimestampsAbsentCompanionsStayAbsent() throws DpException {
        final SampleStatusBucketDocument document = document(
                clockAxis(START_SECONDS, 0, PERIOD, 3), List.of(1, 2, 3), null, null);
        final RemovalResult result = SampleStatusDocumentUtility.removeTimestamps(
                document, Set.of(START_NANOS_EPOCH));
        final SampleStatusBucketDocument replacement = result.replacementDocuments().get(0);
        assertNull(replacement.getConfidence());
        assertNull(replacement.getReasons());
    }

    @Test
    public void testRemoveTimestampsPreservesIdentityAndProvenance() throws DpException {
        final SampleStatusBucketDocument document = document(
                clockAxis(START_SECONDS, 0, PERIOD, 2), List.of(1, 2), null, null);
        final RemovalResult result = SampleStatusDocumentUtility.removeTimestamps(
                document, Set.of(START_NANOS_EPOCH));
        final SampleStatusBucketDocument replacement = result.replacementDocuments().get(0);
        assertEquals("pv_01", replacement.getPvName());
        assertEquals("data_quality", replacement.getDomain());
        assertEquals("layer_a", replacement.getLayer());
        assertEquals("source-1", replacement.getSource());
        assertEquals("user-1", replacement.getModifiedBy());
        assertEquals(document.getUpdatedTime(), replacement.getUpdatedTime());
        assertNull(replacement.getId()); // new document, id assigned on insert
    }

    // ------------------- removeRange (delete path) ---------------------------

    @Test
    public void testRemoveRangeFullyInsideRemovesAll() throws DpException {
        final SampleStatusBucketDocument document = document(
                clockAxis(START_SECONDS, 0, PERIOD, 3), List.of(1, 2, 3), null, null);
        final RemovalResult result = SampleStatusDocumentUtility.removeRange(
                document, START_NANOS_EPOCH, START_NANOS_EPOCH + 3 * PERIOD);
        assertEquals(3, result.removedCount());
        assertTrue(result.replacementDocuments().isEmpty());
    }

    @Test
    public void testRemoveRangeBoundaryTrimYieldsShorterClock() throws DpException {
        // deleting [start, start+2*period) trims the first two ticks; survivors stay a clock
        final SampleStatusBucketDocument document = document(
                clockAxis(START_SECONDS, 0, PERIOD, 5), List.of(1, 2, 3, 4, 5), null, null);
        final RemovalResult result = SampleStatusDocumentUtility.removeRange(
                document, START_NANOS_EPOCH, START_NANOS_EPOCH + 2 * PERIOD);
        assertEquals(2, result.removedCount());
        assertEquals(1, result.replacementDocuments().size());
        final DataTimestamps axis = result.replacementDocuments().get(0).getDataTimestamps().toDataTimestamps();
        assertTrue(axis.hasSamplingClock());
        assertEquals(3, axis.getSamplingClock().getCount());
        assertEquals(List.of(3, 4, 5), result.replacementDocuments().get(0).getStatusCodes());
    }

    @Test
    public void testRemoveRangeInteriorSplitsClockInTwo() throws DpException {
        // deleting an interior range splits the clock document into two clock documents
        final SampleStatusBucketDocument document = document(
                clockAxis(START_SECONDS, 0, PERIOD, 6), List.of(1, 2, 3, 4, 5, 6), null, null);
        final RemovalResult result = SampleStatusDocumentUtility.removeRange(
                document, START_NANOS_EPOCH + 2 * PERIOD, START_NANOS_EPOCH + 4 * PERIOD);
        assertEquals(2, result.removedCount());
        assertEquals(2, result.replacementDocuments().size());

        final SampleStatusBucketDocument before = result.replacementDocuments().get(0);
        assertEquals(List.of(1, 2), before.getStatusCodes());
        assertTrue(before.getDataTimestamps().toDataTimestamps().hasSamplingClock());
        assertEquals(START_NANOS_EPOCH + PERIOD, before.getLastTimeNanos());

        final SampleStatusBucketDocument after = result.replacementDocuments().get(1);
        assertEquals(List.of(5, 6), after.getStatusCodes());
        assertTrue(after.getDataTimestamps().toDataTimestamps().hasSamplingClock());
        assertEquals(START_NANOS_EPOCH + 4 * PERIOD, after.getFirstTimeNanos());
    }

    @Test
    public void testRemoveRangeHalfOpenBoundaries() throws DpException {
        // [begin, end): a sample exactly at begin is removed, a sample exactly at end survives
        final SampleStatusBucketDocument document = document(
                clockAxis(START_SECONDS, 0, PERIOD, 3), List.of(1, 2, 3), null, null);
        final RemovalResult result = SampleStatusDocumentUtility.removeRange(
                document, START_NANOS_EPOCH + PERIOD, START_NANOS_EPOCH + 2 * PERIOD);
        assertEquals(1, result.removedCount());
        assertEquals(2, result.replacementDocuments().size());
        assertEquals(List.of(1), result.replacementDocuments().get(0).getStatusCodes());
        assertEquals(List.of(3), result.replacementDocuments().get(1).getStatusCodes());
    }

    @Test
    public void testRemoveRangeSpanOverlapButNoSampleInRangeUntouched() throws DpException {
        // sparse list document spanning the range with no sample inside it
        final SampleStatusBucketDocument document = document(
                listAxis(START_NANOS_EPOCH, START_NANOS_EPOCH + 10 * PERIOD), List.of(1, 2), null, null);
        final RemovalResult result = SampleStatusDocumentUtility.removeRange(
                document, START_NANOS_EPOCH + 2 * PERIOD, START_NANOS_EPOCH + 3 * PERIOD);
        assertEquals(0, result.removedCount());
        assertTrue(result.replacementDocuments().isEmpty());
    }
}
